package extend

// 同花顺回源守卫（第三方数据采集限流与补采优化方案 20261120 §3 P0）
//
// 职责（Python 侧节拍无法覆盖 Go 内部的两次回源、强刷与手动校准）：
//  1. 进程内共享发送节拍：前复权/后复权/强刷/因子初始化/校准共用一个
//     d.10jqka.com.cn 出站节拍（env THS_MIN_INTERVAL_MS，缺省 500ms）
//  2. 急停暂停文件：<DefaultDatabaseDir>/ths_pause.json（挂载数据卷，跨重启继承）——
//     上游 429/403/非 JSONP 时写入 blocked_until，所有 THS GET 先检查，到期自清
//
// 单容器进程内共享 + 数据卷继承；多副本部署时再统一跨实例准入（方案 §2）。
// 明确拒绝一经确认即独立登记暂停，不等调用方汇总（方案 §2）。

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/injoyai/tdx"
)

const (
	// thsPauseDefault429 无可信 Retry-After 时的默认暂停时长（与 Python 侧 429 语义对齐）
	thsPauseDefault429 = 1800 * time.Second
	// thsPauseDefault403 403/拦截页默认暂停时长（原因待核，先暂停该上游）
	thsPauseDefault403 = 600 * time.Second
	// thsPauseFilename 暂停文件名（位于挂载数据卷内，跨重启继承）
	thsPauseFilename = "ths_pause.json"
)

// thsMinInterval 出站最小间隔（env THS_MIN_INTERVAL_MS；0=不限速，上限 60s 防误配）
var thsMinInterval = func() time.Duration {
	ms := 500
	if v := os.Getenv("THS_MIN_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 60000 {
			ms = n
		}
	}
	return time.Duration(ms) * time.Millisecond
}()

var (
	thsBeatMu   sync.Mutex
	thsBeatNext time.Time
	// thsPauseMu 保护暂停文件的读改写（单容器内多 goroutine 共享）
	thsPauseMu sync.Mutex
)

type thsPause struct {
	BlockedUntil int64  `json:"blocked_until"` // unix 秒
	Reason       string `json:"reason"`
	UpdatedAt    int64  `json:"updated_at"`
}

func thsPausePath() string {
	return filepath.Join(tdx.DefaultDatabaseDir, thsPauseFilename)
}

func loadThsPause() *thsPause {
	bs, err := os.ReadFile(thsPausePath())
	if err != nil {
		return nil
	}
	var p thsPause
	if err := json.Unmarshal(bs, &p); err != nil {
		return nil
	}
	return &p
}

func saveThsPause(p *thsPause) error {
	if dir := filepath.Dir(thsPausePath()); dir != "" {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return err
		}
	}
	bs, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return os.WriteFile(thsPausePath(), bs, 0644)
}

// thsPauseActive 返回生效中的暂停（到期自动清理文件）
func thsPauseActive() (*thsPause, bool) {
	thsPauseMu.Lock()
	defer thsPauseMu.Unlock()
	p := loadThsPause()
	if p == nil {
		return nil, false
	}
	if time.Now().Unix() >= p.BlockedUntil {
		// 到期自清（失败不影响主流程，下次检查再清）
		_ = os.Remove(thsPausePath())
		return nil, false
	}
	return p, true
}

// thsWaitBeforeGet 发送前准入：暂停检查 + 进程内共享节拍等待；暂停中返回错误（请求不外发）。
// 节拍等待在锁外进行（不阻塞其他 goroutine 排队推进各自的发送时刻）。
func thsWaitBeforeGet() error {
	if p, active := thsPauseActive(); active {
		remaining := time.Until(time.Unix(p.BlockedUntil, 0)).Round(time.Second)
		return fmt.Errorf("THS风控冷却中（剩余 %v，原因: %s），请求未发出", remaining, p.Reason)
	}
	thsBeatMu.Lock()
	now := time.Now()
	wait := time.Duration(0)
	if thsBeatNext.After(now) {
		wait = thsBeatNext.Sub(now)
	}
	if thsBeatNext.Before(now) {
		thsBeatNext = now
	}
	thsBeatNext = thsBeatNext.Add(thsMinInterval)
	thsBeatMu.Unlock()
	if wait > 0 {
		time.Sleep(wait)
	}
	// 双检：等待期间其他 goroutine 可能已登记暂停——到期的真实业务请求
	// 只放行先到者（方案 §2：冷却到期只允许一个先行）
	if p, active := thsPauseActive(); active {
		remaining := time.Until(time.Unix(p.BlockedUntil, 0)).Round(time.Second)
		return fmt.Errorf("THS风控冷却中（剩余 %v，原因: %s），请求未发出", remaining, p.Reason)
	}
	return nil
}

// thsNoteBlocked 明确拒绝（429/403/拦截页）即登记暂停，独立提交、跨重启继承。
// 429 解析 Retry-After（取与默认值中较晚者）；403/拦截页默认 600s（原因待核）。
func thsNoteBlocked(statusCode int, retryAfter time.Duration, reason string) {
	d := thsPauseDefault403
	if statusCode == 429 {
		d = thsPauseDefault429
		if retryAfter > d {
			d = retryAfter
		}
	}
	thsPauseMu.Lock()
	defer thsPauseMu.Unlock()
	// 已有更晚的暂停记录时不提前（取更晚期限，方案 §2 登记冷却以较晚为准）
	if old := loadThsPause(); old != nil {
		oldUntil := time.Unix(old.BlockedUntil, 0)
		if oldUntil.After(time.Now().Add(d)) {
			return
		}
	}
	until := time.Now().Add(d)
	p := &thsPause{BlockedUntil: until.Unix(), Reason: reason, UpdatedAt: time.Now().Unix()}
	if err := saveThsPause(p); err != nil {
		log.Printf("[ths-guard] 暂停登记失败（本次错误仍返回调用方）: %v", err)
		return
	}
	log.Printf("[ths-guard] THS 急停：暂停至 %s（%v，原因: %s）",
		until.Format("2006-01-02 15:04:05"), d, reason)
}

// parseRetryAfterSeconds 解析 Retry-After 头（秒数或 HTTP 日期），非法返回 0
func parseRetryAfterSeconds(v string) time.Duration {
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
