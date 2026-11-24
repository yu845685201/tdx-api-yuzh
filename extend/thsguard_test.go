package extend

// 同花顺守卫离线测试（限流方案 20261120 §3 P0）
// 不向真实上游发请求：只测暂停文件读写/到期自清/Retry-After 解析/
// 年度分段校验等纯逻辑。联网回放由容器构建与人工观察覆盖。

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/injoyai/conv"
)

// chdirTmp 进入临时目录（tdx.DefaultDatabaseDir 为相对路径 ./data/database，
// chdir 后暂停文件落在临时目录，不污染真实数据卷）
func chdirTmp(t *testing.T) string {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	return dir
}

func TestThsPauseRoundTrip(t *testing.T) {
	dir := chdirTmp(t)
	if _, active := thsPauseActive(); active {
		t.Fatal("初始不应有暂停")
	}
	thsNoteBlocked(429, 0, "HTTP 429: test")
	p, active := thsPauseActive()
	if !active {
		t.Fatal("登记后应有暂停")
	}
	remaining := time.Until(time.Unix(p.BlockedUntil, 0))
	if remaining < 1500*time.Second {
		t.Fatalf("429 默认暂停应约 1800s: %v", remaining)
	}
	if !strings.Contains(p.Reason, "429") {
		t.Fatalf("原因未记录: %s", p.Reason)
	}
	// 暂停文件确实落在挂载数据卷路径（跨重启继承的基础）
	if _, err := os.Stat(filepath.Join(dir, "data", "database", thsPauseFilename)); err != nil {
		t.Fatalf("暂停文件未写入数据卷: %v", err)
	}
	// 已有更晚暂停时不提前（登记以较晚期限为准）
	before := p.BlockedUntil
	thsNoteBlocked(403, 0, "HTTP 403: test")
	p2, active := thsPauseActive()
	if !active || p2.BlockedUntil != before {
		t.Fatalf("403 不应缩短既有 429 暂停: %d -> %d", before, p2.BlockedUntil)
	}
}

func TestThsPauseRetryAfter(t *testing.T) {
	chdirTmp(t)
	thsNoteBlocked(429, 7200*time.Second, "Retry-After test")
	p, active := thsPauseActive()
	if !active {
		t.Fatal("登记后应有暂停")
	}
	remaining := time.Until(time.Unix(p.BlockedUntil, 0))
	if remaining < 7000*time.Second {
		t.Fatalf("Retry-After=7200s 应优先于默认 1800s: %v", remaining)
	}
}

func TestThsPauseExpiry(t *testing.T) {
	chdirTmp(t)
	if err := saveThsPause(&thsPause{
		BlockedUntil: time.Now().Add(-time.Second).Unix(),
		Reason:       "expired",
	}); err != nil {
		t.Fatal(err)
	}
	if _, active := thsPauseActive(); active {
		t.Fatal("过期暂停应视为无暂停")
	}
	if _, err := os.Stat(thsPausePath()); !os.IsNotExist(err) {
		t.Fatal("过期暂停文件应被自清")
	}
}

func TestThsRetryAfterParse(t *testing.T) {
	if d := parseRetryAfterSeconds("120"); d != 120*time.Second {
		t.Fatalf("秒数解析: %v", d)
	}
	if d := parseRetryAfterSeconds(""); d != 0 {
		t.Fatalf("空值应返回 0: %v", d)
	}
	if d := parseRetryAfterSeconds("junk"); d != 0 {
		t.Fatalf("非法值应返回 0: %v", d)
	}
	if d := parseRetryAfterSeconds("-5"); d != 0 {
		t.Fatalf("负值应返回 0: %v", d)
	}
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if d := parseRetryAfterSeconds(future); d <= 0 {
		t.Fatalf("HTTP 日期解析: %v", d)
	}
}

func TestThsGetParamValidationNoNetwork(t *testing.T) {
	// 参数错误早退分支：不发网络请求（离线）
	if _, err := GetTHSDayKline("sz000001", 9); err == nil {
		t.Fatal("非法复权类型应报错")
	}
	if _, err := GetTHSDayKline("12345", THS_QFQ); err == nil {
		t.Fatal("非法代码应报错")
	}
}

func TestBuildTHSYearSegments(t *testing.T) {
	dates := make([]string, 0, 220)
	for _, md := range []string{"0104", "0105", "0106"} {
		dates = append(dates, md)
	}
	// 正常分段
	seg, err := buildTHSYearSegments(
		[]interface{}{conv.Interfaces([]int{2025, 2}), conv.Interfaces([]int{2026, 1})},
		[]string{"0104", "0105", "0106"})
	if err != nil {
		t.Fatalf("正常分段不应报错: %v", err)
	}
	if len(seg[2025]) != 2 || len(seg[2026]) != 1 {
		t.Fatalf("分段结果异常: %v", seg)
	}
	// 异常年度长度（>300）
	_, err = buildTHSYearSegments([]interface{}{conv.Interfaces([]int{2025, 301})}, dates)
	if err == nil || !strings.Contains(err.Error(), "异常年度长度") {
		t.Fatalf("单年超 300 应报异常年度长度: %v", err)
	}
	// 长度越界
	_, err = buildTHSYearSegments(
		[]interface{}{conv.Interfaces([]int{2025, 10}), conv.Interfaces([]int{2026, 1})}, dates)
	if err == nil || !strings.Contains(err.Error(), "越界") {
		t.Fatalf("分段越界应报错: %v", err)
	}
}
