package main

// 前复权日K缓存模块（技术方案 V1.1 T3）
//
// 设计要点:
//  1. 缓存能力只经新接口 /api/kline-qfq 暴露,既有接口一行不改
//  2. 既有 getQfqKlineDay 作为回源函数原样调用,不修改其签名与逻辑
//  3. 存储复用 per-code SQLite(data/database/kline/{code}.db),与原始域 DayKline 分表
//  4. 失效判定:仅 category=1 且事件日 <= 当日 的除权除息事件触发回源
//  5. refresh=1 旁路缓存,供调用方检测命中后的重拉与周度校准修复使用

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

const (
	// qfqCacheVersion 缓存逻辑版本。上游价格/解析逻辑变更时需 +1,使存量缓存自动重建
	qfqCacheVersion = 1
	// qfqTailLimit 原始尾部拉取条数(覆盖停牌、节假日等导致的跳日)
	qfqTailLimit = 10
)

const qfqSchema = `
CREATE TABLE IF NOT EXISTS DayKlineQfq (
	code   TEXT NOT NULL,
	date   INTEGER NOT NULL,
	open   INTEGER NOT NULL,
	high   INTEGER NOT NULL,
	low    INTEGER NOT NULL,
	close  INTEGER NOT NULL,
	volume INTEGER NOT NULL,
	amount INTEGER NOT NULL,
	PRIMARY KEY (code, date)
);
CREATE TABLE IF NOT EXISTS QfqMeta (
	code           TEXT PRIMARY KEY,
	anchor_date    INTEGER NOT NULL,
	built_at       INTEGER NOT NULL,
	last_xdxr_check INTEGER NOT NULL,
	cache_version  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS XdxrEvent (
	code     TEXT NOT NULL,
	date     INTEGER NOT NULL,
	category INTEGER NOT NULL,
	payload  TEXT,
	PRIMARY KEY (code, date, category)
);`

var (
	qfqLockMu sync.Mutex
	qfqLocks  = map[string]*sync.Mutex{}
)

// qfqCodeLock 单股互斥,保护"读缓存 -> XDXR 校验 -> 追加"临界区
func qfqCodeLock(code string) func() {
	qfqLockMu.Lock()
	mu, ok := qfqLocks[code]
	if !ok {
		mu = new(sync.Mutex)
		qfqLocks[code] = mu
	}
	qfqLockMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

type qfqKline struct {
	Date   int64
	Open   protocol.Price
	High   protocol.Price
	Low    protocol.Price
	Close  protocol.Price
	Volume int64
	Amount protocol.Price
}

type qfqMeta struct {
	Code          string
	AnchorDate    int64
	BuiltAt       int64
	LastXdxrCheck int64
	CacheVersion  int
}

func qfqDBPath(code string) string {
	return filepath.Join(tdx.DefaultDatabaseDir, "kline", code+".db")
}

// qfqSchemaReady 已建表的代码集合,避免每次请求重复执行 DDL
var qfqSchemaReady sync.Map

func openQfqDB(code string) (*sql.DB, error) {
	dir := filepath.Join(tdx.DefaultDatabaseDir, "kline")
	if _, ok := qfqSchemaReady.Load(code); !ok {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, code+".db"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, ok := qfqSchemaReady.Load(code); !ok {
		if _, err := db.Exec(qfqSchema); err != nil {
			db.Close()
			return nil, err
		}
		qfqSchemaReady.Store(code, struct{}{})
	}
	return db, nil
}

// loadQfqCache 读取缓存序列与元信息,未命中或版本不符返回 false
// tail>0 时只取最后 tail 条(日常增量场景无需载入全量历史)
func loadQfqCache(db *sql.DB, code string, tail int) ([]*qfqKline, *qfqMeta, bool) {
	meta := new(qfqMeta)
	err := db.QueryRow(
		`SELECT code,anchor_date,built_at,last_xdxr_check,cache_version FROM QfqMeta WHERE code=?`, code,
	).Scan(&meta.Code, &meta.AnchorDate, &meta.BuiltAt, &meta.LastXdxrCheck, &meta.CacheVersion)
	if err != nil {
		return nil, nil, false
	}
	if meta.CacheVersion != qfqCacheVersion {
		return nil, nil, false
	}

	var rows *sql.Rows
	var err2 error
	if tail > 0 {
		rows, err2 = db.Query(
			`SELECT * FROM (SELECT date,open,high,low,close,volume,amount FROM DayKlineQfq WHERE code=? ORDER BY date DESC LIMIT ?) ORDER BY date`, code, tail)
	} else {
		rows, err2 = db.Query(
			`SELECT date,open,high,low,close,volume,amount FROM DayKlineQfq WHERE code=? ORDER BY date`, code)
	}
	if err2 != nil {
		return nil, nil, false
	}
	defer rows.Close()

	ls := make([]*qfqKline, 0, 512)
	for rows.Next() {
		k := new(qfqKline)
		if err := rows.Scan(&k.Date, &k.Open, &k.High, &k.Low, &k.Close, &k.Volume, &k.Amount); err != nil {
			return nil, nil, false
		}
		ls = append(ls, k)
	}
	if len(ls) == 0 {
		return nil, nil, false
	}
	return ls, meta, true
}

// rebuildQfqCache 用回源结果整股重建缓存并重设锚定日
func rebuildQfqCache(db *sql.DB, code string, resp *protocol.KlineResp) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM DayKlineQfq WHERE code=?`, code); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO DayKlineQfq(code,date,open,high,low,close,volume,amount) VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	anchor := int64(0)
	for _, v := range resp.List {
		unix := v.Time.Unix()
		if _, err := stmt.Exec(code, unix, v.Open, v.High, v.Low, v.Close, v.Volume, v.Amount); err != nil {
			return err
		}
		if unix > anchor {
			anchor = unix
		}
	}
	stmt.Close()

	if _, err := tx.Exec(
		`INSERT OR REPLACE INTO QfqMeta(code,anchor_date,built_at,last_xdxr_check,cache_version) VALUES(?,?,?,?,?)`,
		code, anchor, time.Now().Unix(), time.Now().Unix(), qfqCacheVersion); err != nil {
		return err
	}
	return tx.Commit()
}

// appendQfqBars 追加新 bar(无除权时 qfq == raw)并推进锚定日
func appendQfqBars(db *sql.DB, code string, bars []*protocol.Kline, newAnchor int64) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO DayKlineQfq(code,date,open,high,low,close,volume,amount) VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, v := range bars {
		if _, err := stmt.Exec(code, v.Time.Unix(), v.Open, v.High, v.Low, v.Close, v.Volume, v.Amount); err != nil {
			return err
		}
	}
	stmt.Close()

	if _, err := tx.Exec(
		`INSERT OR REPLACE INTO QfqMeta(code,anchor_date,built_at,last_xdxr_check,cache_version) VALUES(?,?,?,?,?)`,
		code, newAnchor, time.Now().Unix(), time.Now().Unix(), qfqCacheVersion); err != nil {
		return err
	}
	return tx.Commit()
}

// saveXdxrEvents 记录除权除息事件(供后续 float_share 刷新信号使用)
func saveXdxrEvents(db *sql.DB, code string, resp *protocol.XdxrResp) {
	if resp == nil || len(resp.List) == 0 {
		return
	}
	//单事务写入:避免每条 INSERT 各自 fsync
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO XdxrEvent(code,date,category,payload) VALUES(?,?,?,?)`)
	if err != nil {
		return
	}
	defer stmt.Close()
	for _, v := range resp.List {
		if _, err := stmt.Exec(code, v.Date.Unix(), int(v.Category), v.String()); err != nil {
			return
		}
	}
	stmt.Close()
	_ = tx.Commit()
}

// hasEffectiveXdxr 锚定日之后是否存在已生效(category=1 且事件日<=当日)的除权除息事件
// 查询失败时保守返回 true(走回源)
func hasEffectiveXdxr(db *sql.DB, code string, anchor time.Time) bool {
	resp, err := client.GetXdxr(code)
	if err != nil {
		log.Printf("[qfq] XDXR 查询失败 %s: %v", code, err)
		return true
	}
	saveXdxrEvents(db, code, resp)
	events := resp.ExDividendSince(anchor, time.Now())
	if len(events) > 0 {
		log.Printf("[qfq] %s 检测到 %d 条除权除息事件,缓存失效", code, len(events))
		return true
	}
	return false
}

// barsAfter 取锚定日之后的 bar(升序)
func barsAfter(ls []*protocol.Kline, anchor int64) []*protocol.Kline {
	rs := make([]*protocol.Kline, 0, len(ls))
	for _, v := range ls {
		if v.Time.Unix() > anchor {
			rs = append(rs, v)
		}
	}
	return rs
}

// buildQfqResp 缓存行转响应,Last 按上一条 close 填充(与 getQfqKlineDay 行为一致)
func buildQfqResp(ls []*qfqKline) *protocol.KlineResp {
	resp := &protocol.KlineResp{
		Count: uint16(len(ls)),
		List:  make([]*protocol.Kline, 0, len(ls)),
	}
	var last protocol.Price
	for _, v := range ls {
		resp.List = append(resp.List, &protocol.Kline{
			Last:   last,
			Open:   v.Open,
			High:   v.High,
			Low:    v.Low,
			Close:  v.Close,
			Volume: v.Volume,
			Amount: v.Amount,
			Time:   time.Unix(v.Date, 0),
		})
		last = v.Close
	}
	return resp
}

// mergeAppended 把追加的 bar 接在缓存序列尾部(Last 由 buildQfqResp 统一重算)
func mergeAppended(cached []*qfqKline, bars []*protocol.Kline) []*qfqKline {
	for _, v := range bars {
		cached = append(cached, &qfqKline{
			Date:   v.Time.Unix(),
			Open:   v.Open,
			High:   v.High,
			Low:    v.Low,
			Close:  v.Close,
			Volume: v.Volume,
			Amount: v.Amount,
		})
	}
	return cached
}

// getQfqKlineDayCached 缓存优先的前复权日K服务
// refresh=true 时绕过缓存回源并重建缓存
func getQfqKlineDayCached(code string, refresh bool, tail int) (*protocol.KlineResp, error) {
	start := time.Now()
	defer func() {
		log.Printf("[qfq] %s refresh=%v 耗时=%v", code, refresh, time.Since(start))
	}()

	unlock := qfqCodeLock(code)
	defer unlock()

	db, err := openQfqDB(code)
	if err != nil {
		log.Printf("[qfq] 打开缓存库失败 %s: %v", code, err)
		//缓存库不可用不影响主流程,退化为回源
		return getQfqKlineDay(code)
	}
	defer db.Close()

	if !refresh {
		if cached, meta, ok := loadQfqCache(db, code, tail); ok {
			if !hasEffectiveXdxr(db, code, time.Unix(meta.AnchorDate, 0)) {
				//无除权事件:原始域尾部追加(qfq == raw,amount 为真实成交额)
				if tail, err := client.GetKlineDay(code, 0, qfqTailLimit); err == nil && tail != nil {
					newBars := barsAfter(tail.List, meta.AnchorDate)
					if len(newBars) > 0 {
						if err := appendQfqBars(db, code, newBars, newBars[len(newBars)-1].Time.Unix()); err != nil {
							log.Printf("[qfq] 追加缓存失败 %s: %v", code, err)
						} else {
							cached = mergeAppended(cached, newBars)
						}
					} else if err := touchQfqMeta(db, code, meta.AnchorDate); err != nil {
						log.Printf("[qfq] 更新校验时间失败 %s: %v", code, err)
					}
					return buildQfqResp(cached), nil
				}
			}
			//有除权事件或尾部拉取失败:落到回源
		}
	}

	//回源:直接复用既有实现(THS 全量 + TDX amount 合并)
	resp, err := getQfqKlineDay(code)
	if err != nil {
		return nil, err
	}
	if err := rebuildQfqCache(db, code, resp); err != nil {
		log.Printf("[qfq] 重建缓存失败 %s: %v", code, err)
	}
	return resp, nil
}

// touchQfqMeta 无新增 bar 时仅刷新校验时间
func touchQfqMeta(db *sql.DB, code string, anchor int64) error {
	_, err := db.Exec(
		`INSERT OR REPLACE INTO QfqMeta(code,anchor_date,built_at,last_xdxr_check,cache_version) VALUES(?,?,?,?,?)`,
		code, anchor, time.Now().Unix(), time.Now().Unix(), qfqCacheVersion)
	return err
}

// handleGetKlineQfq 前复权K线(缓存优先)
// GET /api/kline-qfq?code=000001&type=day|week|month&limit=N&refresh=1
func handleGetKlineQfq(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	klineType := r.URL.Query().Get("type")
	limitStr := strings.TrimSpace(r.URL.Query().Get("limit"))
	refresh := strings.TrimSpace(r.URL.Query().Get("refresh")) == "1"

	if code == "" {
		errorResponse(w, "股票代码不能为空")
		return
	}

	limit := 0
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	//week/month 需要完整日序列做转换,只有 day 支持尾部裁剪
	tail := 0
	if limit > 0 && (klineType == "day" || klineType == "") {
		tail = limit
	}

	resp, err := getQfqKlineDayCached(code, refresh, tail)
	if err != nil {
		errorResponse(w, fmt.Sprintf("获取K线失败: %v", err))
		return
	}

	switch klineType {
	case "week":
		resp = convertToWeekKline(resp)
	case "month":
		resp = convertToMonthKline(resp)
	}

	if limit > 0 && len(resp.List) > limit {
		resp.List = resp.List[len(resp.List)-limit:]
		resp.Count = uint16(limit)
	}

	successResponse(w, resp)
}
