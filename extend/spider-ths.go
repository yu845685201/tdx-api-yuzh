package extend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/injoyai/conv"
	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	UrlTHSDayKline       = "http://d.10jqka.com.cn/v6/line/hs_%s/0%d/all.js"
	THS_BFQ        uint8 = 0 //不复权
	THS_QFQ        uint8 = 1 //前复权
	THS_HFQ        uint8 = 2 //后复权
)

// GetTHSDayKlineFactorFull 增加计算复权因子
func GetTHSDayKlineFactorFull(code string, c *tdx.Client) ([3][]*Kline, []*THSFactor, error) {
	ks, err := GetTHSDayKlineFull(code, c)
	if err != nil {
		return [3][]*Kline{}, nil, err
	}
	mQPrice := make(map[int64]float64)
	for _, v := range ks[1] {
		mQPrice[v.Date] = v.Close.Float64()
	}
	mHPrice := make(map[int64]float64)
	for _, v := range ks[2] {
		mHPrice[v.Date] = v.Close.Float64()
	}
	fs := make([]*THSFactor, 0, len(ks[0]))
	for _, v := range ks[0] {
		fs = append(fs, &THSFactor{
			Date:    v.Date,
			QFactor: mQPrice[v.Date] / v.Close.Float64(),
			HFactor: mHPrice[v.Date] / v.Close.Float64(),
		})
	}
	return ks, fs, nil
}

/*
GetTHSDayKlineFull
获取[不复权,前复权,后复权]数据,并补充成交金额数据
前复权,和通达信对的上,和东方财富对不上
后复权,和通达信,东方财富都对不上
*/
func GetTHSDayKlineFull(code string, c *tdx.Client) ([3][]*Kline, error) {
	resp, err := c.GetKlineDayAll(code)
	if err != nil {
		return [3][]*Kline{}, err
	}
	mAmount := make(map[int64]protocol.Price)
	bfq := []*Kline(nil)
	for _, v := range resp.List {
		mAmount[v.Time.Unix()] = v.Amount
		bfq = append(bfq, &Kline{
			Code:   code,
			Date:   v.Time.Unix(),
			Open:   v.Open,
			High:   v.High,
			Low:    v.Low,
			Close:  v.Close,
			Volume: v.Volume,
			Amount: v.Amount,
		})
	}
	//前复权
	qfq, err := GetTHSDayKline(code, THS_QFQ)
	if err != nil {
		return [3][]*Kline{}, err
	}
	for i := range qfq {
		qfq[i].Amount = mAmount[qfq[i].Date]
	}
	//后复权
	hfq, err := GetTHSDayKline(code, THS_HFQ)
	if err != nil {
		return [3][]*Kline{}, err
	}
	for i := range hfq {
		hfq[i].Amount = mAmount[hfq[i].Date]
	}
	return [3][]*Kline{bfq, qfq, hfq}, nil
}

/*
GetTHSDayKline
前复权,和通达信对的上,和东方财富对不上
后复权,和通达信,东方财富都对不上
*/
func GetTHSDayKline(code string, _type uint8) ([]*Kline, error) {
	if _type != THS_BFQ && _type != THS_QFQ && _type != THS_HFQ {
		return nil, fmt.Errorf("数据类型错误,例如:不复权0或前复权1或后复权2")
	}

	code = protocol.AddPrefix(code)
	if len(code) != 8 {
		return nil, fmt.Errorf("股票代码错误,例如:SZ000001或000001")
	}

	u := fmt.Sprintf(UrlTHSDayKline, code[2:], _type)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	/*
	 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
	                      'Chrome/90.0.4430.212 Safari/537.36',
	        'Referer': 'http://stockpage.10jqka.com.cn/',
	        'DNT': '1',
	*/
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36 Edg/89.0.774.54")
	req.Header.Set("Referer", "http://stockpage.10jqka.com.cn/")
	req.Header.Set("DNT", "1")
	// 限流方案 20261120 §3 P0：发送前统一节拍（前/后复权、强刷、因子、校准共用）
	// 与急停暂停检查——暂停中请求不外发
	if err := thsWaitBeforeGet(); err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// 批次4（数据采集链路耗时优化技术方案 V1.1 §3.3）：解析前校验上游状态、响应长度
	// 与 JSONP 边界，返回可分类错误——修复 2026-11-19 首轮 451 次 HTTP 请求处理 panic
	// （上游风控/错误页返回非 JSONP 内容时，原代码 bytes.IndexByte 找不到 '(' 后
	//   bs[n+1:len(bs)-1] 在空/短响应上触发 slice bounds out of range）
	// 限流方案 20261120 §3 P0：明确拒绝即登记暂停（暂停文件入数据卷，跨重启继承）
	if resp.StatusCode != http.StatusOK {
		snippet := bs
		if len(snippet) > 120 {
			snippet = snippet[:120]
		}
		if resp.StatusCode == 429 || resp.StatusCode == 403 {
			thsNoteBlocked(resp.StatusCode, parseRetryAfterSeconds(resp.Header.Get("Retry-After")),
				fmt.Sprintf("HTTP %d: %q", resp.StatusCode, snippet))
		}
		return nil, fmt.Errorf("THS上游HTTP %d（%d 字节）: %q", resp.StatusCode, len(bs), snippet)
	}
	if len(bs) < 10 {
		return nil, fmt.Errorf("THS响应过短: %d 字节", len(bs))
	}
	n := bytes.IndexByte(bs, '(')
	if n < 0 || bs[len(bs)-1] != ')' {
		snippet := bs
		if len(snippet) > 120 {
			snippet = snippet[:120]
		}
		thsNoteBlocked(403, 0, fmt.Sprintf("响应非 JSONP 结构（疑似风控/改版）: %q", snippet))
		return nil, fmt.Errorf("THS响应非 JSONP 结构（疑似风控/改版）: %q", snippet)
	}
	bs = bs[n+1 : len(bs)-1]

	m := map[string]any{}
	err = json.Unmarshal(bs, &m)
	if err != nil {
		return nil, err
	}

	total := conv.Int(m["total"])
	sortYears := conv.Interfaces(m["sortYear"])
	priceFactor := conv.Float64(m["priceFactor"])
	prices := strings.Split(conv.String(m["price"]), ",")
	dates := strings.Split(conv.String(m["dates"]), ",")
	volumes := strings.Split(conv.String(m["volumn"]), ",")

	//好像到了22点,总数量会比实际多1
	if total == len(dates)+1 && total == len(volumes)+1 {
		total -= 1
	}
	//判断数量是否对应（既有校验保留）
	if total*4 != len(prices) || total != len(dates) || total != len(volumes) {
		return nil, fmt.Errorf("total=%d prices=%d dates=%d volumns=%d", total, len(prices), len(dates), len(volumes))
	}
	//价格序列长度兜底校验（total 校验异常时防御性返回错误而非越界 panic）
	if len(prices) < 4 || priceFactor <= 0 {
		return nil, fmt.Errorf("THS价格序列或因子异常: prices=%d priceFactor=%v", len(prices), priceFactor)
	}

	mYear, err := buildTHSYearSegments(sortYears, dates)
	if err != nil {
		return nil, err
	}

	ls := []*Kline(nil)
	i := 0
	nowYear := time.Now().Year()
	// 限流方案 20261120 §3 P0：日期序列单调校验（历史倒退/重复即判异常响应，
	// 防截短数据经 total*4 不变式后仍进入缓存重建）
	var prevUnix int64 = 0
	for year := 1990; year <= nowYear; year++ {
		for _, d := range mYear[year] {
			if i*4+3 >= len(prices) || i >= len(volumes) {
				return nil, fmt.Errorf("价格/成交量序列越界: i=%d prices=%d volumes=%d", i, len(prices), len(volumes))
			}
			x, err := time.Parse("0102", d)
			if err != nil {
				return nil, err
			}
			x = time.Date(year, x.Month(), x.Day(), 15, 0, 0, 0, time.Local)
			if x.Unix() <= prevUnix {
				return nil, fmt.Errorf("THS 日期序列非单调（疑似截短/改版）: %d <= %d", x.Unix(), prevUnix)
			}
			prevUnix = x.Unix()
			low := protocol.Price(conv.Float64(prices[i*4+0]) * 1000 / priceFactor)
			ls = append(ls, &Kline{
				Code:   protocol.AddPrefix(code),
				Date:   x.Unix(),
				Open:   protocol.Price(conv.Float64(prices[i*4+1])*1000/priceFactor) + low,
				High:   protocol.Price(conv.Float64(prices[i*4+2])*1000/priceFactor) + low,
				Low:    low,
				Close:  protocol.Price(conv.Float64(prices[i*4+3])*1000/priceFactor) + low,
				Volume: (conv.Int64(volumes[i]) + 50) / 100,
			})
			i++
		}
	}

	return ls, nil
}

// buildTHSYearSegments 解析 sortYear 年度分段（离线可测纯函数）。
// 限流方案 20261120 §3 P0：含异常年度长度校验——单年 bar 数超过交易日常识
// 上限（~250）即判异常响应，避免截短/改版数据进入缓存重建。
func buildTHSYearSegments(sortYears []interface{}, dates []string) (map[int][]string, error) {
	mYear := make(map[int][]string)
	index := 0
	for i, v := range sortYears {
		if ls := conv.Ints(v); len(ls) == 2 {
			year := conv.Int(ls[0])
			length := conv.Int(ls[1])
			if length > 300 {
				return nil, fmt.Errorf("THS 异常年度长度: year=%d bars=%d（上限 300）", year, length)
			}
			if i == len(sortYears)-1 {
				if len(dates)-index > 300 {
					return nil, fmt.Errorf("THS 异常年度长度: year=%d bars=%d（上限 300）", year, len(dates)-index)
				}
				mYear[year] = dates[index:]
				break
			}
			if index+length > len(dates) {
				return nil, fmt.Errorf("sortYear 长度越界: year=%d length=%d dates=%d", year, length, len(dates))
			}
			mYear[year] = dates[index : index+length]
			index += length
		}
	}
	return mYear, nil
}
