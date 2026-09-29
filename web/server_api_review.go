package main

// 每日复盘数据采集扩展接口（每日复盘数据采集技术方案 V1.0 第三章）
//
// 改动原则：只新增，不修改——本文件仅新增两个只读 HTTP handler，暴露协议库/扩展库
// 已实现的能力（GetTHSDayKlineFactorFull / Client.GetXdxr），不触碰任何现有
// handler、缓存与任务逻辑。路由注册在 server.go main() 的 mux 段追加两行。
//
// GET /api/ths-factor?code=000001            复权因子主接口（THS 同源，全历史）
// GET /api/xdxr?code=000001&limit=0          除权除息事件（limit=0 全历史，>0 取尾部 N 条）
//
// 部署：cd tdx-api/web && go run .（必须全文件编译，见 README 警告）

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/injoyai/tdx/extend"
	"github.com/injoyai/tdx/protocol"
)

// thsFactorItem 单日复权因子（date 为 yyyyMMdd）
type thsFactorItem struct {
	Date     string  `json:"date"`
	QFactor  float64 `json:"q_factor"`
	HFactor  float64 `json:"h_factor"`
}

// handleGetTHSFactor 复权因子：GET /api/ths-factor?code=000001
//
// 能力来源【已验证】：extend/spider-ths.go GetTHSDayKlineFactorFull(code, c)，
// 内部调 THS JSONP 接口（d.10jqka.com.cn/v6/line/hs_{code}/01/all.js 等），
// 返回全历史 []*THSFactor{Date, QFactor, HFactor}（前复权/后复权因子序列）。
// 与现有日K qfq 源同为 THS 同源数据，因子可直接解释现有前复权 K 线。
//
// 注意：THS 偶发返回空 body 时，库内 JSONP 剥壳（spider-ths.go:134）会 panic
// （slice bounds [:-1]），本 handler 以 recover 防护转为业务错误（code=-1），
// 客户端按网络异常重试；不修改库代码本身。
func handleGetTHSFactor(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("ths-factor panic recovered: %v", rec)
			errorResponse(w, fmt.Sprintf("获取复权因子失败: %v", rec))
			return
		}
	}()
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if code == "" {
		errorResponse(w, "code 为必填参数")
		return
	}

	_, factors, err := extend.GetTHSDayKlineFactorFull(code, client)
	if err != nil {
		errorResponse(w, fmt.Sprintf("获取复权因子失败: %v", err))
		return
	}

	list := make([]thsFactorItem, 0, len(factors))
	for _, f := range factors {
		if f == nil {
			continue
		}
		list = append(list, thsFactorItem{
			Date:    time.Unix(f.Date, 0).Format("20060102"),
			QFactor: f.QFactor,
			HFactor: f.HFactor,
		})
	}

	successResponse(w, map[string]interface{}{
		"code":  code,
		"count": len(list),
		"list":  list,
	})
}

// handleGetXdxrEvents 除权除息事件：GET /api/xdxr?code=000001&limit=0
//
// 能力来源【已验证】：client.go GetXdxr(code)（功能号 0x000f，protocol/model_xdxr.go），
// 返回 XdxrResp{Count, List[XdxrEvent]}；事件字段含 Date/Category/Fenhong/Peigujia/
// Songzhuangu/Peigu/Suogu/Xingquanjia/Fenshu 及股本变动类字段。
// 用途：送转/分红事件日历；复权因子跳变日交叉校验（跳变日应存在 Category=1 事件）。
func handleGetXdxrEvents(w http.ResponseWriter, r *http.Request) {
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if code == "" {
		errorResponse(w, "code 为必填参数")
		return
	}
	limit := 0
	if s := strings.TrimSpace(r.URL.Query().Get("limit")); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			limit = n
		}
	}

	resp, err := client.GetXdxr(code)
	if err != nil {
		errorResponse(w, fmt.Sprintf("获取除权除息事件失败: %v", err))
		return
	}
	if resp == nil {
		successResponse(w, map[string]interface{}{
			"code":  code,
			"count": 0,
			"list":  []interface{}{},
		})
		return
	}

	list := resp.List
	if limit > 0 && len(list) > limit {
		list = list[len(list)-limit:]
	}
	events := make([]map[string]interface{}, 0, len(list))
	for _, e := range list {
		if e == nil {
			continue
		}
		events = append(events, map[string]interface{}{
			"date":           e.Date.Format("20060102"),
			"category":       e.Category,
			"category_name":  e.CategoryName(),
			"fenhong":        e.Fenhong,     //每10股派息
			"peigujia":       e.Peigujia,    //配股价
			"songzhuangu":    e.Songzhuangu, //每10股送转
			"peigu":          e.Peigu,       //每10股配股
			"suogu":          e.Suogu,       //缩股比例
			"xingquanjia":    e.Xingquanjia, //行权价
			"fenshu":         e.Fenshu,      //份数
			"panqianliutong": e.Panqianliutong,
			"panhouliutong":  e.Panhouliutong,
			"qianzongguben":  e.Qianzongguben,
			"houzongguben":   e.Houzongguben,
		})
	}

	successResponse(w, map[string]interface{}{
		"code":  code,
		"count": len(events),
		"list":  events,
	})
}

// 便于静态检查引用（protocol 包在此文件仅使用其模型定义）
var _ = protocol.TypeXdxr
