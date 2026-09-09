package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/injoyai/logs"
	"github.com/injoyai/tdx"
)

type outRec struct {
	Date          string  `json:"date"`
	Category      uint8   `json:"category"`
	Fenhong       float32 `json:"fenhong"`
	Peigujia      float32 `json:"peigujia"`
	Songzhuangu   float32 `json:"songzhuangu"`
	Peigu         float32 `json:"peigu"`
	Suogu         float32 `json:"suogu"`
	Xingquanjia   float32 `json:"xingquanjia"`
	Fenshu        float32 `json:"fenshu"`
	Panqianliutong float64 `json:"panqianliutong"`
	Panhouliutong  float64 `json:"panhouliutong"`
	Qianzongguben  float64 `json:"qianzongguben"`
	Houzongguben   float64 `json:"houzongguben"`
}

func main() {
	codes := []string{"000001", "600519", "300750", "600000"}

	c, err := tdx.DialWith(tdx.NewHostDial(tdx.Hosts))
	logs.PanicErr(err)

	result := map[string][]outRec{}
	for _, code := range codes {
		start := time.Now()
		resp, err := c.GetXdxr(code)
		if err != nil {
			logs.Err(code, err)
			continue
		}
		cost := time.Since(start)

		recs := make([]outRec, 0, len(resp.List))
		cnt := map[uint8]int{}
		for _, v := range resp.List {
			cnt[v.Category]++
			recs = append(recs, outRec{
				Date:          v.Date.Format("2006-01-02"),
				Category:      v.Category,
				Fenhong:       v.Fenhong,
				Peigujia:      v.Peigujia,
				Songzhuangu:   v.Songzhuangu,
				Peigu:         v.Peigu,
				Suogu:         v.Suogu,
				Xingquanjia:   v.Xingquanjia,
				Fenshu:        v.Fenshu,
				Panqianliutong: v.Panqianliutong,
				Panhouliutong:  v.Panhouliutong,
				Qianzongguben:  v.Qianzongguben,
				Houzongguben:   v.Houzongguben,
			})
		}
		result[code] = recs

		fmt.Printf("=== %s 共 %d 条 耗时 %v 分类分布 %v\n", code, resp.Count, cost, cnt)
		if len(resp.List) > 0 {
			fmt.Printf("    首条: %s\n", resp.List[0])
			fmt.Printf("    末条: %s\n", resp.List[len(resp.List)-1])
		}
		for _, v := range resp.List {
			if v.Category == 1 && v.Date.Year() >= 2025 {
				fmt.Printf("    近期除权除息: %s\n", v)
			}
		}
	}

	bs, _ := json.MarshalIndent(result, "", " ")
	_ = os.WriteFile("/tmp/xdxr_go.json", bs, 0644)
	fmt.Println("[done] 结果已写入 /tmp/xdxr_go.json")
}
