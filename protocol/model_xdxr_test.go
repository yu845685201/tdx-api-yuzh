package protocol

import (
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"
)

// TestXdxrFrame 请求帧对照 pytdx 固定头 0c1f187600010b000b000f000100 + market + code
func TestXdxrFrame(t *testing.T) {
	f, err := MXdxr.Frame("000001")
	if err != nil {
		t.Fatal(err)
	}
	bs := f.Bytes()
	//MsgID 由 SendFrame 赋值,此处只校验控制码/长度/功能号/数据域
	got := hex.EncodeToString(bs[5:])
	want := "01" + "0b00" + "0b00" + "0f00" + "0100" + "00" + hex.EncodeToString([]byte("000001"))
	if got != want {
		t.Fatalf("帧内容不符\n got=%s\nwant=%s", got, want)
	}
	if f.Type != TypeXdxr {
		t.Fatalf("功能号不符: 0x%X", f.Type)
	}
}

// TestXdxrDecode 单条除权除息记录解析(29字节:7+1+4+1+16)
func TestXdxrDecode(t *testing.T) {
	bs := make([]byte, 11+29)
	//前9字节为未知头
	bs[9], bs[10] = 0x01, 0x00 //记录数1
	pos := 11
	pos += 7 //market+code
	pos += 1 //未知字节
	//日期 20260612 -> uint32 LE
	zipday := uint32(20260612)
	bs[pos], bs[pos+1], bs[pos+2], bs[pos+3] = byte(zipday), byte(zipday>>8), byte(zipday>>16), byte(zipday>>24)
	pos += 4
	bs[pos] = XdxrCategory除权除息
	pos++
	//负载 <ffff: fenhong=3.6 peigujia=0 songzhuangu=0 peigu=0
	putFloat32(bs[pos:], 3.6)
	putFloat32(bs[pos+4:], 0)
	putFloat32(bs[pos+8:], 0)
	putFloat32(bs[pos+12:], 0)

	resp, err := MXdxr.Decode(bs)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Count != 1 || len(resp.List) != 1 {
		t.Fatalf("记录数不符: count=%d len=%d", resp.Count, len(resp.List))
	}
	e := resp.List[0]
	if e.Date.Format("20060102") != "20260612" {
		t.Fatalf("日期解析错误: %s", e.Date)
	}
	if e.Category != XdxrCategory除权除息 {
		t.Fatalf("分类解析错误: %d", e.Category)
	}
	if math.Abs(float64(e.Fenhong)-3.6) > 1e-4 {
		t.Fatalf("分红解析错误: %v", e.Fenhong)
	}
	if e.CategoryName() != "除权除息" {
		t.Fatalf("分类名错误: %s", e.CategoryName())
	}
}

// TestXdxrDecodeShort 长度不足时返回空列表(与 pytdx 行为一致)
func TestXdxrDecodeShort(t *testing.T) {
	resp, err := MXdxr.Decode([]byte{0x01, 0x02})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Count != 0 || len(resp.List) != 0 {
		t.Fatalf("应返回空列表,实际 count=%d len=%d", resp.Count, len(resp.List))
	}
}

// TestXdxrExDividendSince 未来事件(除权日未到)不应触发缓存失效
func TestXdxrExDividendSince(t *testing.T) {
	mk := func(date string, cat uint8) *XdxrEvent {
		tm, _ := time.ParseInLocation("20060102", date, time.Local)
		return &XdxrEvent{Date: tm, Category: cat}
	}
	resp := &XdxrResp{List: []*XdxrEvent{
		mk("20260101", XdxrCategory除权除息), //早于锚定日
		mk("20260612", XdxrCategory除权除息), //生效
		mk("20261231", XdxrCategory除权除息), //未来事件
		mk("20260630", XdxrCategory股本变化), //非除权除息
	}}
	anchor, _ := time.ParseInLocation("20060102", "20260301", time.Local)
	today, _ := time.ParseInLocation("20060102", "20260909", time.Local)
	ls := resp.ExDividendSince(anchor, today)
	if len(ls) != 1 || ls[0].Date.Format("20060102") != "20260612" {
		t.Fatalf("失效判定错误,命中 %d 条", len(ls))
	}
}

func putFloat32(bs []byte, v float32) {
	bits := math.Float32bits(v)
	bs[0], bs[1], bs[2], bs[3] = byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24)
}

func TestXdxrFrameCodeLength(t *testing.T) {
	if _, err := MXdxr.Frame("00001"); err == nil {
		t.Fatal("非法代码长度应报错")
	}
	if _, err := MXdxr.Frame("sz000001"); err == nil {
		t.Fatal("非法代码长度应报错")
	}
	if _, err := MXdxr.Frame("000001"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(XdxrCategoryName[1], "除权除息") {
		t.Fatal("分类名映射缺失")
	}
}
