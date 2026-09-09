package protocol

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// TypeXdxr 除权除息信息功能号
// pytdx 常量 KMSG_XDXRINFO = 0x000f,请求帧固定头 0c1f187600010b000b000f000100 + market + code
const TypeXdxr = 0x000f

var MXdxr = xdxr{}

// XdxrCategory 除权除息事件分类
const (
	XdxrCategory除权除息 uint8 = 1 //除权除息
	XdxrCategory送配股上市 uint8 = 2 //送配股上市
	XdxrCategory非流通股上市 uint8 = 3 //非流通股上市
	XdxrCategory未知股本变动 uint8 = 4 //未知股本变动
	XdxrCategory股本变化 uint8 = 5 //股本变化
	XdxrCategory增发新股 uint8 = 6 //增发新股
	XdxrCategory股份回购 uint8 = 7 //股份回购
	XdxrCategory增发新股上市 uint8 = 8 //增发新股上市
	XdxrCategory转配股上市 uint8 = 9 //转配股上市
	XdxrCategory可转债上市 uint8 = 10 //可转债上市
	XdxrCategory扩缩股 uint8 = 11 //扩缩股
	XdxrCategory非流通股缩股 uint8 = 12 //非流通股缩股
	XdxrCategory送认购权证 uint8 = 13 //送认购权证
	XdxrCategory送认沽权证 uint8 = 14 //送认沽权证
)

var XdxrCategoryName = map[uint8]string{
	1:  "除权除息",
	2:  "送配股上市",
	3:  "非流通股上市",
	4:  "未知股本变动",
	5:  "股本变化",
	6:  "增发新股",
	7:  "股份回购",
	8:  "增发新股上市",
	9:  "转配股上市",
	10: "可转债上市",
	11: "扩缩股",
	12: "非流通股缩股",
	13: "送认购权证",
	14: "送认沽权证",
}

// XdxrEvent 除权除息事件
type XdxrEvent struct {
	Date     time.Time //事件日期(除权除息日)
	Category uint8     //事件类别,1=除权除息

	//category=1 除权除息
	Fenhong     float32 //分红,每10股派息
	Peigujia    float32 //配股价
	Songzhuangu float32 //送转股,每10股送转
	Peigu       float32 //配股,每10股配股

	//category=11/12 扩缩股/非流通股缩股
	Suogu float32 //缩股比例

	//category=13/14 送认购权证/送认沽权证
	Xingquanjia float32 //行权价
	Fenshu      float32 //份数

	//category=2-10 股本变动类(经 getVolume 解码)
	Panqianliutong float64 //盘前流通盘
	Panhouliutong  float64 //盘后流通盘
	Qianzongguben  float64 //前总股本
	Houzongguben   float64 //后总股本
}

func (this *XdxrEvent) CategoryName() string {
	if name, ok := XdxrCategoryName[this.Category]; ok {
		return name
	}
	return fmt.Sprintf("%d", this.Category)
}

func (this *XdxrEvent) String() string {
	return fmt.Sprintf("%s %s fenhong=%v peigujia=%v songzhuangu=%v peigu=%v",
		this.Date.Format("2006-01-02"), this.CategoryName(),
		this.Fenhong, this.Peigujia, this.Songzhuangu, this.Peigu)
}

// XdxrResp 除权除息信息响应
type XdxrResp struct {
	Count uint16
	List  []*XdxrEvent
}

// ExDividendSince 返回 since 之后、且事件日不晚于 today 的除权除息(category=1)事件
// 公告在前、除权在后的"未来事件"不应提前失效缓存,故以 today 为上限
func (this *XdxrResp) ExDividendSince(since, today time.Time) []*XdxrEvent {
	ls := []*XdxrEvent(nil)
	if this == nil {
		return ls
	}
	for _, v := range this.List {
		if v.Category != XdxrCategory除权除息 {
			continue
		}
		if v.Date.After(since) && !v.Date.After(today) {
			ls = append(ls, v)
		}
	}
	return ls
}

type xdxr struct{}

/*
Frame 除权除息请求帧

对照 pytdx 固定头 0c1f187600010b000b000f000100:
Prefix: 0c
MsgID: 1f187600
Control: 01
Length: 0b00(11=数据域9+2)
Type: 0f00(0x000f)
Data: 0100 + market + code
*/
func (xdxr) Frame(code string) (*Frame, error) {
	if len(code) != 6 {
		return nil, errors.New("股票代码长度错误")
	}
	exchange, number, err := DecodeCode(code)
	if err != nil {
		return nil, err
	}
	data := []byte{0x01, 0x00}
	data = append(data, exchange.Uint8())
	data = append(data, []byte(number)...)
	return &Frame{
		Control: Control01,
		Type:    TypeXdxr,
		Data:    data,
	}, nil
}

/*
Decode 除权除息响应解析

跳过9字节 -> uint16 记录数
每条记录29字节: market+code(7) + 跳过1 + 日期(4,uint32 YYYYMMDD) + category(1) + 负载(16)
*/
func (xdxr) Decode(bs []byte) (*XdxrResp, error) {
	if len(bs) < 11 {
		//pytdx 在长度不足时返回空列表
		return &XdxrResp{}, nil
	}
	pos := 9
	resp := &XdxrResp{
		Count: Uint16(bs[pos : pos+2]),
	}
	pos += 2

	for i := 0; i < int(resp.Count); i++ {
		if pos+29 > len(bs) {
			break
		}
		pos += 7 //market(1) + code(6)
		pos += 1 //未知字节

		zipday := Uint32(bs[pos : pos+4])
		year := zipday / 10000
		month := (zipday % 10000) / 100
		day := zipday % 100
		pos += 4

		event := &XdxrEvent{
			Date:     time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.Local),
			Category: bs[pos],
		}
		pos += 1

		payload := bs[pos : pos+16]
		pos += 16

		switch event.Category {
		case XdxrCategory除权除息:
			//<ffff
			event.Fenhong = getFloat32(payload[0:4])
			event.Peigujia = getFloat32(payload[4:8])
			event.Songzhuangu = getFloat32(payload[8:12])
			event.Peigu = getFloat32(payload[12:16])
		case XdxrCategory扩缩股, XdxrCategory非流通股缩股:
			//<IIfI
			event.Suogu = getFloat32(payload[8:12])
		case XdxrCategory送认购权证, XdxrCategory送认沽权证:
			//<fIfI
			event.Xingquanjia = getFloat32(payload[0:4])
			event.Fenshu = getFloat32(payload[8:12])
		default:
			//<IIII,经 getVolume 解码
			event.Panqianliutong = getVolumeOrZero(payload[0:4])
			event.Qianzongguben = getVolumeOrZero(payload[4:8])
			event.Panhouliutong = getVolumeOrZero(payload[8:12])
			event.Houzongguben = getVolumeOrZero(payload[12:16])
		}

		resp.List = append(resp.List, event)
	}
	resp.Count = uint16(len(resp.List))
	return resp, nil
}

func getFloat32(bs []byte) float32 {
	return math.Float32frombits(Uint32(bs))
}

// getVolumeOrZero 与 pytdx _get_v 保持一致:原始值0时返回0
func getVolumeOrZero(bs []byte) float64 {
	v := Uint32(bs)
	if v == 0 {
		return 0
	}
	return getVolume(v)
}
