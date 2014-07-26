package dbhelper

import (
	"fmt"
	"github.com/linlexing/datatable.go"
	"reflect"
)

type DataColumn struct {
	*datatable.DataColumn
	Desc DBDesc
}

func (d *DataColumn) OriginName() string {
	switch tv := d.Desc["OriginName"].(type) {
	case string:
		return tv
	default:
		return ""
	}
}

//alloc empty value,return pointer the value
func (d *DataColumn) PtrValue() interface{} {
	defer func() {
		if f := recover(); f != nil {
			panic(fmt.Sprintf("%s,type:%s", f, d.StoreType()))
		}
	}()
	if d.NotNull {
		return reflect.New(d.ReflectType()).Interface()
	} else {
		var v interface{}
		return &v
	}
}
func (d *DataColumn) Clone() *DataColumn {
	return &DataColumn{d.DataColumn.Clone(), d.Desc.Clone()}
}
func NewDataColumn(name string, dataType datatable.ColumnType, maxsize int, notnull bool) *DataColumn {
	return &DataColumn{datatable.NewDataColumn(name, dataType, maxsize, notnull), DBDesc{}}
}
