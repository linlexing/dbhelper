package dbhelper

import (
	"github.com/linlexing/datatable.go"
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

func (d *DataColumn) Clone() *DataColumn {
	return &DataColumn{d.DataColumn.Clone(), d.Desc.Clone()}
}
func NewDataColumn(name string, dataType datatable.ColumnType, maxsize int, notnull bool) *DataColumn {
	return &DataColumn{datatable.NewDataColumn(name, dataType, maxsize, notnull), DBDesc{}}
}
