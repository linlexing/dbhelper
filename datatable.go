// datatable project datatable.go
package dbhelper

import (
	"github.com/linlexing/datatable.go"
	"reflect"
)

type Index struct {
	Columns []string
	Unique  bool
	Desc    DBDesc
}

func (i *Index) Equal(value *Index) bool {
	return reflect.DeepEqual(i.Columns, value.Columns) &&
		i.Unique == value.Unique &&
		i.Desc.Equal(value.Desc)
}

func (i *Index) Clone() *Index {
	newColumns := make([]string, len(i.Columns))
	copy(newColumns, i.Columns)
	return &Index{
		Unique:  i.Unique,
		Columns: newColumns,
		Desc:    i.Desc.Clone(),
	}
}

type DataTable struct {
	*datatable.DataTable
	Columns []*DataColumn
	Indexes map[string]*Index
	Desc    DBDesc
	Temp    bool `json:"-"`
}

func NewDataTable(name string) *DataTable {
	return &DataTable{
		datatable.NewDataTable(name),
		nil,
		map[string]*Index{},
		DBDesc{},
		false,
	}
}
func (d *DataTable) Clone() *DataTable {
	cols := make([]*DataColumn, len(d.Columns))
	for i, v := range d.Columns {
		cols[i] = v.Clone()
	}
	indexes := map[string]*Index{}
	for i, v := range d.Indexes {
		indexes[i] = v.Clone()
	}
	return &DataTable{
		d.DataTable.Clone(),
		cols,
		indexes,
		d.Desc.Clone(),
		d.Temp,
	}
}

//Assign each column empty value pointer,General used by database/sql scan
func (d *DataTable) NewPtrValues() []interface{} {
	result := make([]interface{}, d.ColumnCount())
	for i, c := range d.Columns {
		result[i] = c.PtrZeroValue()
	}
	return result
}

func (d *DataTable) AddIndex(indexName string, index *Index) {
	d.Indexes[indexName] = index
}

func (d *DataTable) AddColumn(col *DataColumn) *DataColumn {

	d.DataTable.AddColumn(col.DataColumn)
	d.Columns = append(d.Columns, col)
	return col
}
