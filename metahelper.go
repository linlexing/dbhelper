package dbhelper

import (
	"fmt"
	"github.com/linlexing/datatable.go"
)

type TableColumn struct {
	Name    string
	Type    datatable.ColumnType
	MaxSize int
	NotNull bool
	Desc    DBDesc
}
type TableIndex struct {
	Name    string
	Columns []string
	Unique  bool
	Desc    DBDesc
}
type RootMeta struct {
	DBHelper *DBHelper
}

func (r *RootMeta) SetDBHelper(h *DBHelper) {
	r.DBHelper = h
	return
}
func (r *RootMeta) DropTable(tablename string) error {
	_, err := r.DBHelper.Exec(fmt.Sprintf("DROP TABLE %s", tablename))
	return err
}
func (r *RootMeta) DropColumn(table, column string) error {
	_, err := r.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, column))
	return err
}

type MetaHelper interface {
	SetDBHelper(helper *DBHelper)

	StringExpress(value string) string
	ParamPlaceholder(num int) string
	RegLike(value, strRegexp string) string
	StringCat(values ...string) string

	TableExists(tablename string) (bool, error)
	DropTable(tablename string) error
	DropPrimaryKey(tablename string) error
	DropColumn(table, column string) error
	DropIndex(tablename, indexname string) error

	AlterColumn(tablename string, oldColumn, newColumn *TableColumn) error
	AlterTableDesc(tablename string, desc DBDesc) error
	AlterIndex(tablename, indexname string, oldIndex, newIndex *Index) error

	CreateTable(table *DataTable) error
	AddColumn(tablename string, column *TableColumn) error
	AddPrimaryKey(tablename string, pks []string) error
	CreateIndex(tableName, indexName string, columns []string, unique bool, desc DBDesc) error

	GetTableDesc(tablename string) (DBDesc, error)
	GetIndexes(tablename string) ([]*TableIndex, error)
	GetColumns(tablename string) ([]*TableColumn, error)
	GetPrimaryKeys(tablename string) ([]string, error)
	Merge(dest, source string, colNames []string, pkColumns []string, autoRemove bool, sqlWhere string) error
}
