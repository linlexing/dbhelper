package dbhelper

import (
	"fmt"
	"github.com/linlexing/datatable.go"
	"strings"
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

type orderField struct {
	Field    string
	SortType string
	Value    interface{}
}

func buildWhere(orderby []*orderField, lastValues []interface{}) (string, []interface{}) {
	if len(orderby) == 0 {
		panic(fmt.Errorf("orderby can't is empty"))
	}
	var opt string
	if orderby[0].SortType == "DESC" {
		opt = "<"
	} else {
		opt = ">"
	}
	result := fmt.Sprintf("\t%s %s {{ph}}", orderby[0].Field, opt)
	lastValues = append(lastValues, orderby[0].Value)
	if len(orderby) > 1 {
		var str string
		lastValues = append(lastValues, orderby[0].Value)
		str, lastValues = buildWhere(orderby[1:], lastValues)
		result = fmt.Sprintf("(%s or (%s = {{ph}} and %s))", result, orderby[0].Field, str)
	}
	return result, lastValues
}
func (r *RootMeta) BuildSelectLimitSql(srcSql string, pkFields []string, startKeyValue map[string]interface{}, selectCols []string, where string, orderby []string, limit int) (string, []interface{}) {
	orderbyArr, lstvalWhere, lstval := func(pk, orderby []string, startKeyValue map[string]interface{}) ([]string, string, []interface{}) {
		usedPk := map[string]bool{}
		result := make([]string, len(orderby))

		for i, v := range orderby {
			vs := strings.Split(v, " ")
			colName := vs[0]
			for _, sv := range pk {
				if sv == colName {
					usedPk[sv] = true
					break
				}
			}
			result[i] = v
		}
		//add pk column if not include
		for _, v := range pk {
			if _, ok := usedPk[v]; !ok {
				result = append(result, v)
			}
		}
		var where string
		var params []interface{}
		if len(startKeyValue) > 0 {
			orderFields := make([]*orderField, len(result))
			params = []interface{}{}
			for i, v := range result {
				vs := strings.Split(v, " ")
				var stype string
				if len(vs) == 2 && vs[1] == "DESC" {
					stype = "DESC"
				} else {
					stype = "ASC"
				}
				orderFields[i] = &orderField{vs[0], stype, startKeyValue[vs[0]]}
			}
			where, params = buildWhere(orderFields, params)
		}
		return result, where, params
	}(pkFields, orderby, startKeyValue)

	selectStr := ""
	selectArr := make([]string, len(selectCols))
	whereStr := ""
	orderbyStr := ""
	for i, v := range selectCols {
		selectArr[i] = "\t" + v
	}
	if len(selectArr) > 0 {
		selectStr = strings.Join(selectArr, ",\n")
	} else {
		selectStr = "\t*"
	}
	if len(orderby) > 0 {
		orderbyStr = "\norder by\n"
		for i, v := range orderbyArr {
			orderbyStr += "\t" + v
			if i < len(orderbyArr)-1 {
				orderbyStr += ",\n"
			}
		}
	}
	if where != "" {
		whereStr = "\nwhere\n\t(" + where + ")"
	}
	if len(lstvalWhere) > 0 {
		if whereStr == "" {
			whereStr = "\nwhere\n" + lstvalWhere
		} else {
			whereStr = where + " and\n" + lstvalWhere
		}
	}
	return fmt.Sprintf(
		"select\n%s\nfrom\n\t(%s) sellmt %s%s\nlimit %d",
		selectStr,
		srcSql,
		whereStr,
		orderbyStr,
		limit), lstval
}

type MetaHelper interface {
	SetDBHelper(helper *DBHelper)
	BuildSelectLimitSql(srcSql string, pkFields []string, startKeyValue map[string]interface{}, selectCols []string, where string, orderby []string, limit int) (string, []interface{})

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
	Merge(dest, source string, colNames []string, pkColumns []string, autoUpdate, autoRemove bool, sqlWhere string) error
}
