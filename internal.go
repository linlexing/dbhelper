package dbhelper

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/linlexing/datatable.go"
	"reflect"
	"strings"
	"time"
)

var (
	ERROR_ColumnNumberError = errors.New("the table column number <> scan column number!")
)

func ERROR_ColumnNotFound(tabColName string) error {
	return fmt.Errorf("the column [%s] not found", tabColName)
}
func buildInsertSql(table *DataTable) string {
	cols := []string{}
	params := []string{}
	for i := 0; i < table.ColumnCount(); i++ {
		cols = append(cols, table.Columns[i].Name)
		params = append(params, "%s")
	}
	return fmt.Sprintf("INSERT INTO %v(%v)VALUES(%v)", table.TableName, strings.Join(cols, ","), strings.Join(params, ","))
}
func buildUpdateSql(table *DataTable) string {
	sets := []string{}
	wheres := []string{}
	for i := 0; i < table.ColumnCount(); i++ {
		sets = append(sets, fmt.Sprintf("%v = %%s", table.Columns[i].Name))
		wheres = append(wheres, fmt.Sprintf("%v = %%s", table.Columns[i].Name))
	}
	return fmt.Sprintf("UPDATE %v SET %v WHERE %v", table.TableName, strings.Join(sets, ","), strings.Join(wheres, " AND "))

}
func buildDeleteSql(table *DataTable) string {
	params := []string{}
	for _, c := range table.PK {
		params = append(params, fmt.Sprintf("%v = %%s", c))
	}
	return fmt.Sprintf("DELETE FROM %v WHERE %v", table.TableName, strings.Join(params, " AND "))

}
func buildSelectSql(table *DataTable) string {
	params := []string{}
	for _, c := range table.PK {
		params = append(params, fmt.Sprintf("%v = %%s", c))
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %v", strings.Join(table.ColumnNames(), ","), table.TableName, strings.Join(params, " AND "))

}
func internalUpdateTableTx(tx *sql.Tx, table *DataTable, pp ParamPlaceholder) (rcount int64, result_err error) {
	changes := table.GetChange()
	if changes.RowCount == 0 {
		return
	}
	var stmt *sql.Stmt
	var result sql.Result
	var iCount int64
	if len(changes.DeleteRows) > 0 {
		strSql := buildDeleteSql(table)
		if stmt, result_err = tx.Prepare(pp(strSql, table.ColumnCount())); result_err != nil {
			result_err = NewSqlError(strSql, result_err)
			return
		}
		for _, r := range changes.DeleteRows {
			if result, result_err = stmt.Exec(r.OriginData...); result_err != nil {
				result_err = NewSqlError(strSql, result_err, r.OriginData...)
				return
			}
			if iCount, result_err = result.RowsAffected(); result_err != nil {
				return
			}
			rcount += iCount

		}
	}
	if len(changes.UpdateRows) > 0 {
		strSql := buildUpdateSql(table)
		if stmt, result_err = tx.Prepare(pp(strSql, 2*table.ColumnCount())); result_err != nil {
			result_err = NewSqlError(strSql, result_err)
			return
		}
		for _, r := range changes.UpdateRows {
			if result, result_err = stmt.Exec(append(r.Data, r.OriginData...)...); result_err != nil {
				result_err = NewSqlError(strSql, result_err, append(r.Data, r.OriginData...)...)
				return
			}
			if iCount, result_err = result.RowsAffected(); result_err != nil {
				return
			}
			rcount += iCount
		}
	}

	if len(changes.InsertRows) > 0 {
		strSql := buildInsertSql(table)
		if stmt, result_err = tx.Prepare(pp(strSql, table.ColumnCount())); result_err != nil {
			result_err = NewSqlError(strSql, result_err)
			return
		}
		for _, r := range changes.InsertRows {
			if _, result_err = stmt.Exec(r.Data...); result_err != nil {
				result_err = NewSqlError(strSql, result_err, r.Data...)
				return
			}
			rcount += 1
		}
	}
	return
}

func internalRowsFillTable(rows *sql.Rows, table *DataTable, maxRow int64, firstRead bool) (eof bool, err error) {
	//先建立实际字段与扫描字段的顺序对应关系
	var cols []string

	if cols, err = rows.Columns(); err != nil {
		return
	}
	if len(cols) != table.ColumnCount() {
		err = ERROR_ColumnNumberError
		return
	}
	//scan index --> table column index
	trueIndex := make([]int, table.ColumnCount())
	for tabColIdx, tabColName := range table.ColumnNames() {
		bfound := false
		for scanColIdx, scanColName := range cols {
			if tabColName == scanColName {
				bfound = true
				trueIndex[scanColIdx] = tabColIdx
				break
			}
		}
		if !bfound {
			return false, ERROR_ColumnNotFound(tabColName)
		}
	}
	rowIndex := int64(0)
	eof = true
	bNext := true
	if !firstRead {
		bNext = rows.Next()
	}
	for bNext {
		if maxRow > 0 && rowIndex == maxRow {
			eof = false
			break
		}
		rowIndex++
		tabVals := table.NewPtrValues()
		//reorder vals
		vals := make([]interface{}, len(tabVals))
		for i, _ := range tabVals {
			vals[i] = tabVals[trueIndex[i]]
		}
		if err = rows.Scan(vals...); err != nil {
			return
		}
		valsToAdd := make([]interface{}, len(vals))
		for scanColIdx, tabColIdx := range trueIndex {
			valsToAdd[tabColIdx] = reflect.ValueOf(vals[scanColIdx]).Elem().Interface()
		}

		if err = table.AddValues(valsToAdd...); err != nil {
			return
		}
		bNext = rows.Next()
	}
	table.AcceptChange()
	return
}
func autoCreateColumn(cname string, value interface{}) (*DataColumn, error) {
	var colType datatable.ColumnType
	switch value.(type) {
	case nil, []byte, string:
		colType = datatable.String
	case int64:
		colType = datatable.Int64
	case float64:
		colType = datatable.Float64
	case time.Time:
		colType = datatable.Time
	case bool:
		colType = datatable.Bool
	default:
		return nil, fmt.Errorf("the column %q value is :%v(%T),can't auto create column", cname, value, value)
	}
	return NewDataColumn(cname, colType, 0, false), nil
}
func scanValues(r *sql.Rows, num int) ([]interface{}, error) {
	vals := make([]interface{}, num, num)
	pvals := make([]interface{}, num, num)
	for i, _ := range vals {
		pvals[i] = &vals[i]
	}
	if err := r.Scan(pvals...); err != nil {
		return nil, err
	}
	return vals, nil
}
