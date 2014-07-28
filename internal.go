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
	cols := table.ColumnNames()
	params := make([]string, table.ColumnCount())
	for i := 0; i < table.ColumnCount(); i++ {
		params[i] = "{{ph}}"
	}
	return fmt.Sprintf("INSERT INTO %s(\n\t%s)VALUES(\n\t%s)", table.TableName, strings.Join(cols, ",\n\t"), strings.Join(params, ",\n\t"))
}
func buildUpdateSql(table *DataTable) string {
	sets := make([]string, table.ColumnCount())
	wheres := make([]string, table.ColumnCount())
	for i := 0; i < table.ColumnCount(); i++ {
		sets[i] = fmt.Sprintf("%s = {{ph}}", table.Columns[i].Name)
		wheres = append(wheres, fmt.Sprintf("%s = {{ph}}", table.Columns[i].Name, table.ColumnCount()+i+1))
	}
	return fmt.Sprintf("UPDATE %s SET\n\t%s\nWHERE\n\t%s", table.TableName, strings.Join(sets, ",\n\t"), strings.Join(wheres, " AND\n\t"))

}
func buildDeleteSql(table *DataTable) string {
	params := make([]string, len(table.PK))
	for i, c := range table.PK {
		params[i] = fmt.Sprintf("%s = {{ph}}", c)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE\n\t%s", table.TableName, strings.Join(params, " AND\n\t"))

}
func buildSelectSql(table *DataTable) string {
	params := make([]string, len(table.PK))
	for i, c := range table.PK {
		params[i] = fmt.Sprintf("%s = {{ph}}", c)
	}
	return fmt.Sprintf("SELECT\n\t%s\nFROM\n\t%s\nWHERE\n\t%s", strings.Join(table.ColumnNames(), ",\n\t"), table.TableName, strings.Join(params, " AND\n\t"))

}
func internalUpdateTableTx(tx *sql.Tx, table *DataTable, pp func(string, map[string]interface{}) string) (rcount int64, result_err error) {
	changes := table.GetChange()
	if changes.RowCount == 0 {
		return
	}
	var stmt *sql.Stmt
	var result sql.Result
	var iCount int64
	if len(changes.DeleteRows) > 0 {
		strSql := buildDeleteSql(table)
		if stmt, result_err = tx.Prepare(pp(strSql, nil)); result_err != nil {
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
		if stmt, result_err = tx.Prepare(pp(strSql, nil)); result_err != nil {
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
		if stmt, result_err = tx.Prepare(pp(strSql, nil)); result_err != nil {
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
