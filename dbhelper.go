package dbhelper

import (
	"database/sql"
	"fmt"
	"reflect"
)

type DBHelper struct {
	driverName     string
	dataSourceName string
	metaHelper     MetaHelper
	db             *sql.DB
	tx             *sql.Tx
}
type ParamPlaceholder func(strSql string, num int) string

func NewDBHelper(driverName, dataSourceName string, metaHelper MetaHelper) *DBHelper {
	fmt.Print(metaHelper)
	rev := &DBHelper{driverName, dataSourceName, metaHelper, nil, nil}
	metaHelper.SetDBHelper(rev)
	return rev
}
func (h *DBHelper) Open() error {
	if h.db != nil {
		return fmt.Errorf("already open")
	}
	db, err := sql.Open(h.driverName, h.dataSourceName)
	if err != nil {
		return err
	}
	h.db = db
	return nil
}
func (h *DBHelper) Close() error {
	if h.db == nil {
		return fmt.Errorf("the db not open")
	}
	err := h.db.Close()
	if err != nil {
		return err
	}
	h.db = nil
	return nil
}
func (h *DBHelper) Begin() error {
	if h.tx != nil {
		return fmt.Errorf("already begin trans")
	}
	if h.db == nil {
		return fmt.Errorf("db not open")
	}
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	h.tx = tx
	return nil
}
func (h *DBHelper) Commit() error {
	if h.tx == nil {
		return fmt.Errorf("the trans not begin")
	}
	err := h.tx.Commit()
	if err != nil {
		return err
	}
	h.tx = nil
	return nil
}
func (h *DBHelper) Rollback() error {
	if h.tx == nil {
		return fmt.Errorf("the trans not begin")
	}
	err := h.tx.Rollback()
	if err != nil {
		return err
	}
	h.tx = nil
	return nil
}
func (h *DBHelper) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if h.tx != nil {
		return h.tx.Query(h.metaHelper.ParamPlaceholder(query, len(args)), args...)
	} else {
		return h.db.Query(h.metaHelper.ParamPlaceholder(query, len(args)), args...)
	}
}
func (h *DBHelper) QueryRow(query string, args ...interface{}) *sql.Row {
	if h.tx != nil {
		return h.tx.QueryRow(h.metaHelper.ParamPlaceholder(query, len(args)), args...)
	} else {
		return h.db.QueryRow(h.metaHelper.ParamPlaceholder(query, len(args)), args...)
	}
}
func (h *DBHelper) Exists(Query string, args ...interface{}) (bool, error) {
	if rows, err := h.Query(Query, args...); err != nil {
		return false, err
	} else {
		defer rows.Close()
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return false, err
			}
			return false, nil
		} else {
			return true, nil
		}

	}
}
func (h *DBHelper) QueryOne(query string, args ...interface{}) (interface{}, error) {
	var row *sql.Row
	if h.tx != nil {
		row = h.tx.QueryRow(h.metaHelper.ParamPlaceholder(query, len(args)), args...)
	} else {
		row = h.db.QueryRow(h.metaHelper.ParamPlaceholder(query, len(args)), args...)
	}
	var rev interface{}
	err := row.Scan(&rev)
	return rev, err
}
func (h *DBHelper) Exec(query string, args ...interface{}) (sql.Result, error) {
	if h.tx != nil {
		return h.tx.Exec(h.metaHelper.ParamPlaceholder(query, len(args)), args...)
	} else {
		return h.db.Exec(h.metaHelper.ParamPlaceholder(query, len(args)), args...)
	}
}
func (h *DBHelper) Prepare(query string, argsnum int) (*sql.Stmt, error) {
	if h.tx != nil {
		return h.tx.Prepare(h.metaHelper.ParamPlaceholder(query, argsnum))
	} else {
		return h.db.Prepare(h.metaHelper.ParamPlaceholder(query, argsnum))
	}
}
func (h *DBHelper) FillTable(table *DataTable, query string, args ...interface{}) error {
	rows, err := h.Query(query, args)
	if err != nil {
		return err
	}
	_, err = internalRowsFillTable(rows, table, 0, false)
	return err
}
func (h *DBHelper) StepTable(table *DataTable, step int64, query string, args ...interface{}) (*StepTable, error) {
	rows, err := h.Query(query, args)
	if err != nil {
		return nil, err
	}
	return &StepTable{rows, table, step}, nil
}
func (h *DBHelper) GetData(query string, args ...interface{}) (*DataTable, error) {
	rows, err := h.Query(query, args)
	if err != nil {
		return nil, err
	}
	result := NewDataTable("table1")
	if rows.Next() {
		var vals []interface{}
		//创建表结构
		var cols []string
		if cols, err = rows.Columns(); err != nil {
			return nil, err
		}
		if vals, err = scanValues(rows, len(cols)); err != nil {
			return nil, err
		}
		for i, v := range vals {
			col, err := autoCreateColumn(cols[i], v)
			if err != nil {
				return nil, err
			}
			result.AddColumn(col)
		}
		_, err := internalRowsFillTable(rows, result, 0, true)
		if err != nil {
			return nil, err
		}
	}

	return result, err
}
func (h *DBHelper) DropTable(tablename string) error {
	return h.metaHelper.DropTable(tablename)
}
func (h *DBHelper) Table(tablename string) (*DataTable, error) {
	result := NewDataTable(tablename)
	var err error
	if exists, err := h.metaHelper.TableExists(tablename); err != nil {
		return nil, err
	} else if !exists {
		return nil, fmt.Errorf("the table %q not found", tablename)
	}
	//获取描述
	if result.Desc, err = h.metaHelper.GetTableDesc(tablename); err != nil {
		return nil, err
	}
	//获取字段

	columns, err := h.metaHelper.GetColumns(tablename)
	if err != nil {
		return nil, err
	}
	for _, col := range columns {
		aColumn := NewDataColumn(col.Name, col.Type, col.MaxSize, col.NotNull)
		aColumn.Desc = col.Desc
		result.AddColumn(aColumn)
	}
	//获取主键
	tPks, err := h.metaHelper.GetPrimaryKeys(tablename)
	if err != nil {
		return nil, err
	}
	if len(tPks) > 0 {
		result.SetPK(tPks...)
	}
	//获取索引
	tIndexes, err := h.metaHelper.GetIndexes(tablename)
	if err != nil {
		return nil, err
	}
	for _, oneIndex := range tIndexes {
		result.AddIndex(oneIndex.Name, &Index{oneIndex.Columns, oneIndex.Unique, oneIndex.Desc})
	}

	return result, nil
}
func (h *DBHelper) SaveChange(table *DataTable) (err error) {
	if h.tx == nil {
		if err = h.Begin(); err != nil {
			return
		}
		defer func() {
			if p := recover(); p != nil {
				switch p := p.(type) {
				case error:
					err = p
				default:
					err = fmt.Errorf("%s", p)
				}
			}
			if err != nil {
				h.tx.Rollback()
				return
			}
			err = h.tx.Commit()
		}()
	}
	_, err = internalUpdateTableTx(h.tx, table, h.metaHelper.ParamPlaceholder)
	return
}
func (p *DBHelper) UpdateStruct(oldStruct, newStruct *DataTable) error {
	if len(newStruct.TableName) == 0 {
		return fmt.Errorf("the table name is empty")
	}
	tablename := newStruct.TableName
	if oldStruct == nil {
		return p.metaHelper.CreateTable(newStruct)
	}

	//首先判断主关键字是否有变化
	bKeyChange := false
	if !reflect.DeepEqual(oldStruct.PK, newStruct.PK) {
		bKeyChange = true
	}
	if !bKeyChange {
		//判断主键的数据类型是否变化
		oldPks := oldStruct.PK
		newPks := newStruct.PK
		for i := 0; i < len(oldPks); i++ {
			if oldStruct.Columns[oldStruct.ColumnIndex(oldPks[i])].DataType !=
				newStruct.Columns[newStruct.ColumnIndex(newPks[i])].DataType ||
				oldStruct.Columns[oldStruct.ColumnIndex(oldPks[i])].MaxSize !=
					newStruct.Columns[newStruct.ColumnIndex(newPks[i])].MaxSize ||
				oldStruct.Columns[oldStruct.ColumnIndex(oldPks[i])].NotNull !=
					newStruct.Columns[newStruct.ColumnIndex(newPks[i])].NotNull {
				bKeyChange = true
				break
			}
		}
	}
	if bKeyChange && oldStruct.HasPrimaryKey() {
		//删除主键
		if err := p.metaHelper.DropPrimaryKey(tablename, oldStruct.PKConstraintName); err != nil {
			return err
		}
	}
	//找出相对应的一对字段
	oldColumns := oldStruct.Columns
	newColumns := []*DataColumn{}
	for _, v := range newStruct.Columns {
		newColumns = append(newColumns, v)
	}
	type FoundColumn struct {
		OldColumn *DataColumn
		NewColumn *DataColumn
	}
	foundColumns := []FoundColumn{}

	for _, vNew := range newColumns {
		trueNewName := vNew.Name

		if vNew.OriginName() != "" && vNew.Name != vNew.OriginName() {
			trueNewName = vNew.OriginName()
		}
		for _, vOld := range oldColumns {
			if vOld.Name == trueNewName {
				foundColumns = append(foundColumns, FoundColumn{vOld, vNew})
			}
		}
	}
	//删除字段
	for _, oldColumn := range oldColumns {
		bFound := false
		for _, foundColumn := range foundColumns {
			if oldColumn == foundColumn.OldColumn {
				bFound = true
				break
			}
		}
		//找不到的需要删除
		if !bFound {
			if err := p.metaHelper.DropColumn(tablename, oldColumn.Name); err != nil {
				return err
			}
		}
	}

	//修改字段类型或者重命名
	for _, column := range foundColumns {
		if err := p.metaHelper.AlterColumn(tablename,
			&TableColumn{column.OldColumn.Name, column.OldColumn.DataType, column.OldColumn.MaxSize, column.OldColumn.NotNull, column.OldColumn.Desc},
			&TableColumn{column.NewColumn.Name, column.NewColumn.DataType, column.NewColumn.MaxSize, column.NewColumn.NotNull, column.NewColumn.Desc}); err != nil {
			return err
		}
	}
	//新增字段
	for _, newColumn := range newColumns {
		bFound := false
		for _, foundColumn := range foundColumns {
			if newColumn == foundColumn.NewColumn {
				bFound = true
				break
			}
		}
		if !bFound {
			if err := p.metaHelper.AddColumn(tablename, &TableColumn{newColumn.Name, newColumn.DataType, newColumn.MaxSize, newColumn.NotNull, newColumn.Desc}); err != nil {
				return err
			}
		}
	}
	if bKeyChange && newStruct.HasPrimaryKey() {
		//创建主键
		if err := p.metaHelper.AddPrimaryKey(tablename, newStruct.PK); err != nil {
			return err
		}
	}
	//处理索引
	//删除不存在的,并修改存在的
	for idxName, oldIdx := range oldStruct.Indexes {
		if newIdx, ok := newStruct.Indexes[idxName]; ok {
			if !oldIdx.Equal(newIdx) {
				if err := p.metaHelper.AlterIndex(tablename, idxName, oldIdx, newIdx); err != nil {
					return err
				}
			}
		} else {
			if err := p.metaHelper.DropIndex(tablename, idxName); err != nil {
				return err
			}
		}
	}
	//新增索引
	for idxName, newIdx := range newStruct.Indexes {
		if _, ok := oldStruct.Indexes[idxName]; !ok {
			if err := p.metaHelper.CreateIndex(newStruct.TableName, idxName, newIdx.Columns, newIdx.Unique, newIdx.Desc); err != nil {
				return err
			}
		}
	}
	//处理表的描述
	if !oldStruct.Desc.Equal(newStruct.Desc) {
		if err := p.metaHelper.AlterTableDesc(newStruct.TableName, newStruct.Desc); err != nil {
			return err
		}
	}
	return nil
}
