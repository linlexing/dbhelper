package dbhelper

import (
	"database/sql"
)

type StepTable struct {
	rows    *sql.Rows
	table   *DataTable
	StepNum int64
}

func (s *StepTable) Step() (*DataTable, bool, error) {
	s.table.Clear()
	eof, err := internalRowsFillTable(s.rows, s.table, s.StepNum, false)
	return s.table, eof, err
}
func (s *StepTable) Close() error {
	return s.rows.Close()
}
