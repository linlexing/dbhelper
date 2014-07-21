package dbhelper

import (
	"fmt"
)

type SqlError struct {
	sql    string
	params interface{}
	err    error
}

func (s *SqlError) Error() string {
	return fmt.Sprintf("%v:\n%v\nparams:%v\n", s.err, s.sql, s.params)
}
func NewSqlError(strSql string, err error, params ...interface{}) *SqlError {
	return &SqlError{
		sql:    strSql,
		params: params,
		err:    err,
	}
}
