package dbhelper

import (
	"encoding/json"
	"reflect"
)

type DBDesc map[string]interface{}

func copyMap(src map[string]interface{}) map[string]interface{} {
	buf, err := json.Marshal(src)
	if err != nil {
		panic(err)
	}
	rev := map[string]interface{}{}
	err = json.Unmarshal(buf, &rev)
	if err != nil {
		panic(err)
	}
	return rev
}
func (p DBDesc) IsEmpty() bool {
	return len(p) == 0
}
func (p DBDesc) Clone() DBDesc {
	return copyMap(p)
}
func (p DBDesc) Equal(p1 DBDesc) bool {
	if len(p) != len(p1) {
		return false
	}
	return reflect.DeepEqual(p, p1)
}
func (p DBDesc) String() string {
	buf, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return string(buf)
}
func (p DBDesc) Parse(str string) {
	if str == "" {
		return
	}
	err := json.Unmarshal([]byte(str), &p)
	if err != nil {
		panic(err)
	}
}
