package dbhelper

import (
	"fmt"
	"testing"
)

func Test_decodeQuery(t *testing.T) {
	fmt.Printf("%#v", decodeQuery(`

DROP FUNCTION IF EXISTS grade_canuse
go
CREATE FUNCTION grade_canuse(current_grade text, canuse_grade text) RETURNS bit(1)
    DETERMINISTIC
BEGIN
	IF current_grade = '' THEN
	  RETURN true;
	END IF;
	IF current_grade like concat(canuse_grade,'%') THEN
	  RETURN true;
	END IF;
	RETURN false;
END`))
}
