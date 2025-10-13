package output

import (
	"fmt"
	"strings"
)

func CleanSpecialChars(data any) string {
	st := fmt.Sprintf("%v", data)
	//替换掉换行符
	st = strings.ReplaceAll(st, "<null>", "")
	st = strings.ReplaceAll(st, "<nil>", "")
	st = strings.ReplaceAll(st, "null", "")
	st = strings.ReplaceAll(st, "nil", "")
	st = strings.ReplaceAll(st, "\n", "")
	st = strings.ReplaceAll(st, "\r", "")
	st = strings.ReplaceAll(st, "\"", "")
	st = strings.ReplaceAll(st, "\\", "")
	st = strings.ReplaceAll(st, "'", "")
	st = strings.ReplaceAll(st, "|", " ")
	return st
}
