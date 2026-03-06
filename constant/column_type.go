package constant

type ColumnType struct {
	Mysql       string
	Meilisearch string
	Manticore   string
	Default     any
}

const (
	COLUMN_TYPE_STRING     = "string"
	COLUMN_TYPE_INT32      = "int32"
	COLUMN_TYPE_INT64      = "int64"
	COLUMN_TYPE_FLOAT32    = "float32"
	COLUMN_TYPE_FLOAT64    = "float64"
	COLUMN_TYPE_DATETIME   = "datetime"
	COLUMN_TYPE_TIMESTAMP  = "timestamp"
	COLUMN_TYPE_DATE       = "date"
	COLUMN_TYPE_TIME       = "time"
	COLUMN_TYPE_JSON       = "json"
	COLUMN_TYPE_TEXT       = "text"
	COLUMN_TYPE_INT32ARR   = "int32[]"
	COLUMN_TYPE_INT64ARR   = "int64[]"
	COLUMN_TYPE_FLOAT32ARR = "float32[]"
	COLUMN_TYPE_FLOAT64ARR = "float64[]"
	COLUMN_TYPE_STRINGARR  = "string[]"
	COLUMN_TYPE_GEOPOINT   = "geopoint"
)

// 支持的列类型映射 key未所有input数据库的类型集合，value为不同output数据源的类型
var NsyncColumnTypes = map[string]ColumnType{
	COLUMN_TYPE_STRING:    {Default: "", Mysql: "varchar", Meilisearch: "string"},
	COLUMN_TYPE_INT32:     {Default: 0, Mysql: "tinyint", Meilisearch: "number"},
	COLUMN_TYPE_INT64:     {Default: 0, Mysql: "bingint", Meilisearch: "number"},
	COLUMN_TYPE_FLOAT32:   {Default: 0, Mysql: "float", Meilisearch: "number"},
	COLUMN_TYPE_FLOAT64:   {Default: 0, Mysql: "double", Meilisearch: "number"},
	COLUMN_TYPE_DATETIME:  {Default: 0, Mysql: "datetime", Meilisearch: "number"},
	COLUMN_TYPE_TIMESTAMP: {Default: 0, Mysql: "timestamp", Meilisearch: "number"},
	COLUMN_TYPE_DATE:      {Default: 0, Mysql: "date", Meilisearch: "number"},
	COLUMN_TYPE_TIME:      {Default: 0, Mysql: "time", Meilisearch: "number"},
	COLUMN_TYPE_JSON:      {Default: "", Mysql: "json", Meilisearch: "json"},
	COLUMN_TYPE_TEXT:      {Default: "", Mysql: "text", Meilisearch: "text"},
	COLUMN_TYPE_INT32ARR:  {Default: []int32{}, Mysql: "string", Meilisearch: "string"},       //多值整数属性
	COLUMN_TYPE_INT64ARR:  {Default: []int64{}, Mysql: "string", Meilisearch: "string"},       //多值整数属性
	COLUMN_TYPE_STRINGARR: {Default: []string{}, Mysql: "string", Meilisearch: "string"},      //多值整数属性
	COLUMN_TYPE_GEOPOINT:  {Default: []float64{0, 0}, Mysql: "string", Meilisearch: "string"}, //多值整数属性
}
var MysqlColumnType = map[string]string{
	"char":       COLUMN_TYPE_STRING,
	"varchar":    COLUMN_TYPE_STRING,
	"int":        COLUMN_TYPE_INT32,
	"tinyint":    COLUMN_TYPE_INT32,
	"bigint":     COLUMN_TYPE_INT64,
	"float":      COLUMN_TYPE_FLOAT32,
	"double":     COLUMN_TYPE_FLOAT64,
	"datetime":   COLUMN_TYPE_DATETIME,
	"timestamp":  COLUMN_TYPE_TIMESTAMP,
	"date":       COLUMN_TYPE_DATE,
	"time":       COLUMN_TYPE_TIME,
	"json":       COLUMN_TYPE_JSON,
	"mediumtext": COLUMN_TYPE_TEXT,
	"text":       COLUMN_TYPE_TEXT,
}

var TypesenseColumnType = map[string]string{
	"string":   COLUMN_TYPE_STRING,
	"string[]": COLUMN_TYPE_STRINGARR,
	"int32":    COLUMN_TYPE_INT32,
	"int32[]":  COLUMN_TYPE_INT32ARR,
	"int64":    COLUMN_TYPE_INT64,
	"int64[]":  COLUMN_TYPE_INT64ARR,
	"float":    COLUMN_TYPE_FLOAT64,
	"float[]":  COLUMN_TYPE_FLOAT64ARR,
	"geopoint": COLUMN_TYPE_GEOPOINT,
}

// IsDateType 判断是否为日期类型
func IsDateType(t string) bool {
	switch t {
	case COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_DATETIME, COLUMN_TYPE_DATE, COLUMN_TYPE_TIME:
		return true
	}
	return false
}
func IsTextType(t string) bool {
	switch t {
	case COLUMN_TYPE_TEXT:
		return true
	}
	return false
}

func IsStringType(t string) bool {
	switch t {
	case COLUMN_TYPE_STRING, COLUMN_TYPE_JSON, COLUMN_TYPE_TEXT:
		return true
	}
	return false
}
func IsMultiType(t string) bool {
	switch t {
	case COLUMN_TYPE_STRINGARR, COLUMN_TYPE_INT32ARR, COLUMN_TYPE_INT64ARR:
		return true
	}
	return false
}
