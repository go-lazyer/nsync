package constant

type ColumnType struct {
	Mysql       string
	Meilisearch string
	Manticore   string
}

// 支持的列类型映射 key未所有input数据库的类型集合，value为不同output数据源的类型
var ColumnTypes = map[string]ColumnType{
	"char":       {Mysql: "char", Meilisearch: "string"},
	"varchar":    {Mysql: "varchar", Meilisearch: "string"},
	"int":        {Mysql: "int", Meilisearch: "number"},
	"tinyint":    {Mysql: "tinyint", Meilisearch: "number"},
	"bigint":     {Mysql: "bigint", Meilisearch: "number"},
	"float":      {Mysql: "float", Meilisearch: "number"},
	"double":     {Mysql: "double", Meilisearch: "number"},
	"datetime":   {Mysql: "datetime", Meilisearch: "number"},
	"timestamp":  {Mysql: "timestamp", Meilisearch: "number"},
	"date":       {Mysql: "date", Meilisearch: "number"},
	"time":       {Mysql: "time", Meilisearch: "number"},
	"json":       {Mysql: "json", Meilisearch: "json"},
	"mediumtext": {Mysql: "mediumtext", Meilisearch: "json"},
	"text":       {Mysql: "text", Meilisearch: "json"},
}

// IsDateType 判断是否为日期类型
func IsDateType(t string) bool {
	switch t {
	case "datetime", "timestamp", "date", "time":
		return true
	}
	return false
}
func IsTextType(t string) bool {
	switch t {
	case "mediumtext", "text":
		return true
	}
	return false
}

func IsStringType(t string) bool {
	switch t {
	case "char", "varchar", "json", "mediumtext", "text":
		return true
	}
	return false
}
