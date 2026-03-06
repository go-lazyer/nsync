package filter

import (
	"github.com/go-lazyer/nsync/config"
)

type Databases struct {
	databases map[string]string
}

func NewDatabases(dbs []config.Database) (*Databases, error) {
	databases := make(map[string]string, 0)
	for _, schema := range dbs {
		databases[schema.Input] = schema.Output
	}
	return &Databases{databases}, nil
}
func (f *Databases) Apply(database, table string) (string, string, bool) {
	if len(f.databases) == 0 {
		return database, table, false
	}
	outputDatabase, ok := f.databases[database]
	if !ok || outputDatabase == "" {
		return database, table, false
	}
	return outputDatabase, table, true
}
