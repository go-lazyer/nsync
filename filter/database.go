package filter

import (
	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
)

type Databases struct {
	databases map[string]string
}

func NewDatabases(conf []config.Database) (*Databases, error) {
	databases := make(map[string]string, 0)
	for _, schema := range conf {
		databases[schema.Input] = schema.Output
	}
	return &Databases{databases}, nil
}
func (f *Databases) Apply(event *input.Event) bool {
	if len(f.databases) == 0 {
		return true
	}
	outputSchema, ok := f.databases[event.SourceSchema]
	if !ok || outputSchema == "" {
		return false
	}
	event.Schema = outputSchema
	return true
}
