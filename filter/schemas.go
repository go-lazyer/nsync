package filter

import (
	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
)

type Schemas struct {
	schemas map[string]string
}

func NewSchemas(conf []config.Schema) (*Schemas, error) {
	schemas := make(map[string]string, 0)
	for _, schema := range conf {
		schemas[schema.Input] = schema.Output
	}
	return &Schemas{schemas}, nil

}
func (f *Schemas) Apply(event *input.Event) bool {
	if len(f.schemas) == 0 {
		return true
	}
	outputSchema, ok := f.schemas[event.SourceSchema]
	if !ok || outputSchema == "" {
		return false
	}
	event.Schema = outputSchema
	return true
}
