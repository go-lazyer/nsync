package output

import (
	"reflect"
	"time"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
)

const (
	// 批量大小
	DefaultMaxBatchSize = 1
	// 批量等待时间
	DefaultMaxWaitDuration = time.Second * 5
)

// Output 通用输出接口
type Output interface {
	NewOutput(cfg *config.OutputConfig) error
	Buffer(event *input.Event) (string, error)
	Flush() error
	// FlushAll() error
}

func NewOutput(cfg *config.OutputConfig) (Output, error) {
	// if !reflect.ValueOf(cfg.Postgresql).IsZero() {
	// 	// 2. 创建输入实例
	// 	output := &PostgresqlOutput{}

	// 	err := output.NewOutput(cfg)

	// 	return output, err
	// }
	if !reflect.ValueOf(cfg.MeiliSearch).IsZero() {
		// 2. 创建输入实例
		output := &MeiliSearchOutput{}

		err := output.NewOutput(cfg)

		return output, err
	}
	if !reflect.ValueOf(cfg.Mysql).IsZero() {
		// 2. 创建输入实例
		output := &MysqlOutput{}

		err := output.NewOutput(cfg)

		return output, err
	}
	return nil, nil
}
