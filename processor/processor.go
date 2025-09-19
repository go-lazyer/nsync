package processor

import (
	"context"
	"log"

	"github.com/go-lazyer/nsync/cleaning"
	"github.com/go-lazyer/nsync/filter"
	"github.com/go-lazyer/nsync/input"
	"github.com/go-lazyer/nsync/output"
)

type Processor struct {
	input    input.Input
	output   output.Output
	filter   *filter.Chain
	cleaning *cleaning.Cleaning
	doneChan chan struct{}
}

func NewProcessor(input input.Input, filter *filter.Chain, cleaning *cleaning.Cleaning, output output.Output) *Processor {
	return &Processor{
		input:    input,
		filter:   filter,
		cleaning: cleaning,
		output:   output,
		doneChan: make(chan struct{}), // 初始化通道
	}
}

func (p *Processor) Run(ctx context.Context) error {
	defer close(p.doneChan) // 确保结束时关闭通道
	eventChan := p.input.GetEvent()

	for event := range eventChan {
		// 过滤处理
		if !p.filter.Apply(event) {
			continue
		}
		err := p.cleaning.Cleaning(event)
		if err != nil {
			log.Fatal("Failed to buffer event:", err)
			return err
		}

		savePosition, err := p.output.Buffer(event)
		if err != nil {
			log.Fatal("Failed to buffer event:", err)
			return err
		}
		if savePosition != "" {
			p.input.SavePosition(savePosition)
		}
	}
	return nil
}

// processor.go
func (p *Processor) Shutdown(ctx context.Context) error {
	// 停止接收新事件
	p.input.CloseEvent()

	// 等待处理中的事件完成
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.doneChan: // 添加doneChan来通知处理完成
	}

	// // 刷新所有输出缓冲
	// p.output.Flush()
	// // 保存最终位置
	// p.input.SavePosition(p.input.GetPosition())
	return nil
}
