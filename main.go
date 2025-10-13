package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-lazyer/nsync/cleaning"
	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/filter"
	"github.com/go-lazyer/nsync/input"
	"github.com/go-lazyer/nsync/output"
	"github.com/go-lazyer/nsync/processor"
	"github.com/pingcap/errors"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "./nsync.toml", "path to config file")
	flag.Parse()

	// 加载配置
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	// 初始化输入源
	input, err := input.NewInput(&cfg.Input)
	if err != nil {
		log.Fatalf("Failed to init input: %v", err)
	}

	// 初始化过滤器链
	filterChain, err := filter.NewChain(cfg)
	if err != nil {
		log.Fatalf("Failed to init filter: %v", err)
	}

	// 初始化清洗器
	cleaning := cleaning.NewCleaning()

	output, err := output.NewOutput(&cfg.Output)
	if err != nil {
		log.Fatalf("Failed to init  output: %v", err)
	}

	// 创建处理器,真实处理数据的
	processor := processor.NewProcessor(
		input,
		filterChain,
		cleaning,
		output,
	)
	// 创建上下文用于优雅关机
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动处理器
	go func() {
		defer func() {
			if erro := recover(); erro != nil {
				err = errors.Errorf("%v", erro) //可以把recover返回的信息抛给调用着
				return
			}
		}()
		if err := processor.Run(ctx); err != nil {
			log.Fatalf("Processor failed: %v", err)
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	// 启动优雅关机
	cancel()

	// 等待资源清理
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// 确保所有数据刷新并保存位置
	processor.Shutdown(shutdownCtx)

	log.Println("Shutdown completed")
}
