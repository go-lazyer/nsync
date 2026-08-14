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

	// 清洗器在 main 中初始化，按业务拆分保留在调用层
	cleaning := cleaning.NewCleaning()
	cleaning.AddCleaner(nil)

	// 创建处理器，内部统一初始化 input / output / filter
	proc, err := processor.NewProcessor(cfg, cleaning)
	if err != nil {
		log.Fatalf("Failed to init processor: %v", err)
	}

	// 启动处理器
	go func() {
		defer func() {
			if erro := recover(); erro != nil {
				err = errors.Errorf("%v", erro) //可以把recover返回的信息抛给调用着
				return
			}
		}()
		if err := proc.Run(); err != nil {
			log.Fatalf("Processor failed: %v", err)
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	// 等待资源清理
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// 确保所有数据刷新并保存位置
	proc.Shutdown(shutdownCtx)

	log.Println("Shutdown completed")
}
