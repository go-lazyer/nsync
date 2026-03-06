package config

import (
	"github.com/BurntSushi/toml"
)

type InputMysql struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
}
type InputConfig struct {
	Mysql InputMysql `toml:"mysql"`
}

type OutputPostgresql struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	Database string `toml:"database"`
}
type OutputMysql struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	Database string `toml:"database"`
}

type OutputManticore struct {
	Host      string `toml:"host"`
	Port      int    `toml:"port"`
	HttpPort  int    `toml:"http_port"`
	Username  string `toml:"username"`
	Password  string `toml:"password"`
	Database  string `toml:"database"`
	BatchSize int    `toml:"batch_size"`
}
type OutputTypesense struct {
	Host      string `toml:"host"`
	Port      int    `toml:"port"`
	Apikey    string `toml:"apikey"`
	BatchSize int    `toml:"batch_size"`
}

type OutputStarrocks struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	Database string `toml:"database"`
}
type OutputMeiliSearch struct {
	Origin      string `toml:"origin"`
	AdminApiKey string `toml:"admin_api_key"`
}
type OutputConfig struct {
	Postgresql  OutputPostgresql  `toml:"postgresql"`
	Mysql       OutputMysql       `toml:"mysql"`
	Starrocks   OutputStarrocks   `toml:"starrocks"`
	MeiliSearch OutputMeiliSearch `toml:"meilisearch"`
	Manticore   OutputManticore   `toml:"manticore"`
	Typesense   OutputTypesense   `toml:"typesense"`
}

type Database struct {
	Input  string `toml:"input"`
	Output string `toml:"output"`
}
type Table struct {
	Input  string `toml:"input"`
	Output string `toml:"output"`
}
type Filter struct {
	Databases []Database `toml:"databases"`
	Tables    []Table    `toml:"tables"`
}

type Config struct {
	Input  InputConfig  `toml:"input"`
	Filter Filter       `toml:"filter"`
	Output OutputConfig `toml:"output"`
}

func LoadConfig(path string) (*Config, error) {
	var config Config
	if _, err := toml.DecodeFile(path, &config); err != nil {
		panic(err)
	}
	return &config, nil
}
