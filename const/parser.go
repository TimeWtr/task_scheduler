package _const

import (
	"github.com/robfig/cron/v3"
)

// Parser 定时时间解析器
var Parser = cron.NewParser(cron.Minute | cron.Hour |
	cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
