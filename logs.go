package task_scheduler

import (
	"go.uber.org/zap"
)

type Logger interface {
	Debug(msg string, args ...Field)
	Info(msg string, args ...Field)
	Warn(msg string, args ...Field)
	Error(msg string, args ...Field)
}

type Field struct {
	Key string
	Val any
}

type ZapLogger struct {
	zap *zap.Logger
}

func NewZapLogger(zap *zap.Logger) Logger {
	return &ZapLogger{zap: zap}
}

func (z *ZapLogger) Debug(msg string, args ...Field) {
	z.zap.Debug(msg, z.toZapFields(args)...)
}

func (z *ZapLogger) Info(msg string, args ...Field) {
	z.zap.Info(msg, z.toZapFields(args)...)
}

func (z *ZapLogger) Warn(msg string, args ...Field) {
	z.zap.Warn(msg, z.toZapFields(args)...)
}

func (z *ZapLogger) Error(msg string, args ...Field) {
	z.zap.Error(msg, z.toZapFields(args)...)
}

func (z *ZapLogger) toZapFields(args []Field) []zap.Field {
	res := make([]zap.Field, 0, len(args))
	for _, arg := range args {
		res = append(res, zap.Any(arg.Key, arg.Val))
	}

	return res
}
