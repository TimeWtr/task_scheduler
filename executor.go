package task_scheduler

import "context"

type ExecutorFunc func(ctx context.Context, js JobSwap) error

// Executor 执行器抽象
type Executor interface {
	// Name Executor名称
	Name() string
	// Execute 执行方法
	Execute(ctx context.Context, exec ExecutorFunc) error
}
