package task_scheduler

import (
	"context"
	_const "github.com/TimeWtr/task_scheduler/const"
	"golang.org/x/sync/semaphore"
	"time"
)

type Scheduler interface {
	// Scheduler 开启调度
	Scheduler(ctx context.Context) error
	// Register 注册执行器方法
	Register(name string, executorFunc ExecutorFunc) error
}

type Options func(core *SchedulerCore)

func WithPreemptStrategy(strategy _const.PreemptStrategy) Options {
	return func(c *SchedulerCore) {
		c.schedulerStrategy = strategy
	}
}

func WithRetryStrategy(retry RetryStrategy) Options {
	return func(c *SchedulerCore) {
		c.retry = retry
	}
}

// WithLimiter 设置节点并发调度执行的Job数量，防止无限制的抢占
func WithLimiter(limiter int64) Options {
	return func(c *SchedulerCore) {
		c.limiter = semaphore.NewWeighted(limiter)
	}
}

type SchedulerCore struct {
	logger Logger
	// 本地的执行器注册中心
	execCenter map[string]ExecutorFunc
	// 抢占
	pmt Preempt
	// 抢占策略
	schedulerStrategy _const.PreemptStrategy
	// 抢占失败的重试策略
	retry RetryStrategy
	// 限流
	limiter *semaphore.Weighted
	// 本地缓存，缓存距离下次执行时间间隔很短的Job，不放回数据库
	cache Cache
}

func NewSchedulerCore(
	pmt Preempt,
	schedulerStrategy _const.PreemptStrategy,
	logger Logger,
	opts ...Options) Scheduler {
	scheduler := &SchedulerCore{
		execCenter:        map[string]ExecutorFunc{},
		pmt:               pmt,
		schedulerStrategy: schedulerStrategy,
		logger:            logger,
	}

	for _, opt := range opts {
		opt(scheduler)
	}

	if scheduler.limiter == nil {
		scheduler.limiter = semaphore.NewWeighted(_const.DefaultLimiter)
	}

	return scheduler
}

func (s *SchedulerCore) Scheduler(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 抢占任务
		var js JobSwap
		var err error

		err = s.limiter.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		// 先从本地缓存获取Job，如果可以直接执行

		switch s.schedulerStrategy {
		case _const.TryPreemptStrategy:
			lctx, cancel := context.WithTimeout(ctx, time.Second)
			js, err = s.pmt.TryPreempt(lctx)
			cancel()
			if err != nil {
				// 抢占失败，等待下一轮调度
				s.logger.Error("failed to try preempt task", Field{
					Key: "err",
					Val: err.Error(),
				})
				continue
			}
		case _const.RetryPreemptStrategy:
			lctx, cancel := context.WithTimeout(ctx, time.Second)
			js, err = s.pmt.Preempt(lctx, s.retry)
			cancel()
			if err != nil {
				s.logger.Error("failed to preempt task", Field{
					Key: "err",
					Val: err.Error(),
				})
				continue
			}
		}

		// 开启一个goroutine自动续约
		errCh := make(chan error)
		go func() {
			select {
			case <-ctx.Done():
				s.logger.Error("context canceled", Field{
					Key: "err",
					Val: ctx.Err().Error(),
				})
				return
			default:
			}
			lctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			err1 := js.AutoRefresh(lctx, 5*time.Second)
			cancel()
			if err1 != nil {
				s.logger.Error("failed to auto refresh task", Field{
					Key: "err",
					Val: err1.Error(),
				})
				errCh <- err1
			}
		}()

		// 执行器执行Job
		select {
		case err = <-errCh:
		default:
			execFunc, ok := s.execCenter[js.JobName]
			if !ok {
				// 没有执行程序
				s.logger.Error("failed to find executor function", Field{
					Key: "job",
					Val: js.JobName,
				})
			}

			go func() {
				defer s.limiter.Release(1)
				defer func() {
					err1 := js.Release()
					if err1 != nil {
						s.logger.Error("failed to release job", Field{
							Key: "job",
							Val: js.JobName,
						})
					}
				}()

				lctx, cancel := context.WithTimeout(ctx, 20*time.Second)
				err1 := execFunc(lctx, js)
				cancel()
				if err1 != nil {
					s.logger.Error("failed to execute job", Field{
						Key: "job",
						Val: js.JobName,
					})
				}
			}()
		}

		// 重置下次调度执行的时间

	}
}

func (s *SchedulerCore) Register(name string, executorFunc ExecutorFunc) error {
	if _, ok := s.execCenter[name]; ok {
		s.execCenter[name] = executorFunc
	}
	return nil
}
