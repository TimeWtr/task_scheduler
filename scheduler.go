package task_scheduler

import (
	"context"
	_const "github.com/TimeWtr/task_scheduler/const"
	"github.com/TimeWtr/task_scheduler/domain"
)

type Scheduler interface {
	// Scheduler 开启调度
	Scheduler(ctx context.Context) (domain.Jobs, error)
	// Register 注册执行器方法
	Register(ctx context.Context, name string, executorFunc ExecutorFunc) error
}

type SchedulerCore struct {
	// 本地的执行器注册中心
	execCenter map[string]ExecutorFunc
	// 抢占
	pmt Preempt
	// 抢占策略
	schedulerStrategy _const.PreemptStrategy
}

func NewSchedulerCore(pmt Preempt, schedulerStrategy _const.PreemptStrategy) Scheduler {
	return &SchedulerCore{
		execCenter:        map[string]ExecutorFunc{},
		pmt:               pmt,
		schedulerStrategy: schedulerStrategy,
	}
}

func (s *SchedulerCore) Scheduler(ctx context.Context) (domain.Jobs, error) {
	//TODO implement me
	panic("implement me")
}

func (s *SchedulerCore) Register(ctx context.Context, name string, executorFunc ExecutorFunc) error {
	if _, ok := s.execCenter[name]; ok {
		s.execCenter[name] = executorFunc
	}
	return nil
}
