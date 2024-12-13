package task_scheduler

import (
	"context"
	"github.com/TimeWtr/task_scheduler/domain"
)

type Scheduler interface {
	Preempt(ctx context.Context) (domain.Jobs, error)
}
