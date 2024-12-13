package repository

import (
	"context"
	"github.com/TimeWtr/task_scheduler/repository/dao"
)

type RepoPreempt interface {
	Preempt(ctx context.Context) (dao.Jobs, error)
}
