package dao

import (
	"context"
	"errors"
	"github.com/TimeWtr/task_scheduler"
	"github.com/TimeWtr/task_scheduler/const"
	"gorm.io/gorm"
	"time"
)

var ErrPreemptFailed = errors.New("preempt failed")

type Preempt interface {
	// TryPreempt 尝试调度一次
	TryPreempt(ctx context.Context) (Jobs, error)
	// Preempt 调度失败会进行重试
	Preempt(ctx context.Context, strategy task_scheduler.FixedScheduleStrategy) (Jobs, error)
}

type JobPreempt struct {
	db *gorm.DB
}

func NewJobPreempt(db *gorm.DB) Preempt {
	return &JobPreempt{db: db}
}

func (j *JobPreempt) TryPreempt(ctx context.Context) (Jobs, error) {
	var jobs Jobs
	// 状态位执行中且执行时间小于等于当前时间的
	err := j.db.WithContext(ctx).Where("status = ? AND next_exec_time <= ?",
		_const.SchedulerStatusWaiting, time.Now().Unix()).First(&jobs).Error
	if err != nil {
		return Jobs{}, err
	}

	res := j.db.WithContext(ctx).Where("id = ? AND epoch = ?", jobs.ID, jobs.Epoch).
		Updates(map[string]interface{}{
			"status":     _const.SchedulerStatusRunning,
			"epoch":      jobs.Epoch + 1,
			"updated_at": time.Now().Unix(),
		})
	if res.Error != nil {
		return Jobs{}, err
	}

	if res.RowsAffected == 0 {
		// 抢占失败
		return Jobs{}, ErrPreemptFailed
	}

	return jobs, nil
}

func (j *JobPreempt) Preempt(ctx context.Context,
	strategy task_scheduler.FixedScheduleStrategy) (Jobs, error) {
	var timer *time.Timer
	defer timer.Stop()

	for {
		var jobs Jobs
		// 状态位执行中且执行时间小于等于当前时间的
		err := j.db.WithContext(ctx).Where("status = ? AND next_exec_time <= ?",
			_const.SchedulerStatusWaiting, time.Now().Unix()).First(&jobs).Error
		if err != nil {
			return Jobs{}, err
		}

		res := j.db.WithContext(ctx).Where("id = ? AND epoch = ?", jobs.ID, jobs.Epoch).
			Updates(map[string]interface{}{
				"status":     _const.SchedulerStatusRunning,
				"epoch":      jobs.Epoch + 1,
				"updated_at": time.Now().Unix(),
			})
		if res.Error != nil {
			return Jobs{}, err
		}

		if res.RowsAffected == 1 {
			// 抢占成功
			return jobs, nil
		}

		interval, err := strategy.Next()
		if err != nil {
			return Jobs{}, ErrPreemptFailed
		}

		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}

		select {
		case <-ctx.Done():
			// 超时了
			return Jobs{}, ctx.Err()
		case <-timer.C:
			// 等待下一次调度执行
		}
	}

}

type Jobs struct {
	// ID 在数据库中的ID信息
	ID int `gorm:"column:id;type:int;primaryKey;not null;autoIncrement" json:"id"`
	// Cfg 通用的配置信息
	Cfg string `gorm:"column:cfg;type:text;not null" json:"cfg"`
	// Status job的调度状态
	Status _const.SchedulerStatus `gorm:"column:status;type:int;not null" json:"status"`
	// Epoch 乐观锁，等同于version，用于确定调度期间执行节点的保活
	Epoch int `gorm:"column:epoch;type:int;not null" json:"epoch"`
	// NextExecTime 下次等待调度的时间
	NextExecTime int64 `gorm:"column:next_exec_time;type:int;not null" json:"next_exec_time"`
	// UpdatedTime 更新时间
	UpdatedTime int64 `gorm:"column:updated_time;type:int;not null" json:"updated_time"`
	// CreatedTime 创建时间
	CreatedTime int64 `gorm:"column:created_time;type:int;not null" json:"created_time"`
}
