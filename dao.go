package task_scheduler

import (
	"context"
	"errors"
	"github.com/TimeWtr/task_scheduler/const"
	"gorm.io/gorm"
	"sync"
	"time"
)

var ErrPreemptFailed = errors.New("preempt failed")

type Preempt interface {
	// TryPreempt 尝试调度一次
	TryPreempt(ctx context.Context) (JobSwap, error)
	// Preempt 调度失败会进行重试
	Preempt(ctx context.Context, strategy FixedScheduleStrategy) (JobSwap, error)
}

type JobPreempt struct {
	db *gorm.DB
}

func NewJobPreempt(db *gorm.DB) Preempt {
	return &JobPreempt{db: db}
}

func (j *JobPreempt) TryPreempt(ctx context.Context) (JobSwap, error) {
	var jobs Jobs
	// 状态位执行中且执行时间小于等于当前时间的
	err := j.db.WithContext(ctx).Where("status = ? AND next_exec_time <= ?",
		_const.SchedulerStatusWaiting, time.Now().Unix()).First(&jobs).Error
	if err != nil {
		return JobSwap{}, err
	}

	res := j.db.WithContext(ctx).Where("id = ? AND epoch = ?", jobs.ID, jobs.Epoch).
		Updates(map[string]interface{}{
			"status":     _const.SchedulerStatusRunning,
			"epoch":      jobs.Epoch + 1,
			"updated_at": time.Now().Unix(),
		})
	if res.Error != nil {
		return JobSwap{}, err
	}

	if res.RowsAffected == 0 {
		// 抢占失败
		return JobSwap{}, ErrPreemptFailed
	}

	return newJobSwap(j.db, jobs), nil
}

func (j *JobPreempt) Preempt(ctx context.Context,
	strategy FixedScheduleStrategy) (JobSwap, error) {
	var timer *time.Timer
	defer timer.Stop()

	for {
		var jobs Jobs

		// 状态位执行中且执行时间小于等于当前时间的
		// TODO 需要注意这里会存在多个节点同时抢到一个Job的高并发问题，后续处理
		lctx, cancel := context.WithTimeout(ctx, time.Second)
		err := j.db.WithContext(lctx).Where("status = ? AND next_exec_time <= ?",
			_const.SchedulerStatusWaiting, time.Now().Unix()).First(&jobs).Error
		cancel()
		if err != nil {
			return JobSwap{}, err
		}

		lctx, cancel = context.WithTimeout(ctx, time.Second)
		res := j.db.WithContext(lctx).Where("id = ? AND epoch = ?", jobs.ID, jobs.Epoch).
			Updates(map[string]interface{}{
				"status":     _const.SchedulerStatusRunning,
				"epoch":      jobs.Epoch + 1,
				"updated_at": time.Now().Unix(),
			})
		cancel()
		if res.Error != nil {
			return JobSwap{}, err
		}

		if res.RowsAffected == 1 {
			// 抢占成功
			return newJobSwap(j.db, jobs), nil
		}

		interval, err := strategy.Next()
		if err != nil {
			return JobSwap{}, ErrPreemptFailed
		}

		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}

		select {
		case <-ctx.Done():
			// 超时了
			return JobSwap{}, ctx.Err()
		case <-timer.C:
			// 等待下一次调度执行
		}
	}

}

type Jobs struct {
	// ID 在数据库中的ID信息
	ID int `gorm:"column:id;type:int;primaryKey;not null;autoIncrement" json:"id"`
	// Job的唯一标识
	JobID string `gorm:"column:job_id;type:varchar(255);not null" json:"job_id"`
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

type JobSwap struct {
	Jobs
	// 数据库连接
	db *gorm.DB
	// 关闭通道
	closeCh chan struct{}
	// once
	once *sync.Once
}

func newJobSwap(db *gorm.DB, jobs Jobs) JobSwap {
	return JobSwap{
		db:      db,
		Jobs:    jobs,
		closeCh: make(chan struct{}),
		once:    &sync.Once{},
	}
}

// Release 释放抢占到的任务
func (j *JobSwap) Release() error {
	j.once.Do(func() {
		close(j.closeCh)
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	return j.db.WithContext(ctx).
		Where("id = ? AND epoch = ?", j.JobID, j.Epoch).
		Updates(map[string]interface{}{
			"status":     _const.SchedulerStatusWaiting,
			"updated_at": time.Now().Unix(),
		}).Error
}

// Refresh 抢占任务续约机制，用于定期保活，防止节点故障导致任务长时间未得到释放
// 续约条件：
// 1. 持有Job的ID，防止出现续约错误的Job；
// 2. 版本为当前版本
// 3. 状态为运行中
func (j *JobSwap) Refresh() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := j.db.WithContext(ctx).
		Where("id = ? AND epoch = ? AND status = ?", j.JobID, j.Epoch, _const.SchedulerStatusRunning).
		Updates(map[string]interface{}{
			"updated_at": time.Now().Unix(),
		}).Error
	if err != nil {
		// 立刻重试一次
		err = j.db.WithContext(ctx).
			Where("id = ? AND epoch = ? AND status = ?", j.JobID, j.Epoch, _const.SchedulerStatusRunning).
			Updates(map[string]interface{}{
				"updated_at": time.Now().Unix(),
			}).Error
		return err
	}

	return nil
}

func (j *JobSwap) AutoRefresh(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	errCh := make(chan struct{}, 1)
	defer close(errCh)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-j.closeCh:
			// 主动停止续约
			return nil
		case <-ticker.C:
			lctx, cancel := context.WithTimeout(ctx, time.Second)
			err := j.db.WithContext(lctx).
				Where("id = ? AND epoch = ? AND status = ?", j.JobID, j.Epoch, _const.SchedulerStatusRunning).
				Updates(map[string]interface{}{
					"updated_at": time.Now().Unix(),
				}).Error
			cancel()
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				// 超时导致续约失败
				errCh <- struct{}{}
				continue
			}

			if err != nil {
				// 未知原因续约失败
				return err
			}
		case <-errCh:
			lctx, cancel := context.WithTimeout(ctx, time.Second)
			err := j.db.WithContext(lctx).
				Where("id = ? AND epoch = ? AND status = ?", j.JobID, j.Epoch, _const.SchedulerStatusRunning).
				Updates(map[string]interface{}{
					"updated_at": time.Now().Unix(),
				}).Error
			cancel()
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				// 超时导致续约失败
				errCh <- struct{}{}
				continue
			}

			if err != nil {
				// 未知原因续约失败
				return err
			}
		}
	}
}
