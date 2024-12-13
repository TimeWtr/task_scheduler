package task_scheduler

import (
	"errors"
	"time"
)

var ErrOverMaxCount = errors.New("over max count")

type ScheduleStrategy interface {
	Next() (time.Duration, error)
}

type FixedScheduleStrategy struct {
	// 固定时间间隔
	interval time.Duration
	// 最大调度次数
	maxCount int
	// 当前已经调度的次数
	counter int
}

func NewFixedScheduleStrategy(interval time.Duration, maxCount int) *FixedScheduleStrategy {
	return &FixedScheduleStrategy{
		interval: interval,
		maxCount: maxCount,
	}
}

func (s *FixedScheduleStrategy) Next() (time.Duration, error) {
	if s.counter >= s.maxCount {
		return 0, ErrOverMaxCount
	}
	s.counter++
	return s.interval, nil
}
