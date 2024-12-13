package domain

import (
	"github.com/TimeWtr/task_scheduler/const"
)

type Jobs struct {
	// ID 在数据库中的ID信息
	ID int
	// Cfg 通用的配置信息
	Cfg string
	// Status job的调度状态
	Status _const.SchedulerStatus
	// Epoch 乐观锁，等同于version，用于确定调度期间执行节点的保活
	Epoch int
	// NextExecTime 下次等待调度的时间
	NextExecTime int64
}
