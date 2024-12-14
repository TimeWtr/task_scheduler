package _const

type SchedulerStatus int

const (
	SchedulerStatusWaiting SchedulerStatus = 0x00000001 // 等待抢占调度
	SchedulerStatusRunning                 = 0x00000002 // 调度执行中
	SchedulerStatusSuccess                 = 0x00000003 // 调度执行成功
	SchedulerStatusFailed                  = 0x00000004 // 调度执行失败
	SchedulerStatusPaused                  = 0x00000005 // 调度执行终止
)

func (s SchedulerStatus) String() string {
	switch s {
	case SchedulerStatusWaiting:
		return "Waiting"
	case SchedulerStatusRunning:
		return "Running"
	case SchedulerStatusSuccess:
		return "Success"
	case SchedulerStatusFailed:
		return "Failed"
	case SchedulerStatusPaused:
		return "Paused"
	default:
		return "Unknown"
	}
}
