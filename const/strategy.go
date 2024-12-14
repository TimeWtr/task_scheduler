package _const

// PreemptStrategy 抢占策略
type PreemptStrategy int

const (
	TryPreemptStrategy   PreemptStrategy = 0x00000001 // 每次调度只尝试抢占一次，抢占失败则等待下次调度
	RetryPreemptStrategy                 = 0x00000002 // 每次调度当抢占失败后进行有限次的重试抢占
)

func (s PreemptStrategy) Preempt() string {
	switch s {
	case TryPreemptStrategy:
		return "try-preempt"
	case RetryPreemptStrategy:
		return "retry-preempt"
	default:
		return "unknown"
	}
}
