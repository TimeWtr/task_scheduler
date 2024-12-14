package _const

type Priority int

const (
	PriorityHigh   Priority = 0x00000001 // 高优先级
	PriorityMedium Priority = 0x00000002 // 中优先级
	PriorityLow    Priority = 0x00000003 // 低优先级
)

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "High"
	case PriorityMedium:
		return "Medium"
	case PriorityLow:
		return "Low"
	default:
		return "Unknown"
	}
}

func (p Priority) Less(v Priority) bool {
	return p < v
}
