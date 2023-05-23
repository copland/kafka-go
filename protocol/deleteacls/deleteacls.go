package deleteacls

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	// We need at least one tagged field to indicate that v3+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	Filters []Filter `kafka:"min=v0,max=v3"`
}

type Filter struct {
	ResourceTypeFilter int8   `kafka:"min=v0,max=v3"`
	ResourceNameFilter string `kafka:"min=v0,max=v3,compact,nullable"`
	PatternTypeFilter  int8   `kafka:"min=v1,max=v3"`
	PrincipalFilter    string `kafka:"min=v0,max=v3,compact,nullable"`
	HostFilter         string `kafka:"min=v0,max=v3,compact,nullable"`
	Operation          int8   `kafka:"min=v0,max=v3"`
	PermissionType     int8   `kafka:"min=v0,max=v3"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.DeleteAcls }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type Response struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	ThrottleTimeMs int32          `kafka:"min=v0,max=v3"`
	FilterResults  []FilterResult `kafka:"min=v0,max=v3"`
}

type FilterResult struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	ErrorCode    int16         `kafka:"min=v0,max=v3"`
	ErrorMessage string        `kafka:"min=v0,max=v3,compact,nullable"`
	MatchingAcls []MatchingACL `kafka:"min=v0,max=v3"`
}

type MatchingACL struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	ErrorCode      int16  `kafka:"min=v0,max=v3"`
	ErrorMessage   string `kafka:"min=v0,max=v3,compact,nullable"`
	ResourceType   int8   `kafka:"min=v0,max=v3"`
	ResourceName   string `kafka:"min=v0,max=v3,compact"`
	PatternType    int8   `kafka:"min=v1,max=v3"`
	Principal      string `kafka:"min=v0,max=v3,compact"`
	Host           string `kafka:"min=v0,max=v3,compact"`
	Operation      int8   `kafka:"min=v0,max=v3"`
	PermissionType int8   `kafka:"min=v0,max=v3"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.DeleteAcls }

var _ protocol.BrokerMessage = (*Request)(nil)
