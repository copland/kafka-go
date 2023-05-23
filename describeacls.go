package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/describeacls"
)

// DescribeACLsRequest represents a request sent to a kafka broker to describe
// ACLs.
type DescribeACLsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	ResourceTypeFilter ResourceType
	ResourceNameFilter string
	PatternTypeFilter  PatternType
	PrincipalFilter    string
	HostFilter         string
	Operation          ACLOperationType
	PermissionType     ACLPermissionType
}

// DescribeACLsResponse represents a response from a kafka broker to an ACL
// describe request.
type DescribeACLsResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// The error that occurred while attempting to describe the ACLs.
	//
	// The error contains the kafka error code. Programs may use the standard
	// errors.Is function to test the error against kafka error codes.
	Error error

	// List of ACLs
	ACLs []ACLEntry
}

// DescribeACLs sends a describe request to a kafka broker and returns the
// response.
func (c *Client) DescribeACLs(ctx context.Context, req *DescribeACLsRequest) (*DescribeACLsResponse, error) {

	op := req.Operation
	if op == ACLOperationTypeUnknown {
		op = ACLOperationTypeAny
	}

	perm := req.PermissionType
	if perm == ACLPermissionTypeUnknown {
		perm = ACLPermissionTypeAny
	}

	resourceType := req.ResourceTypeFilter
	if req.ResourceTypeFilter == ResourceTypeUnknown {
		resourceType = ResourceTypeAny
	}

	patternType := req.PatternTypeFilter
	if patternType == PatternTypeUnknown {
		patternType = PatternTypeAny
	}

	m, err := c.roundTrip(ctx, req.Addr, &describeacls.Request{
		ResourceTypeFilter: int8(resourceType),
		ResourceNameFilter: req.ResourceNameFilter,
		PatternTypeFilter:  int8(patternType),
		PrincipalFilter:    req.PrincipalFilter,
		HostFilter:         req.HostFilter,
		Operation:          int8(op),
		PermissionType:     int8(perm),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).DescribeACLs: %w", err)
	}

	res := m.(*describeacls.Response)
	ret := &DescribeACLsResponse{
		Throttle: makeDuration(res.ThrottleTimeMs),
		Error:    makeError(res.ErrorCode, res.ErrorMessage),
		ACLs:     []ACLEntry{},
	}

	for _, r := range res.Resources {
		for _, acl := range r.ACLs {
			ret.ACLs = append(ret.ACLs, ACLEntry{
				ResourceType:        ResourceType(r.ResourceType),
				ResourceName:        r.ResourceName,
				ResourcePatternType: PatternType(r.PatternType),
				Principal:           acl.Principal,
				Host:                acl.Host,
				Operation:           ACLOperationType(acl.Operation),
				PermissionType:      ACLPermissionType(acl.PermissionType),
			})
		}
	}

	return ret, nil
}
