package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/deleteacls"
)

// DeleteACLsRequest represents a request sent to a kafka broker to delete
// matching ACLs.
type DeleteACLsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// List of filters used to delete matching ACLs
	Filters []ACLFilter
}

// DeleteACLsResponse represents a response from a kafka broker to an ACL
// deletion request.
type DeleteACLsResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// List of deletion results. There should be one result for every filter
	// in the request.
	Results []DeleteACLResult
}

type DeleteACLResult struct {
	// The error that occurred while attempting to delete the ACL
	//
	// The error contains the kafka error code. Programs may use the standard
	// errors.Is function to test the error against kafka error codes.
	Error error

	// List of ACLs that were deleted
	ACLs []ACLEntry
}

// DeleteACLs sends a delete request for ACLs matching the provided filters
// to a kafka broker and returns the response.
func (c *Client) DeleteACLs(ctx context.Context, req *DeleteACLsRequest) (*DeleteACLsResponse, error) {

	filters := make([]deleteacls.Filter, 0, len(req.Filters))
	for _, filter := range req.Filters {
		op := filter.Operation
		if op == ACLOperationTypeUnknown {
			op = ACLOperationTypeAny
		}

		perm := filter.PermissionType
		if perm == ACLPermissionTypeUnknown {
			perm = ACLPermissionTypeAny
		}

		resourceType := filter.ResourceTypeFilter
		if resourceType == ResourceTypeUnknown {
			resourceType = ResourceTypeAny
		}

		patternType := filter.PatternTypeFilter
		if patternType == PatternTypeUnknown {
			patternType = PatternTypeAny
		}

		filters = append(filters, deleteacls.Filter{
			ResourceTypeFilter: int8(resourceType),
			ResourceNameFilter: filter.ResourceNameFilter,
			PatternTypeFilter:  int8(patternType),
			PrincipalFilter:    filter.PrincipalFilter,
			HostFilter:         filter.HostFilter,
			Operation:          int8(op),
			PermissionType:     int8(perm),
		})
	}

	m, err := c.roundTrip(ctx, req.Addr, &deleteacls.Request{
		Filters: filters,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).DeleteACLs: %w", err)
	}

	res := m.(*deleteacls.Response)
	ret := &DeleteACLsResponse{
		Throttle: makeDuration(res.ThrottleTimeMs),
		Results:  []DeleteACLResult{},
	}

	for _, filterResult := range res.FilterResults {
		r := DeleteACLResult{
			Error: makeError(filterResult.ErrorCode, filterResult.ErrorMessage),
			ACLs:  make([]ACLEntry, 0, len(filterResult.MatchingAcls)),
		}
		for _, acl := range filterResult.MatchingAcls {
			r.ACLs = append(r.ACLs, ACLEntry{
				ResourceType:        ResourceType(acl.ResourceType),
				ResourceName:        acl.ResourceName,
				ResourcePatternType: PatternType(acl.PatternType),
				Principal:           acl.Principal,
				Host:                acl.Host,
				Operation:           ACLOperationType(acl.Operation),
				PermissionType:      ACLPermissionType(acl.PermissionType),
			})
		}
		ret.Results = append(ret.Results, r)
	}

	return ret, nil
}
