package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestClientDescribeACLsAll(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.0.1") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	listAllACLsRequest := &DescribeACLsRequest{}
	res, err := client.DescribeACLs(context.Background(), listAllACLsRequest)
	if err != nil {
		t.Fatal(err)
	}

	if res.Error != nil {
		t.Error(err)
	}
	assert.Equal(t, 0, len(res.ACLs))
	acls := []ACLEntry{
		{
			Principal:           "User:alice",
			PermissionType:      ACLPermissionTypeAllow,
			Operation:           ACLOperationTypeRead,
			ResourceType:        ResourceTypeTopic,
			ResourcePatternType: PatternTypeLiteral,
			ResourceName:        "fake-topic-for-alice",
			Host:                "*",
		},
		{
			Principal:           "User:bob",
			PermissionType:      ACLPermissionTypeAllow,
			Operation:           ACLOperationTypeRead,
			ResourceType:        ResourceTypeGroup,
			ResourcePatternType: PatternTypeLiteral,
			ResourceName:        "fake-group-for-bob",
			Host:                "*",
		},
	}
	createACLs(t, client, acls)
	defer deleteACLs(t, client, acls)

	res, err = client.DescribeACLs(context.Background(), listAllACLsRequest)
	if err != nil {
		t.Fatal(err)
	}

	if res.Error != nil {
		t.Error(err)
	}
	assert.Equal(t, 2, len(res.ACLs))
}

func TestClientDescribeACLsBasicFiltering(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.0.1") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()
	acls := []ACLEntry{
		{
			Principal:           "User:alice",
			PermissionType:      ACLPermissionTypeAllow,
			Operation:           ACLOperationTypeRead,
			ResourceType:        ResourceTypeTopic,
			ResourcePatternType: PatternTypeLiteral,
			ResourceName:        "fake-read-topic-for-alice",
			Host:                "*",
		},
		{
			Principal:           "User:alice",
			PermissionType:      ACLPermissionTypeAllow,
			Operation:           ACLOperationTypeWrite,
			ResourceType:        ResourceTypeTopic,
			ResourcePatternType: PatternTypeLiteral,
			ResourceName:        "fake-write-topic-for-alice",
			Host:                "*",
		},
		{
			Principal:           "User:bob",
			PermissionType:      ACLPermissionTypeAllow,
			Operation:           ACLOperationTypeRead,
			ResourceType:        ResourceTypeGroup,
			ResourcePatternType: PatternTypeLiteral,
			ResourceName:        "fake-group-for-bob",
			Host:                "*",
		},
		{
			Principal:           "User:bob",
			PermissionType:      ACLPermissionTypeAllow,
			Operation:           ACLOperationTypeRead,
			ResourceType:        ResourceTypeTopic,
			ResourcePatternType: PatternTypeLiteral,
			ResourceName:        "fake-topic-for-bob",
			Host:                "*",
		},
	}
	createACLs(t, client, acls)
	defer deleteACLs(t, client, acls)

	res, err := client.DescribeACLs(context.Background(), &DescribeACLsRequest{
		ResourceTypeFilter: ResourceTypeTopic,
		PrincipalFilter:    "User:alice",
		Operation:          ACLOperationTypeWrite,
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.Error != nil {
		t.Error(err)
	}
	assert.Equal(t, 1, len(res.ACLs))
}

func createACLs(t *testing.T, client *Client, acls []ACLEntry) {
	t.Helper()

	_, err := client.CreateACLs(context.Background(), &CreateACLsRequest{
		ACLs: acls,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func deleteACLs(t *testing.T, client *Client, acls []ACLEntry) {
	t.Helper()
	filters := convertACLEntriesToFilters(acls)
	resp, err := client.DeleteACLs(context.Background(), &DeleteACLsRequest{
		Filters: filters,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, res := range resp.Results {
		if res.Error != nil {
			t.Fatalf("error occurred during deletion of an ACL: %s", err)
		}
	}
}

func convertACLEntriesToFilters(acls []ACLEntry) []ACLFilter {
	filters := make([]ACLFilter, 0, len(acls))
	for _, entry := range acls {
		filters = append(filters, ACLFilter{
			ResourceTypeFilter: entry.ResourceType,
			ResourceNameFilter: entry.ResourceName,
			PatternTypeFilter:  entry.ResourcePatternType,
			PrincipalFilter:    entry.Principal,
			HostFilter:         entry.Host,
			Operation:          entry.Operation,
			PermissionType:     entry.PermissionType,
		})
	}
	return filters
}
