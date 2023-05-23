package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestClientDeleteACLs(t *testing.T) {
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

	_, err := client.CreateACLs(context.Background(), &CreateACLsRequest{
		ACLs: acls,
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.DeleteACLs(context.Background(), &DeleteACLsRequest{
		Filters: []ACLFilter{
			{
				PrincipalFilter:    "User:alice",
				PermissionType:     ACLPermissionTypeAllow,
				Operation:          ACLOperationTypeRead,
				ResourceTypeFilter: ResourceTypeTopic,
				PatternTypeFilter:  PatternTypeLiteral,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(res.Results))
	if res.Results[0].Error != nil {
		t.Error(err)
	}

	deleteACLs(t, client, acls)
}
