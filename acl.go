package kafka

type ACLPermissionType int8

const (
	ACLPermissionTypeUnknown ACLPermissionType = 0
	ACLPermissionTypeAny     ACLPermissionType = 1
	ACLPermissionTypeDeny    ACLPermissionType = 2
	ACLPermissionTypeAllow   ACLPermissionType = 3
)

type ACLOperationType int8

const (
	ACLOperationTypeUnknown         ACLOperationType = 0
	ACLOperationTypeAny             ACLOperationType = 1
	ACLOperationTypeAll             ACLOperationType = 2
	ACLOperationTypeRead            ACLOperationType = 3
	ACLOperationTypeWrite           ACLOperationType = 4
	ACLOperationTypeCreate          ACLOperationType = 5
	ACLOperationTypeDelete          ACLOperationType = 6
	ACLOperationTypeAlter           ACLOperationType = 7
	ACLOperationTypeDescribe        ACLOperationType = 8
	ACLOperationTypeClusterAction   ACLOperationType = 9
	ACLOperationTypeDescribeConfigs ACLOperationType = 10
	ACLOperationTypeAlterConfigs    ACLOperationType = 11
	ACLOperationTypeIdempotentWrite ACLOperationType = 12
)

type ACLFilter struct {
	ResourceTypeFilter ResourceType
	ResourceNameFilter string
	PatternTypeFilter  PatternType
	PrincipalFilter    string
	HostFilter         string
	Operation          ACLOperationType
	PermissionType     ACLPermissionType
}

type ACLEntry struct {
	ResourceType        ResourceType
	ResourceName        string
	ResourcePatternType PatternType
	Principal           string
	Host                string
	Operation           ACLOperationType
	PermissionType      ACLPermissionType
}
