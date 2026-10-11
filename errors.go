package oss_storage

import "fmt"

var (
	ErrKeyPrefixEmpty       = fmt.Errorf("KeyPrefix can not empty")
	ErrConnectionManagerNil = fmt.Errorf("ConnectionManager can not nil")
	ErrBucketNameEmpty      = fmt.Errorf("BucketName can not empty")
)
