package oss_storage

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/storage-lock/go-storage"
)

// OSSStorageOptions 阿里云 OSS 存储的选项
type OSSStorageOptions struct {
	ConnectionManager storage.ConnectionManager[*oss.Bucket]
	BucketName        string
	KeyPrefix         string
	TimeProvider      storage.TimeProvider
}

func NewOSSStorageOptions() *OSSStorageOptions {
	return &OSSStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func (x *OSSStorageOptions) SetConnectionManager(cm storage.ConnectionManager[*oss.Bucket]) *OSSStorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *OSSStorageOptions) SetBucketName(bucketName string) *OSSStorageOptions {
	x.BucketName = bucketName
	return x
}

func (x *OSSStorageOptions) SetKeyPrefix(keyPrefix string) *OSSStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *OSSStorageOptions) SetTimeProvider(tp storage.TimeProvider) *OSSStorageOptions {
	x.TimeProvider = tp
	return x
}

func (x *OSSStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.BucketName == "" {
		return ErrBucketNameEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}