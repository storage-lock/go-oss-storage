package oss_storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/storage-lock/go-storage"
)

// OSSConnectionManager 管理 OSS 客户端连接
type OSSConnectionManager struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	BucketName      string

	presetClient *oss.Client
	presetBucket *oss.Bucket

	initMu      sync.Mutex
	initialized bool
	client      *oss.Client
	bucket      *oss.Bucket
}

var _ storage.ConnectionManager[*oss.Bucket] = &OSSConnectionManager{}

// NewOSSConnectionManagerFromEndpoint 从端点参数创建连接管理器
// ossNew 默认创建真实客户端，测试可替换。
var ossNew = oss.New

func NewOSSConnectionManagerFromEndpoint(endpoint, accessKeyID, accessKeySecret, bucketName string) *OSSConnectionManager {
	return &OSSConnectionManager{
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyID,
		AccessKeySecret: accessKeySecret,
		BucketName:      bucketName,
	}
}

// NewOSSConnectionManagerFromClient 从已有客户端创建连接管理器
func NewOSSConnectionManagerFromClient(client *oss.Client, bucket *oss.Bucket) *OSSConnectionManager {
	return &OSSConnectionManager{
		presetClient: client,
		presetBucket: bucket,
		initialized:  true,
		client:       client,
		bucket:       bucket,
	}
}

func (x *OSSConnectionManager) Name() string {
	return "oss-connection-manager"
}

func (x *OSSConnectionManager) Take(ctx context.Context) (*oss.Bucket, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.initialized {
		return x.bucket, nil
	}

	client, err := ossNew(x.Endpoint, x.AccessKeyID, x.AccessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("failed to create OSS client: %w", err)
	}

	bucket, err := client.Bucket(x.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get OSS bucket: %w", err)
	}

	x.client = client
	x.bucket = bucket
	x.initialized = true
	return x.bucket, nil
}

func (x *OSSConnectionManager) Return(ctx context.Context, connection *oss.Bucket) error {
	return nil
}

func (x *OSSConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	x.client = nil
	x.bucket = nil
	x.initialized = false
	return nil
}
