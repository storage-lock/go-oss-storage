package oss_storage

import (
	"context"
	"os"
	"testing"

	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
)

func TestOSSStorage(t *testing.T) {
	envName := "STORAGE_LOCK_OSS_ENDPOINT"
	endpoint := os.Getenv(envName)
	if endpoint == "" {
		t.Skip("STORAGE_LOCK_OSS_ENDPOINT not set, skipping OSS test")
	}

	accessKeyID := os.Getenv("STORAGE_LOCK_OSS_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("STORAGE_LOCK_OSS_ACCESS_KEY_SECRET")
	bucketName := os.Getenv("STORAGE_LOCK_OSS_BUCKET")
	if bucketName == "" {
		bucketName = "storage-lock-test"
	}

	ctx := context.Background()
	cm := NewOSSConnectionManagerFromEndpoint(endpoint, accessKeyID, accessKeySecret, bucketName)
	s, err := NewOSSStorage(ctx, &OSSStorageOptions{
		ConnectionManager: cm,
		BucketName:        bucketName,
		KeyPrefix:         storage_test_helper.TestTableName,
		TimeProvider:      &LocalTimeProvider{},
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
