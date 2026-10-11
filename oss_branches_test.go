package oss_storage

import (
	"context"
	"errors"
	"testing"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

type takeErrorManager struct{}

func (takeErrorManager) Name() string { return "take-error" }
func (takeErrorManager) Take(context.Context) (*oss.Bucket, error) {
	return nil, errors.New("take failed")
}
func (takeErrorManager) Return(context.Context, *oss.Bucket) error { return nil }
func (takeErrorManager) Shutdown(context.Context) error            { return nil }

func TestOSSConnectionManager(t *testing.T) {
	preset := &oss.Bucket{}
	fromClient := NewOSSConnectionManagerFromClient(&oss.Client{}, preset)
	assert.Equal(t, "oss-connection-manager", fromClient.Name())
	got, err := fromClient.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, preset, got)
	assert.NoError(t, fromClient.Return(context.Background(), got))
	assert.NoError(t, fromClient.Shutdown(context.Background()))

	orig := ossNew
	defer func() { ossNew = orig }()
	ossNew = func(string, string, string, ...oss.ClientOption) (*oss.Client, error) {
		return nil, errors.New("open failed")
	}
	fromConfig := NewOSSConnectionManagerFromEndpoint("https://oss.example.com", "ak", "sk", "bucket")
	_, err = fromConfig.Take(context.Background())
	assert.Error(t, err)

	client, err := oss.New("http://127.0.0.1:1", "ak", "sk")
	assert.NoError(t, err)
	ossNew = func(string, string, string, ...oss.ClientOption) (*oss.Client, error) {
		return client, nil
	}
	fromConfig.BucketName = "A"
	_, err = fromConfig.Take(context.Background())
	assert.Error(t, err)

	fromConfig.BucketName = "bucket"
	got, err = fromConfig.Take(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, got)
}

func TestOSSStorage_OfflineBranches(t *testing.T) {
	assert.Panics(t, func() { _, _ = NewOSSStorage(context.Background(), nil) })
	_, err := NewOSSStorage(context.Background(), NewOSSStorageOptions())
	assert.ErrorIs(t, err, ErrBucketNameEmpty)
	opts := NewOSSStorageOptions().SetKeyPrefix("locks")
	assert.ErrorIs(t, opts.Check(), ErrBucketNameEmpty)
	opts.SetBucketName("bucket")
	assert.ErrorIs(t, opts.Check(), ErrConnectionManagerNil)

	_, err = NewOSSStorage(context.Background(), &OSSStorageOptions{ConnectionManager: takeErrorManager{}, BucketName: "bucket", KeyPrefix: "locks"})
	assert.EqualError(t, err, "take failed")

	s, err := NewOSSStorage(context.Background(), NewOSSStorageOptions().
		SetConnectionManager(NewOSSConnectionManagerFromClient(&oss.Client{}, &oss.Bucket{})).
		SetBucketName("bucket").
		SetKeyPrefix("locks").
		SetTimeProvider(&LocalTimeProvider{}))
	assert.NoError(t, err)
	assert.Equal(t, OSSStorageName, s.GetName())
	assert.Len(t, s.Capabilities(), 3)
	assert.Equal(t, "locks/lock/id", s.objectKey("id"))
	encoded := encodeValue(7, `{"owner_id":"owner"}`)
	version, jsonText, err := decodeValue(encoded)
	assert.NoError(t, err)
	assert.Equal(t, storage.Version(7), version)
	assert.Contains(t, jsonText, "owner")
	_, _, err = decodeValue([]byte("bad"))
	assert.Error(t, err)
	_, _, err = decodeValue([]byte("bad\x00json"))
	assert.Error(t, err)
	now, err := s.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
	assert.NoError(t, s.Close(context.Background()))
	_, err = (&OSSStorage{}).Watch(context.Background(), "")
	assert.Error(t, err)

	assert.False(t, isPreconditionError(nil))
	assert.True(t, isPreconditionError(errors.New("PreconditionFailed")))
	assert.True(t, isPreconditionError(errors.New("412")))
	assert.True(t, isPreconditionError(errors.New("condition")))
	assert.True(t, isPreconditionError(errors.New("Conflict")))
	assert.True(t, isPreconditionError(errors.New("Forbid Overwrite")))
	assert.False(t, isPreconditionError(errors.New("other")))
	assert.False(t, isNotFoundError(nil))
	assert.True(t, isNotFoundError(errors.New("NoSuchKey")))
	assert.True(t, isNotFoundError(errors.New("404")))
	assert.True(t, isNotFoundError(errors.New("does not exist")))
	assert.False(t, isNotFoundError(errors.New("other")))

	plain, err := NewOSSStorage(context.Background(), &OSSStorageOptions{
		ConnectionManager: NewOSSConnectionManagerFromClient(&oss.Client{}, &oss.Bucket{}),
		BucketName:        "bucket",
		KeyPrefix:         "locks",
	})
	assert.NoError(t, err)
	now, err = plain.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
}
