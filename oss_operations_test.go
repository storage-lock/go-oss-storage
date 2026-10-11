package oss_storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

func newTestOSSStorage(t *testing.T) *OSSStorage {
	t.Helper()
	s, err := NewOSSStorage(context.Background(), &OSSStorageOptions{
		ConnectionManager: NewOSSConnectionManagerFromClient(&oss.Client{}, &oss.Bucket{}),
		BucketName:        "bucket",
		KeyPrefix:         "locks",
	})
	assert.NoError(t, err)
	return s
}

func restoreOSSOps(t *testing.T) {
	t.Helper()
	put, get, meta, del, list := ossPutObject, ossGetObject, ossGetObjectDetailedMeta, ossDeleteObject, ossListObjects
	t.Cleanup(func() {
		ossPutObject, ossGetObject, ossGetObjectDetailedMeta, ossDeleteObject, ossListObjects = put, get, meta, del, list
	})
}

type failReader struct{ err error }

func (r failReader) Read([]byte) (int, error) { return 0, r.err }
func (r failReader) Close() error             { return nil }

func ossLockJSON(owner string) string {
	return (&storage.LockInformation{LockId: "id", OwnerId: owner}).ToJsonString()
}

func TestOSSStorage_CreateWithVersion(t *testing.T) {
	s := newTestOSSStorage(t)
	restoreOSSOps(t)
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()

	ossPutObject = func(*oss.Bucket, string, io.Reader, ...oss.Option) error { return nil }
	assert.NoError(t, s.CreateWithVersion(ctx, "id", 1, info))

	ossPutObject = func(*oss.Bucket, string, io.Reader, ...oss.Option) error {
		return errors.New("Forbid Overwrite")
	}
	assert.ErrorIs(t, s.CreateWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)

	ossPutObject = func(*oss.Bucket, string, io.Reader, ...oss.Option) error {
		return errors.New("put failed")
	}
	assert.EqualError(t, s.CreateWithVersion(ctx, "id", 1, info), "put failed")
}

func TestOSSStorage_UpdateWithVersion(t *testing.T) {
	s := newTestOSSStorage(t)
	restoreOSSOps(t)
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()

	ossGetObjectDetailedMeta = func(*oss.Bucket, string) (http.Header, error) {
		return nil, errors.New("NoSuchKey")
	}
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	ossGetObjectDetailedMeta = func(*oss.Bucket, string) (http.Header, error) {
		return nil, errors.New("meta failed")
	}
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "meta failed")

	header := http.Header{"ETag": []string{"etag-1"}}
	ossGetObjectDetailedMeta = func(*oss.Bucket, string) (http.Header, error) { return header, nil }

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return nil, errors.New("404")
	}
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return nil, errors.New("get failed")
	}
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "get failed")

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return failReader{errors.New("read failed")}, nil
	}
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "read failed")

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("bad"))), nil
	}
	assert.Error(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(encodeValue(9, ossLockJSON("other")))), nil
	}
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(encodeValue(1, ossLockJSON("owner")))), nil
	}
	ossPutObject = func(*oss.Bucket, string, io.Reader, ...oss.Option) error {
		return errors.New("412")
	}
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	ossPutObject = func(*oss.Bucket, string, io.Reader, ...oss.Option) error {
		return errors.New("put failed")
	}
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "put failed")

	ossPutObject = func(*oss.Bucket, string, io.Reader, ...oss.Option) error { return nil }
	assert.NoError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))
}

func TestOSSStorage_DeleteWithVersion(t *testing.T) {
	s := newTestOSSStorage(t)
	restoreOSSOps(t)
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return nil, errors.New("does not exist")
	}
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrLockNotFound)

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return nil, errors.New("get failed")
	}
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "get failed")

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return failReader{errors.New("read failed")}, nil
	}
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "read failed")

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("bad"))), nil
	}
	assert.Error(t, s.DeleteWithVersion(ctx, "id", 1, info))

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(encodeValue(4, ossLockJSON("owner")))), nil
	}
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(encodeValue(1, ossLockJSON("owner")))), nil
	}
	ossDeleteObject = func(*oss.Bucket, string) error { return errors.New("delete failed") }
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "delete failed")

	ossDeleteObject = func(*oss.Bucket, string) error { return nil }
	assert.NoError(t, s.DeleteWithVersion(ctx, "id", 1, info))
}

func TestOSSStorage_Get(t *testing.T) {
	s := newTestOSSStorage(t)
	restoreOSSOps(t)
	ctx := context.Background()

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return nil, errors.New("NoSuchKey")
	}
	_, err := s.Get(ctx, "id")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return nil, errors.New("get failed")
	}
	_, err = s.Get(ctx, "id")
	assert.EqualError(t, err, "get failed")

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return failReader{errors.New("read failed")}, nil
	}
	_, err = s.Get(ctx, "id")
	assert.EqualError(t, err, "read failed")

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	_, err = s.Get(ctx, "id")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("bad"))), nil
	}
	_, err = s.Get(ctx, "id")
	assert.Error(t, err)

	ossGetObject = func(*oss.Bucket, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(encodeValue(1, ossLockJSON("owner")))), nil
	}
	got, err := s.Get(ctx, "id")
	assert.NoError(t, err)
	assert.Contains(t, got, "owner")
}

func TestOSSStorage_List(t *testing.T) {
	s := newTestOSSStorage(t)
	restoreOSSOps(t)
	ctx := context.Background()

	ossListObjects = func(*oss.Bucket, ...oss.Option) (oss.ListObjectsResult, error) {
		return oss.ListObjectsResult{}, errors.New("list failed")
	}
	_, err := s.List(ctx)
	assert.EqualError(t, err, "list failed")

	calls := 0
	ossListObjects = func(_ *oss.Bucket, _ ...oss.Option) (oss.ListObjectsResult, error) {
		calls++
		if calls == 1 {
			return oss.ListObjectsResult{
				Objects: []oss.ObjectProperties{
					{Key: "locks/lock/skip-get"},
					{Key: "locks/lock/skip-read"},
					{Key: "locks/lock/skip-empty"},
					{Key: "locks/lock/skip-decode"},
					{Key: "locks/lock/skip-json"},
					{Key: "locks/lock/ok"},
				},
				IsTruncated: true,
				NextMarker:  "next",
			}, nil
		}
		return oss.ListObjectsResult{IsTruncated: false}, nil
	}
	ossGetObject = func(_ *oss.Bucket, key string) (io.ReadCloser, error) {
		switch key {
		case "locks/lock/skip-get":
			return nil, errors.New("missing")
		case "locks/lock/skip-read":
			return failReader{errors.New("read")}, nil
		case "locks/lock/skip-empty":
			return io.NopCloser(bytes.NewReader(nil)), nil
		case "locks/lock/skip-decode":
			return io.NopCloser(bytes.NewReader([]byte("bad"))), nil
		case "locks/lock/skip-json":
			return io.NopCloser(bytes.NewReader(encodeValue(1, "not-json"))), nil
		default:
			return io.NopCloser(bytes.NewReader(encodeValue(1, ossLockJSON("owner")))), nil
		}
	}
	listed, err := s.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, calls)
	count := 0
	for listed.Next() {
		count++
		assert.Equal(t, "owner", listed.Value().OwnerId)
	}
	assert.Equal(t, 1, count)
}

func TestOSSOptions_EmptyPrefix(t *testing.T) {
	opts := NewOSSStorageOptions()
	opts.KeyPrefix = ""
	assert.ErrorIs(t, opts.Check(), ErrKeyPrefixEmpty)
	local, err := (&LocalTimeProvider{}).GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, local.IsZero())
}

func TestOSSDefaultOperations_Delegate(t *testing.T) {
	client, err := oss.New("http://127.0.0.1:1", "ak", "sk")
	assert.NoError(t, err)
	bucket, err := client.Bucket("bucket")
	assert.NoError(t, err)
	assert.Error(t, ossPutObject(bucket, "key", bytes.NewReader([]byte("value"))))
	_, err = ossGetObject(bucket, "key")
	assert.Error(t, err)
	_, err = ossGetObjectDetailedMeta(bucket, "key")
	assert.Error(t, err)
	assert.Error(t, ossDeleteObject(bucket, "key"))
	_, err = ossListObjects(bucket, oss.Prefix("locks"), oss.MaxKeys(1))
	assert.Error(t, err)
}
