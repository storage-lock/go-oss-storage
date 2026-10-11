package oss_storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

const valueSeparator = "\x00"

// 对象操作默认走真实 Bucket。SDK 方法是值接收者，无法用嵌入类型覆盖，测试通过替换这些函数覆盖分支。
var (
	ossPutObject = func(bucket *oss.Bucket, key string, reader io.Reader, options ...oss.Option) error {
		return bucket.PutObject(key, reader, options...)
	}
	ossGetObject = func(bucket *oss.Bucket, key string) (io.ReadCloser, error) {
		return bucket.GetObject(key)
	}
	ossGetObjectDetailedMeta = func(bucket *oss.Bucket, key string) (http.Header, error) {
		return bucket.GetObjectDetailedMeta(key)
	}
	ossDeleteObject = func(bucket *oss.Bucket, key string) error {
		return bucket.DeleteObject(key)
	}
	ossListObjects = func(bucket *oss.Bucket, options ...oss.Option) (oss.ListObjectsResult, error) {
		return bucket.ListObjects(options...)
	}
)

// OSSStorage 基于 OSS 的存储引擎实现
// OSS 支持条件 PUT，但不支持原子条件删除，释放走墓碑降级路径
type OSSStorage struct {
	options      *OSSStorageOptions
	bucket       *oss.Bucket
	timeProvider storage.TimeProvider
}

var _ storage.Storage = &OSSStorage{}

func NewOSSStorage(ctx context.Context, options *OSSStorageOptions) (*OSSStorage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &OSSStorage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const OSSStorageName = "oss-storage"

func (x *OSSStorage) GetName() string {
	return OSSStorageName
}

func (x *OSSStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityWatch,
	}
}

func (x *OSSStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}

func (x *OSSStorage) Init(ctx context.Context) error {
	bucket, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.bucket = bucket

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

func (x *OSSStorage) objectKey(lockId string) string {
	return fmt.Sprintf("%s/lock/%s", x.options.KeyPrefix, lockId)
}

func encodeValue(version storage.Version, jsonStr string) []byte {
	return []byte(strconv.FormatUint(uint64(version), 10) + valueSeparator + jsonStr)
}

func decodeValue(data []byte) (storage.Version, string, error) {
	parts := strings.SplitN(string(data), valueSeparator, 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid oss value format")
	}
	version, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return storage.Version(version), parts[1], nil
}

// CreateWithVersion 原子创建锁记录
// OSS 条件 PUT：使用 x-oss-forbid-overwrite=true 防止覆盖
func (x *OSSStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)
	value := encodeValue(version, lockInformation.ToJsonString())

	options := []oss.Option{
		oss.ForbidOverWrite(true),
	}
	err := ossPutObject(x.bucket, key, bytes.NewReader(value), options...)
	if err != nil {
		if isPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
// 流程：GetObjectMeta 取 ETag → GetObject 取内容 → 检查版本 → PutObject with If-Match
func (x *OSSStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	// 先获取对象元数据（包含 ETag）
	meta, err := ossGetObjectDetailedMeta(x.bucket, key)
	if err != nil {
		if isNotFoundError(err) {
			// 记录不存在返回 ErrVersionMiss（可重试），对齐 S3 基类/memory 已修口径。
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	etag := meta.Get("ETag")

	// 再获取对象内容
	result, err := ossGetObject(x.bucket, key)
	if err != nil {
		if isNotFoundError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	data, err := io.ReadAll(result)
	result.Close()
	if err != nil {
		return err
	}

	// 检查版本
	currentVersion, _, err := decodeValue(data)
	if err != nil {
		return err
	}
	if currentVersion != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 使用 If-Match ETag 条件更新
	newValue := encodeValue(newVersion, lockInformation.ToJsonString())
	options := []oss.Option{
		oss.IfMatch(etag),
	}
	err = ossPutObject(x.bucket, key, bytes.NewReader(newValue), options...)
	if err != nil {
		if isPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// DeleteWithVersion OSS 不支持原子条件删除，采用读取-检查-删除降级方案
func (x *OSSStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	result, err := ossGetObject(x.bucket, key)
	if err != nil {
		if isNotFoundError(err) {
			return storage_lock.ErrLockNotFound
		}
		return err
	}
	data, err := io.ReadAll(result)
	result.Close()
	if err != nil {
		return err
	}

	currentVersion, _, err := decodeValue(data)
	if err != nil {
		return err
	}
	if currentVersion != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	return ossDeleteObject(x.bucket, key)
}

func (x *OSSStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.objectKey(lockId)
	result, err := ossGetObject(x.bucket, key)
	if err != nil {
		if isNotFoundError(err) {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	defer result.Close()
	data, err := io.ReadAll(result)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", storage_lock.ErrLockNotFound
	}
	_, jsonStr, err := decodeValue(data)
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}

func (x *OSSStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

func (x *OSSStorage) Close(ctx context.Context) error {
	x.bucket = nil
	return nil
}

func (x *OSSStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	prefix := fmt.Sprintf("%s/lock/", x.options.KeyPrefix)
	slice := make([]*storage.LockInformation, 0)

	marker := ""
	for {
		lsRes, err := ossListObjects(x.bucket, oss.Prefix(prefix), oss.Marker(marker), oss.MaxKeys(1000))
		if err != nil {
			return nil, err
		}

		for _, object := range lsRes.Objects {
			result, err := ossGetObject(x.bucket, object.Key)
			if err != nil {
				continue
			}
			data, err := io.ReadAll(result)
			result.Close()
			if err != nil || len(data) == 0 {
				continue
			}
			_, jsonStr, err := decodeValue(data)
			if err != nil {
				continue
			}
			lockInfo, err := storage.LockInformationFromJsonString(jsonStr)
			if err != nil {
				continue
			}
			slice = append(slice, lockInfo)
		}

		if !lsRes.IsTruncated {
			break
		}
		marker = lsRes.NextMarker
	}

	return go_iterator.FromSlice(slice), nil
}

// isPreconditionError 检查是否为前置条件失败错误
func isPreconditionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "412") ||
		strings.Contains(errStr, "condition") ||
		strings.Contains(errStr, "Conflict") ||
		strings.Contains(strings.ToLower(errStr), "forbid overwrite")
}

// isNotFoundError 检查是否为对象不存在错误
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "NoSuchKey") ||
		strings.Contains(errStr, "404") ||
		strings.Contains(errStr, "does not exist")
}
