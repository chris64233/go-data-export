package dataexport

import (
	"errors"
	"fmt"
)

// Code 是稳定的机器可读错误类别，调用方应基于 Code 而非错误文案做分支。
type Code string

const (
	CodeInvalidArgument   Code = "INVALID_ARGUMENT"
	CodeTaskNotFound      Code = "TASK_NOT_FOUND"
	CodeTaskConflict      Code = "TASK_CONFLICT" // 同一外部请求号提交了不同的导出范围
	CodeTaskNotRunning    Code = "TASK_NOT_RUNNING"
	CodeShardNotFound     Code = "SHARD_NOT_FOUND"
	CodeNoShardAvailable  Code = "NO_SHARD_AVAILABLE"
	CodeLeaseMismatch     Code = "LEASE_MISMATCH" // 租约 ID 或围栏令牌与当前持有者不一致
	CodeLeaseExpired      Code = "LEASE_EXPIRED"
	CodeVersionMismatch   Code = "VERSION_MISMATCH"   // 回执的任务执行版本与当前版本不一致
	CodeWatermarkMismatch Code = "WATERMARK_MISMATCH" // 回执的快照水位与任务固定水位不一致
	CodeReceiptConflict   Code = "RECEIPT_CONFLICT"   // 回执与已接受内容冲突，拒绝覆盖
	CodeManifestNotReady  Code = "MANIFEST_NOT_READY"
	CodeManifestExpired   Code = "MANIFEST_EXPIRED"
	CodeObjectStore       Code = "OBJECT_STORE_FAILURE"
)

// Error 是服务返回的唯一错误类型。
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func errorf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf 提取错误的 Code；非服务错误返回 ok=false。
func CodeOf(err error) (code Code, ok bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Code, true
	}
	return "", false
}

// IsCode 判断 err 是否属于给定错误类别。
func IsCode(err error, code Code) bool {
	c, ok := CodeOf(err)
	return ok && c == code
}
