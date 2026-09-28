package dataexport

import (
	"errors"
	"fmt"
)

// ErrorCode 是服务对外暴露的、稳定的错误类型标识。调用方应当依据错误码而不是
// 错误文本进行分支处理（HTTP 层也依据错误码映射状态码）。
type ErrorCode string

const (
	// ErrCodeInvalidArgument 请求参数本身不合法。
	ErrCodeInvalidArgument ErrorCode = "invalid_argument"
	// ErrCodeNotFound 任务 / 分片 / 对象 / 清单等资源不存在。
	ErrCodeNotFound ErrorCode = "not_found"
	// ErrCodeRequestConflict 同一外部请求号绑定了不同的导出范围。
	ErrCodeRequestConflict ErrorCode = "request_conflict"
	// ErrCodeExecutionVersionMismatch 回执携带的执行版本与任务当前执行版本不一致。
	ErrCodeExecutionVersionMismatch ErrorCode = "execution_version_mismatch"
	// ErrCodeLeaseStale 回执中的租约已不是该分片的当前租约（已被更高代际的租约取代）。
	ErrCodeLeaseStale ErrorCode = "lease_stale"
	// ErrCodeLeaseExpired 租约存在但已经超过到期时间。
	ErrCodeLeaseExpired ErrorCode = "lease_expired"
	// ErrCodeNoShardAvailable 当前没有可领取的分片（均已完成或租约仍有效）。
	ErrCodeNoShardAvailable ErrorCode = "no_shard_available"
	// ErrCodeDigestMismatch 回执摘要与快照水位上的期望摘要或对象实际摘要不一致。
	ErrCodeDigestMismatch ErrorCode = "digest_mismatch"
	// ErrCodeSnapshotMismatch 分片不属于任务创建时固定的快照水位。
	ErrCodeSnapshotMismatch ErrorCode = "snapshot_watermark_mismatch"
	// ErrCodeObjectNotFound 回执引用的上传对象在对象存储中不存在。
	ErrCodeObjectNotFound ErrorCode = "object_not_found"
	// ErrCodeShardAlreadyCompleted 分片已经接受过另一份回执，不能被覆盖。
	ErrCodeShardAlreadyCompleted ErrorCode = "shard_already_completed"
	// ErrCodeTaskCanceled 任务已取消，不再接受领取或回执。
	ErrCodeTaskCanceled ErrorCode = "task_canceled"
	// ErrCodeTaskExpired 任务已超过处理截止时间而进入过期终态。
	ErrCodeTaskExpired ErrorCode = "task_expired"
	// ErrCodeTaskCompleted 任务已完成（终态下不允许该操作）。
	ErrCodeTaskCompleted ErrorCode = "task_completed"
	// ErrCodeTaskTerminal 任务已经处于其他终态，无法再迁移到目标状态。
	ErrCodeTaskTerminal ErrorCode = "task_terminal"
	// ErrCodeManifestNotFound 任务尚未生成清单。
	ErrCodeManifestNotFound ErrorCode = "manifest_not_found"
	// ErrCodeManifestExpired 清单已超过保留期，下载必须被拒绝。
	ErrCodeManifestExpired ErrorCode = "manifest_expired"
)

// Error 是服务返回的所有业务错误的具体类型。
type Error struct {
	Code    ErrorCode
	Message string
	Details map[string]string
}

func (e *Error) Error() string {
	if len(e.Details) == 0 {
		return string(e.Code) + ": " + e.Message
	}
	return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Details)
}

// Is 使任意两个具有相同 ErrorCode 的 *Error 互相匹配，
// 因而 errors.Is(err, ErrLeaseStale) 这样的判断对动态构造的错误同样有效。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// 哨兵错误，供 errors.Is / errors.As 使用。
var (
	ErrInvalidArgument       = &Error{Code: ErrCodeInvalidArgument, Message: "invalid argument"}
	ErrNotFound              = &Error{Code: ErrCodeNotFound, Message: "resource not found"}
	ErrRequestConflict       = &Error{Code: ErrCodeRequestConflict, Message: "request id is bound to a different export scope"}
	ErrVersionMismatch       = &Error{Code: ErrCodeExecutionVersionMismatch, Message: "execution version mismatch"}
	ErrLeaseStale            = &Error{Code: ErrCodeLeaseStale, Message: "lease is stale"}
	ErrLeaseExpired          = &Error{Code: ErrCodeLeaseExpired, Message: "lease has expired"}
	ErrNoShardAvailable      = &Error{Code: ErrCodeNoShardAvailable, Message: "no shard available to claim"}
	ErrDigestMismatch        = &Error{Code: ErrCodeDigestMismatch, Message: "digest mismatch"}
	ErrSnapshotMismatch      = &Error{Code: ErrCodeSnapshotMismatch, Message: "shard does not belong to the task snapshot watermark"}
	ErrObjectNotFound        = &Error{Code: ErrCodeObjectNotFound, Message: "uploaded object not found"}
	ErrShardAlreadyCompleted = &Error{Code: ErrCodeShardAlreadyCompleted, Message: "shard receipt already accepted"}
	ErrTaskCanceled          = &Error{Code: ErrCodeTaskCanceled, Message: "task is canceled"}
	ErrTaskExpired           = &Error{Code: ErrCodeTaskExpired, Message: "task is expired"}
	ErrTaskCompleted         = &Error{Code: ErrCodeTaskCompleted, Message: "task is completed"}
	ErrTaskTerminal          = &Error{Code: ErrCodeTaskTerminal, Message: "task already in a terminal state"}
	ErrManifestNotFound      = &Error{Code: ErrCodeManifestNotFound, Message: "manifest not found"}
	ErrManifestExpired       = &Error{Code: ErrCodeManifestExpired, Message: "manifest download window has elapsed"}
)

func errorf(code ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func withDetails(err *Error, details map[string]string) *Error {
	err.Details = details
	return err
}

// CodeOf 返回错误对应的错误码；非业务错误返回空串。
func CodeOf(err error) ErrorCode {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
