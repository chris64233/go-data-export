package dataexport

import (
	"encoding/json"
	"net/http"
	"strings"
)

// HTTPHandler 把 Service 暴露为 REST 风格接口：
//
//	POST   /v1/tasks                       创建任务（幂等：X-Request-Id + scope）
//	GET    /v1/tasks/{id}                  读取任务
//	POST   /v1/tasks/{id}/cancel           取消任务
//	POST   /v1/tasks/{id}/claims           工作者领取一个分片（X-Worker-Id）
//	POST   /v1/tasks/{id}/shards/{idx}/receipts   提交分片回执
//	GET    /v1/tasks/{id}/manifest         读取不可变清单
//	GET    /v1/tasks/{id}/shards/{idx}/object      下载分片对象（保留期内）
//	POST   /v1/maintenance/expire          过期扫描终结任务
//	POST   /v1/maintenance/cleanup         过期对象清理
//
// 所有业务错误以 {"error":{"code":...,"message":...}} 返回，并映射到稳定的
// HTTP 状态码（见 statusForErrorCode）。
type HTTPHandler struct {
	svc *Service
	mux *http.ServeMux
}

// NewHTTPHandler 装配路由。
func NewHTTPHandler(svc *Service) *HTTPHandler {
	h := &HTTPHandler{svc: svc, mux: http.NewServeMux()}
	h.routes()
	return h
}

// ServeHTTP 实现 http.Handler。
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *HTTPHandler) routes() {
	h.mux.HandleFunc("POST /v1/tasks", h.createTask)
	h.mux.HandleFunc("GET /v1/tasks/{id}", h.getTask)
	h.mux.HandleFunc("POST /v1/tasks/{id}/cancel", h.cancelTask)
	h.mux.HandleFunc("POST /v1/tasks/{id}/claims", h.claimShard)
	h.mux.HandleFunc("POST /v1/tasks/{id}/shards/{idx}/receipts", h.submitReceipt)
	h.mux.HandleFunc("GET /v1/tasks/{id}/manifest", h.getManifest)
	h.mux.HandleFunc("GET /v1/tasks/{id}/shards/{idx}/object", h.getObject)
	h.mux.HandleFunc("POST /v1/maintenance/expire", h.expire)
	h.mux.HandleFunc("POST /v1/maintenance/cleanup", h.cleanup)
}

type createTaskRequest struct {
	Scope     Scope       `json:"scope"`
	Watermark int64       `json:"watermark"`
	Shards    []ShardSpec `json:"shards"`
}

func (h *HTTPHandler) createTask(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
	var req createTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, ErrInvalidArgument)
		return
	}
	t, err := h.svc.CreateTask(CreateTaskInput{
		RequestID: requestID,
		Scope:     req.Scope,
		Watermark: req.Watermark,
		Shards:    req.Shards,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *HTTPHandler) getTask(w http.ResponseWriter, r *http.Request) {
	t, err := h.svc.GetTask(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *HTTPHandler) cancelTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Reason string `json:"reason"`
	}
	_ = decodeJSON(r, &body) // body 可省略
	t, err := h.svc.CancelTask(r.PathValue("id"), body.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *HTTPHandler) claimShard(w http.ResponseWriter, r *http.Request) {
	workerID := strings.TrimSpace(r.Header.Get("X-Worker-Id"))
	c, err := h.svc.ClaimShardOf(r.PathValue("id"), workerID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

type receiptRequest struct {
	ExecutionVersion int64  `json:"execution_version"`
	LeaseGeneration  int64  `json:"lease_generation"`
	ObjectKey        string `json:"object_key"`
}

func (h *HTTPHandler) submitReceipt(w http.ResponseWriter, r *http.Request) {
	workerID := strings.TrimSpace(r.Header.Get("X-Worker-Id"))
	idx, ok := parseIndex(r.PathValue("idx"))
	if !ok {
		writeError(w, ErrInvalidArgument)
		return
	}
	var req receiptRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, ErrInvalidArgument)
		return
	}
	res, err := h.svc.SubmitReceipt(ReceiptInput{
		TaskID:           r.PathValue("id"),
		Index:            idx,
		ExecutionVersion: req.ExecutionVersion,
		LeaseGeneration:  req.LeaseGeneration,
		WorkerID:         workerID,
		ObjectKey:        req.ObjectKey,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) getManifest(w http.ResponseWriter, r *http.Request) {
	m, err := h.svc.GetManifest(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (h *HTTPHandler) getObject(w http.ResponseWriter, r *http.Request) {
	idx, ok := parseIndex(r.PathValue("idx"))
	if !ok {
		writeError(w, ErrInvalidArgument)
		return
	}
	data, err := h.svc.GetShardObject(r.PathValue("id"), idx)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (h *HTTPHandler) expire(w http.ResponseWriter, r *http.Request) {
	ids, err := h.svc.ExpireDueTasks()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"expired_task_ids": ids})
}

func (h *HTTPHandler) cleanup(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CandidateKeys []string `json:"candidate_keys"`
	}
	_ = decodeJSON(r, &body)
	res, err := h.svc.CleanupExpiredWithCandidates(body.CandidateKeys)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// ---- HTTP 辅助 ----

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	code := CodeOf(err)
	if code == "" {
		code = ErrCodeInvalidArgument
	}
	writeJSON(w, statusForErrorCode(code), map[string]any{
		"error": map[string]string{"code": string(code), "message": err.Error()},
	})
}

// statusForErrorCode 把业务错误码映射到 HTTP 状态码。
func statusForErrorCode(code ErrorCode) int {
	switch code {
	case ErrCodeInvalidArgument:
		return http.StatusBadRequest
	case ErrCodeNotFound, ErrCodeManifestNotFound, ErrCodeObjectNotFound:
		return http.StatusNotFound
	case ErrCodeRequestConflict:
		return http.StatusConflict
	case ErrCodeExecutionVersionMismatch, ErrCodeLeaseStale,
		ErrCodeDigestMismatch, ErrCodeSnapshotMismatch,
		ErrCodeShardAlreadyCompleted:
		return http.StatusConflict
	case ErrCodeLeaseExpired, ErrCodeTaskExpired, ErrCodeManifestExpired:
		return http.StatusGone
	case ErrCodeNoShardAvailable:
		return http.StatusConflict
	case ErrCodeTaskCanceled, ErrCodeTaskCompleted, ErrCodeTaskTerminal:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func parseIndex(s string) (int, bool) {
	n := 0
	if s == "" {
		return 0, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
