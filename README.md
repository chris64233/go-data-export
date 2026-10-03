# go-data-export

用于承载用户数据导出、分片生成与文件生命周期管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

运行测试：

    go test ./...

## 导出结果分片下载授权

本轮在导出完成后增加访问控制层（`grant.go`、`service.go`），不改动快照、
分片恢复与租约接管逻辑。

### 授权模型

- `Service.CreateGrant` 签发下载授权，固定导出清单摘要、接收方、可下载
  分片范围 `[Start, End)`、有效期（TTL）与最大下载次数。
- 相同授权号重复提交且固定字段一致时返回原结果；清单摘要、接收方或范围
  发生变化时返回 `ErrGrantConflict`；已过期授权不会被重复提交重新激活。

### 下载流程

1. `RequestDownload` 校验授权号、接收方、清单摘要与分片范围必须同时匹配，
   任何一项不一致即拒绝；凭证绑定授权中的导出任务标识，请求方无法换取
   其他任务的文件。越界范围返回 `ErrRangeOutOfBounds`，重复下载（含在途）
   返回 `ErrShardAlreadyDownloaded`。
2. `ConfirmDownload` 确认下载完成，合并已下载范围、累计次数并写入审计
   记录；达到最大次数或覆盖全部授权范围后进入 `completed` 终态。

### 并发与终态

- 所有状态迁移在同一把互斥锁下串行执行，撤销（`Revoke`）、下载确认
  （`ConfirmDownload`）与过期扫描（`ExpireScan`）并发时只形成一个终态
  （`completed` / `revoked` / `expired`），已完成的下载保留审计记录。

### 查询与日志

- `GetGrant` 返回授权状态、已下载范围、下载次数、撤销原因与最后访问时间。
- 审计记录（`AuditLog`）与 `DownloadTicket.Redacted` 均不包含下载地址，
  普通日志不得输出 `DownloadTicket.URL`。
