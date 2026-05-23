# 端到端回归测试计划

## Summary

新增一组基于 Rust integration test 的真实端到端测试，直接 spawn `photo-org` 二进制，走实际命令行和 HTTP API，而不是直接调用内部函数或 handler。

第一轮按“全 API 套件”做，但范围控制在本地可稳定运行的小型 fixture 上，重点验证：

- CLI 子命令可正常运行
- `serve` 可真实启动并对外提供 HTTP
- 主要读写 API 在真实网络边界上可用
- DB 状态、文件移动/删除、空目录清理等副作用正确
- `dest=repo` 和 `dest=./repo` 两种启动方式行为一致

## Key Changes

### 1. 新增 Rust 端到端测试 harness

新增独立 integration test 文件，统一负责：

- 创建临时工作目录和小型测试库
- 通过 `Command` 启动 `photo-org scan/import/initcache/serve`
- 为 `serve` 选择空闲本地端口
- 轮询等待服务 ready，而不是固定 `sleep`
- 用真实 HTTP 客户端访问接口
- 在测试结束时回收子进程并打印 stdout/stderr 便于排错

默认实现细节：

- 直接用标准库 `Command` 启动 `env!("CARGO_BIN_EXE_photo-org")`
- HTTP 客户端优先用阻塞型轻量依赖；若不想加依赖，则用 `std::net::TcpStream` + 原始 HTTP 请求封装最小 client
- 统一 helper：`spawn_serve`, `wait_for_http_ok`, `get_json`, `post_json`, `read_catalog_state`

### 2. 端到端覆盖 CLI 主流程

把现有 `integration_test.sh` 的核心能力迁到 Rust integration test 中，形成 `cargo test` 可直接跑的稳定回归：

- `scan`：能生成 `scan.db`
- `import`：能生成 `catalog.db` 和目标树
- `initcache`：能重新采用现有目标树，且不会复制文件
- `serve`：能真实监听 HTTP 并返回有效 JSON/图片响应

这里不要求替换掉 `integration_test.sh`；shell 脚本保留为人工冒烟，但 CI/常规回归以 Rust integration test 为主。

### 3. 端到端覆盖主要 HTTP API

在真实 `serve` 进程上覆盖一套“全 API 套件”，每个用例都只通过 HTTP 和磁盘/SQLite 结果断言，不直接调用内部符号：

- `GET /api/groups`
  验证 pending / trash 两种视图都能返回预期结构
- `POST /api/groups/{id}/resolve`
  验证 reject 后文件进入 `.photo-org/trash/group-*`，DB `target_path` 和 `keep_state` 更新
- `POST /api/groups/resolve_bulk`
  验证批量 resolve 的 DB 与文件副作用
- `POST /api/groups/{id}/delete_trash`
  验证按组永久删除 trash 成员、必要时解组/补 primary
- `POST /api/groups/delete_trash_bulk`
  验证页面级批量删除 trash 成员
- `POST /api/groups/{group_id}/members/{member_id}/restore_trash`
  验证 restore 后文件回到正式目录、DB 恢复为 `kept`
- `POST /api/groups/{group_id}/members/{member_id}/delete_trash`
  验证单文件永久删除
- `GET /api/groups/{id}/archive`
  验证可读
- `GET /image`
  验证预览接口返回 200

### 4. 专门加入“路径表示等价性”回归

把这次问题上升成一类全程序回归，不只测一个 util 函数。

新增两组完全等价的端到端场景：

- `--dest repo`
- `--dest ./repo`

两组都跑真实 `serve` + 写接口，断言结果一致，尤其是：

- reject 后生成的 trash 路径一致可用
- delete_trash / delete_trash_bulk 成功
- 空目录清理不会误报 escape
- 所有 target_path/逻辑路径仍保持项目要求的 `repo/...` 形式

这类测试以后专门防“路径词法表示不同但语义相同”的回归。

## Test Plan

### 必测场景

- `scan -> import -> serve` 基本可用
- `initcache -> serve` 基本可用
- `resolve` 后 trash review 模式可看到目标组
- `delete_trash_bulk` 在 `dest=./repo` 下成功，不出现 cleanup-root escape
- `restore_trash` 后文件恢复、组状态正确
- `delete_trash_group` 和 `delete_trash_member` 对 survivor/primary 的后续状态正确
- `GET /image` 在至少一个真实 target_path 上返回 200

### 关键断言

- HTTP status code 正确
- JSON 结构与关键字段存在
- SQLite 中 `target_items`, `keep_state`, `group_id`, `is_group_primary` 符合预期
- 文件确实被移动/恢复/删除
- `.photo-org/trash/group-*` 空目录清理正确
- `repo` 与 `./repo` 两种入口行为一致

### 失败输出要求

- 所有 spawn 的子进程 stdout/stderr 在断言失败时回显
- HTTP 响应 body 在失败时打印
- DB 快照查询结果在关键断言失败时打印

## Assumptions

- 第一轮不追求覆盖每个 UI 交互细节，只覆盖所有后端公开 HTTP API 和 CLI 主流程
- 端到端测试数据继续使用现有小型 fixture，不拉入大型真实库
- `integration_test.sh` 保留为人工冒烟脚本，但不再作为主要防回归手段
- 路径等价性回归将以 `repo` vs `./repo` 为最低要求；若后续发现更多变体，再扩到绝对路径和符号链接场景
