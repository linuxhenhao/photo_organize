# 照片整理工具 (Photo Organizer)

## 概述
`photo_organize` 是一款基于 Go 语言的高性能命令行工具，用于整理海量照片和视频。它将源文件夹扫描到 SQLite 数据库中，并根据元数据将文件整理到结构化目标目录，支持**感知图像匹配**等高级去重功能。

## 功能特性
- **并行扫描**: 使用 10 个 goroutine 工作池进行并发元数据提取。
- **智能元数据提取**: 
    - 优先使用 EXIF 数据（通过 `exiftool`）以获取准确的创建日期。
    - 备选方案包括文件名日期模式匹配和文件系统出生时间。
- **高级去重**:
    - **二进制匹配**: 使用 MMH3 哈希值识别完全相同的文件。
    - **感知匹配**: 使用 **dHash** 和自定义 **BK-Tree** (BK树) 识别视觉相似的图像（例如缩略图或不同分辨率的相同图片）。
- **高性能 Web UI 缓存**:
    - 完整的元数据和缩略图关联以原生 **JSON** 格式缓存在 SQLite 中。
    - 在 Web UI 中浏览重复组时，实现零磁盘 I/O。
- **优化存储**: 
    - 数据库采用 **WAL (预写日志)** 模式，确保高并发下的稳定性。
    - 采用事务批处理更新，提升大规模扫描性能。
    - **Actor 模式缓存管理**: 使用 SQLite 的原子 JSON 操作在后台异步持久化目标状态。
- **结构化组织**: 
    - 将文件导入到 `[目标目录]/年/月/日/` 层次结构中。
    - 自动冲突解决，通过添加后缀（如 `-1`, `-2`）处理重名不同内容的文件。

## 安装说明

### 前提条件
- **Go**: 1.24 或更高版本。
- **Exiftool**: 必须已安装并在系统的 `PATH` 路径中可用。

### 构建
```bash
go build -o photo-organizer ./cmd/photo-organizer
```

## 使用方法

### 1. 扫描源文件夹
扫描您的源文件夹以填充元数据数据库。
```bash
./photo-organizer scan -db photos.db -src /路径/到/照片1,/路径/到/照片2
```
- `-db`: SQLite 数据库路径（默认为 `photos.db`）。
- `-src`: 逗号分隔的源文件夹列表。

### 2. 导入到目标目录
将数据库中的文件复制到已整理的照片库中，并自动执行去重。
```bash
./photo-organizer import -db photos.db -dest /路径/到/整理后的文件夹
```

### 3. 初始化目标缓存
预先索引现有的已整理目录，以避免在未来的导入中重复计算哈希值。
```bash
./photo-organizer initcache -dest /路径/到/整理后的文件夹
```

### 4. 启动 Web UI 进行去重
启动交互式 Web 界面来解决视觉重复项。
```bash
./photo-organizer serve -dest /路径/到/整理后的文件夹 -port 8080
```

## 开发相关
- **测试数据**: 使用 `test_data/` 进行本地实验。
- **集成测试**: 运行 `./integration_test.sh` 验证构建和核心功能。

## 数据库结构
该工具在其 SQLite 数据库中使用两个主要表：
- `photos` (存放在 `photo.db`): 包含 `source_path`, `size`, `create_time`, `mmh3_hash`, `phash`, `group_id`, `mime_type`。
- `file_cache` (存放在 `cache.db`): 包含 `target_path`, `mmh3_hash`, `phash`, `size`, `metadata` (JSON), `thumbnails` (JSON 数组)。

## 许可协议
MIT