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
    - **感知匹配**: 使用 **dHash** 和自定义 **BK-Tree** (BK树) 识别视觉相似的图像。
    - **AI 深度验证**: 可选的 ORB 特征匹配 (OpenCV) 用于深入验证缩略图与主图的关系。
- **高性能 Web UI**:
    - 独立的、轻量化的 Web UI 二进制文件 (`photo-web-ui`) 用于解决重复项。
    - 完整的元数据和缩略图关联以原生 **JSON** 格式缓存在 SQLite 中。
- **优化存储**: 
    - 数据库采用 **WAL (预写日志)** 模式，确保高并发下的稳定性。
    - 采用事务批处理更新，提升大规模扫描性能。
- **结构化组织**: 
    - 将文件导入到 `[目标目录]/年/月/日/` 层次结构中。
    - 自动冲突解决，通过添加后缀（如 `-1`, `-2`）处理重名不同内容的文件。

## 安装说明

### 前提条件
- **Go**: 1.24 或更高版本。
- **Exiftool**: 必须已安装并在系统的 `PATH` 路径中可用。
- **OpenCV (可选)**: 仅当需要构建包含高级 ORB 验证功能的 `photo-organizer` 时需要。

### 构建
项目生成两个主要的可执行文件：

1. **photo-organizer**: 用于扫描、导入和维护的核心命令行工具。**默认支持 OpenCV (gocv)** 以进行高级 ORB 验证。
   ```bash
   # 标准构建 (包含 OpenCV/ORB 支持)
   go build -tags gocv -o photo-organizer ./cmd/photo-organizer

   # 轻量化构建 (无 OpenCV 依赖)
   go build -o photo-organizer ./cmd/photo-organizer
   ```

2. **photo-web-ui**: 用于处理重复项的轻量级 Web 界面。此二进制文件**不依赖 OpenCV**，可在任何环境中运行。
   ```bash
   go build -o photo-web-ui ./cmd/photo-web-ui
   ```

## 使用方法

### 1. 扫描源文件夹
扫描您的源文件夹以填充元数据数据库。
```bash
./photo-organizer scan -db photos.db -src /路径/到/照片1,/路径/到/照片2
```

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
./photo-web-ui -dest /路径/到/整理后的文件夹 -host 127.0.0.1 -port 8080
```
- `-dest`: 包含 `cache.db` 的已整理目录。
- `-host`: 绑定 IP 地址 (默认 `127.0.0.1`)。
- `-port`: Web 服务器端口 (默认 `8080`)。

## 开发相关
- **集成测试**: 运行 `./integration_test.sh` 验证构建和核心功能。

## 许可协议
MIT
