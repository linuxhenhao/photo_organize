# 照片整理工具

## 概述
一个命令行工具，通过扫描源文件夹到SQLite数据库，并根据数据库信息将文件整理到目标目录，支持去重和结构化组织。

## 功能特性
- **扫描命令（scan）**: 遍历目录收集文件元数据（路径、大小、创建时间）到SQLite数据库。
  - 使用10个goroutine并行处理。
  - 创建时间优先级：EXIF数据 > 文件名日期模式 > 文件元信息。
  - 为相同大小的文件计算MMH3哈希值。
  - 为相同哈希的文件分配组ID。
- **导入命令（import）**: 从数据库复制文件到目标目录。
  - 去重策略：每个组ID仅复制第一个文件。
  - 组织规则：按创建时间生成`[目标目录]/年/月/日/文件名`路径。
  - 冲突解决：同名文件内容不同时添加`-n`后缀（n递增）。
  - 进度跟踪：追加写入`stats.txt`，每200行强制刷盘。

## 安装
1. 安装Go 1.18及以上版本
2. 安装exiftool（用于提取EXIF元数据）
3. 克隆仓库：`git clone https://github.com/your-repo/photo-organize.git`
4. 构建：`go build -o photo-organizer`

## 使用说明
### 扫描
`./photo-organizer scan -db photos.db -src /路径/到/照片1,/路径/到/照片2`
- `-db`: SQLite数据库路径（默认：photos.db）
- `-src`: 逗号分隔的源目录

### 导入
`./photo-organizer import -db photos.db -dest /路径/到/整理后的照片`

### 初始化缓存（initcache）
`./photo-organizer initcache -dest /路径/到/整理后的照片`
- `-dest`: 目标目录路径（已存在的整理目录）
- 作用：预先生成目标目录的MMH3哈希缓存文件`mmh3_hash_cache.txt`，避免重复导入时重新计算已有文件的哈希值，提升`import`命令效率。适用于目标目录已存在部分文件的场景。
- `-db`: SQLite数据库路径
- `-dest`: 整理后照片的目标目录

## 数据库结构
| 列名           | 类型    | 描述                               |
|----------------|---------|------------------------------------|
| source_path    | TEXT    | 源文件绝对路径（主键）             |
| size           | INTEGER | 文件大小（字节）                   |
| create_time    | TEXT    | RFC3339格式的创建时间              |
| mmh3_hash      | TEXT    | MMH3哈希值（唯一大小文件为空）     |
| group_id       | INTEGER | 组ID（0表示唯一，>0表示重复组）    |

## 许可
MIT