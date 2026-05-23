# 派生文件名重复组调查

基于 `2026-05-01` 从 `nas-photo:/volume3/DocsAndMedia/Multimedia/repo/repo.db` 导出的 `target_items` 做了一次面向“主文件明显更大，且文件名能看出派生关系”的重复组排查。

## 筛选口径

- 只看 `group_id IS NOT NULL`、组内至少 2 个成员、且恰好有 1 个 `is_group_primary = 1` 的重复组。
- 对组内每个非 primary 成员，只有同时满足下面条件才记为候选 pair：
  - `primary_size / other_size >= 5`
  - `max(primary_width, primary_height) >= 2 * max(other_width, other_height)`
  - `primary` 的最大边至少 `1200px`
  - 非 primary 文件名落在明显的派生命名家族里：`default*`、`*embedded*`、`*shotwell*`、时间戳版 `YYYYMMDD-HHMMSS*`

这个口径不是“所有误判”，而是专门压出“看文件名就很像派生物”的一类 case。

## 总体统计

- 重复组总数：`10,277`
- 分组成员总数：`28,413`
- 命中候选 pair：`10,774`
- 命中候选 group：`8,150`
- 其中带“邻号/错号 default”特征的 pair：`97`
- 其中带“邻号/错号 default”特征的 group：`54`
- `54` 个邻号 group 里，有 `47` 个同时也含有“同号 default”成员，说明不少组更像是先有正常派生链，再被少量邻号缩略图桥接进来。

## 主要模式

按 pair 数量看，最常见的是：

| 类别 | pair | group | 说明 |
| --- | ---: | ---: | --- |
| `default_camera:same_core` | 6131 | 5392 | `IMG_5793.CR2` 对 `defaultimg_5793-2.cr2` 这类同号派生 |
| `default_other:same_core` | 1442 | 1314 | `defaultdpp_*`、`default<hash>` 之类的同核派生 |
| `timestamp_rendition:*` | 1949 | 1351 | `IMG_20200305_144300.jpg` 对 `20200305-144300-2.jpg` 这类时间戳版本 |
| `default_embedded:same_core` | 379 | 326 | `defaultimg_*_embedded.jpg` |
| `default_shotwell:same_core` | 268 | 254 | `defaultimg_*_shotwell.jpg` |

更值得单独盯的是这些“邻号/错号 default”：

| 类别 | pair | group | 说明 |
| --- | ---: | ---: | --- |
| `default_camera:adjacent_id` | 53 | 34 | `DSC00758.ARW` 对 `defaultdsc00759.arw` |
| `default_embedded:adjacent_id` | 21 | 15 | `IMG_7460_CR2_embedded.jpg` 对 `defaultimg_7459_cr2_embedded.jpg` |
| `default_shotwell:adjacent_id` | 19 | 13 | `IMG_2015_CR2_shotwell.jpg` 对 `defaultimg_2014_cr2_shotwell.jpg` |

这些邻号 case 更接近“组内存在派生物，但派生物并不都属于 primary”。

## 代表性 group

- `group 3`
  - `IMG_0162.JPG`
  - `IMG_20171215_134834_0162.JPG`
  - `20171215-134834-3.jpg`
  - `20171215-134834.jpg`
  - `defaultimg_0162-3.jpg`
  - 这是标准的“原图 -> 时间戳重命名 -> default 缩略图”链条。
- `group 2042`
  - `img_1823.cr2`
  - `1-defaultimg_1823_cr2_shotwell.jpg`
  - 这是 RAW 对 Shotwell/default 派生预览的极端尺寸差样本。
- `group 8934`
  - `DSC00977.ARW`
  - `20240330-185345997.arw`
  - `DSC00977.JPG`
  - `20240330-185345997.jpg`
  - `defaultdsc00977.arw`
  - `defaultdsc00977.jpg`
  - 这是 Sony RAW/JPEG 与 default 预览并存的典型组。
- `group 8082`
  - `DSC00758.ARW`
  - `DSC00759.ARW`
  - `defaultdsc00758.arw`
  - `defaultdsc00759.arw`
  - 这是最值得继续追的“同组里既有同号 default，又有邻号 default”样本。
- `group 5735`
  - `IMG_7457` 到 `IMG_7461` 的 embedded/shotwell/default 全混在一个组里
  - 这是“邻近帧 + 多种派生物一起塌组”的代表样本。

## 本地产物

- 原始导出：[grouped_items.tsv](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/data/grouped_items.tsv)
- 候选 pair 明细：[derived_candidate_pairs.tsv](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/data/derived_candidate_pairs.tsv)
- 分类汇总：[category_summary.tsv](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/data/category_summary.tsv)
- 邻号 case 明细：[adjacent_id_cases.tsv](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/data/adjacent_id_cases.tsv)
- 组级概览：[group_summary.tsv](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/data/group_summary.tsv)
- 代表组快照：
  - [group-3](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/snapshots/group-3)
  - [group-2042](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/snapshots/group-2042)
  - [group-5735](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/snapshots/group-5735)
  - [group-8082](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/snapshots/group-8082)
  - [group-8934](/home/huangyu/workspace/gitrepo/photo_organize/docs/derived-filename-duplicate-investigation/snapshots/group-8934)

## 结论

当前库里，这类“primary 明显更大，文件名又能看出派生关系”的组不是零星个例，而是一个很大的族群。

其中绝大多数是正常的同号派生物：

- `defaultimg_5793-2.cr2`
- `defaultimg_5808_cr2_embedded.jpg`
- `1-defaultimg_1823_cr2_shotwell.jpg`
- `20200305-144300-2.jpg`

但至少还有一批更可疑的“邻号 default/preview 混入”：

- `DSC00758.ARW` vs `defaultdsc00759.arw`
- `IMG_7460_CR2_embedded.jpg` vs `defaultimg_7459_cr2_embedded.jpg`

这批 group 很适合下一步针对“弱边如何把相邻帧的 default/embedded/shotwell 预览桥接进来”继续排查。
