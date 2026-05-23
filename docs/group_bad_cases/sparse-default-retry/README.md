# Sparse Default Retry

这个 bad case 目录记录的是一类 “`sparse` 已经 `ready`，但关键点过少” 的回归。

这里放的是从 `2013-02-05 14.59.19` 样本做过强脱敏后的测试资产：

- `2013-02-05-14.59.19-anon-main.jpg`
- `2013-02-05-14.59.19-anon-default.jpg`

处理原则：

- 转灰度
- 降采样再放大
- 遮挡人脸区域
- 降低可识别细节，但尽量保留低纹理和缩略派生关系

对应测试覆盖两件事：

- 脱敏后的 `default` 夹具仍然会落入 `sparse ready + low keypoints` 这类输入
- 当 `sparse` 点数低于匹配门槛时，提取逻辑会重试 `Akaze::default()`，并优先使用更丰富的结果
