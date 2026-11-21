# 媒体库离线数据分片

由 GitHub Actions 自动生成，每轮覆盖（不是一个持续追加的仓库）。

- `catalog.json` — 清单，App 只读这一个
- `*.jsonl.gz` — 分片，一行一部
- `*.txt.gz` — 失效名单（已确认下架的 series_id，一行一个）。App 装进本地
  gone 表，搜索时把这些过滤掉，免得用户点进去发现放不了。**主分片不剔除它们**：
  分片按 id 升序切，抽掉中间的条目会让每片的边界和指纹全变，所有人都要重下。
- `scripts/`、`.github/` — 抓取脚本与工作流

**本仓库整个树必须小于 50MB**（jsDelivr 对单个 ref 的硬上限，超了整站 403）。
抓取库快照不在这个分支，走 `state-keep`。
- `scripts/` — 抓取脚本

分发走 jsDelivr，前缀见 `catalog.json` 里各分片的 `url` 字段。
