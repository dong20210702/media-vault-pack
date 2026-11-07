# 媒体库离线数据分片

由 GitHub Actions 自动生成，每轮覆盖（不是一个持续追加的仓库）。

- `catalog.json` — 清单，App 只读这一个
- `*.jsonl.gz` — 分片，一行一部
- `state/` — 抓取库快照，只给下一轮 Actions 恢复用，App 不下载
- `scripts/` — 抓取脚本

分发走 jsDelivr，前缀见 `catalog.json` 里各分片的 `url` 字段。
