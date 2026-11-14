#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
剧库数据包：抓全站 -> 切片打包 -> 传到 R2。

为什么要有这个脚本
------------------
App 里扩充剧库只能靠站内搜索（唯一免签的批量入口），一个词封顶 212 条，
实测清洗后约 89 部/词。攒到站方在架的六十多万部要六七千个词、十几万请求，
手机连跑几十天也追不上。所以在电脑上一次性抓全，切成分片放对象存储，
App 端「设置 - 全量剧库数据包」下载导入即可。

用法
----
    python tools/catalog_pack.py crawl            # 关键词抽样，断点续传
    python tools/catalog_pack.py sitemap          # 灌全站 id（29 个请求）
    python tools/catalog_pack.py fill --minutes 240   # 按 id 补齐长尾
    python tools/catalog_pack.py build --base https://pack.example.com
    python tools/catalog_pack.py upload           # 需要 R2_* 环境变量
    python tools/catalog_pack.py all --base ...

R2 环境变量
-----------
    R2_ACCOUNT_ID / R2_BUCKET / R2_ACCESS_KEY / R2_SECRET_KEY
    （R2_PUBLIC_BASE 可选，默认用 --base）

分片格式（App 端 CatalogPack 认这个）
------------------------------------
    catalog.json   {"version":1,"built":"..","total":N,"shards":[{"id","kind","url","bytes","count"}]}
    <id>.jsonl.gz  一行一部：{"i":"series_id","t":"剧名","c":"封面","e":82,"g":["古装"]}
"""

import argparse
import gzip
import hashlib
import hmac
import json
import math
import os
import random
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone

SITE = "https://hongguoduanju.com"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
ROOT = os.path.dirname(os.path.abspath(__file__))
WORK = os.path.join(ROOT, "_pack")
DB_PATH = os.path.join(WORK, "catalog.db")
OUT_DIR = os.path.join(WORK, "out")

# 每个词最多翻到第几页（实测 22 页左右就到顶，再往后是 404）
MAX_PAGES = 22
# 一页约 10 条
PER_PAGE = 10
# 一个分片放多少部。6 万部 ≈ 3MB（gzip 后），手机上下载/解压都轻松
SHARD_SIZE = 60000

# 分几个就该拆仓库了。jsDelivr 的 50MB 是**单个 GitHub 仓库**的上限
# （不是账号），超了连 catalog.json 都 403 —— 2026-11-13 踩过一次，
# 整站更新同时失效。按 2.95MB/片算 16 片 ≈ 47MB、第 17 片撞线，
# 所以定 15 片预警：拆仓库要新建仓库 + 改发布流程，得留出改代码的时间。
SPLIT_WARN_SHARDS = 15
# 体积兜底：万一某类分片突然变胖（比如主题词变长），片数没到也得提醒
SPLIT_WARN_BYTES = 45 * 1024 * 1024

# 词表清洗：命中这些就丢掉（实测能省掉约三成无效请求，产能反而升 18%）
DROP_CHARS = set("的了个是在有和就不人都一这那了")
DROP_SUFFIX = ("季", "部", "集", "篇", "版")
DROP_PREFIX = ("第",)


def log(*a):
    line = " ".join(str(x) for x in a)
    print(line, flush=True)
    # 同时落一份日志：跑几小时的活，窗口关掉/滚屏丢了还能回头看
    try:
        os.makedirs(WORK, exist_ok=True)
        with open(os.path.join(WORK, "crawl.log"), "a", encoding="utf-8") as f:
            f.write("%s  %s\n" % (datetime.now().strftime("%m-%d %H:%M:%S"), line))
    except Exception:
        pass


def load_env():
    """读 tools/.env.pack（KEY=VALUE 一行一个）。

    比每次开终端 set 环境变量省事，也不用把密钥贴进聊天记录。
    已经存在的环境变量优先，不覆盖。
    """
    path = os.path.join(ROOT, ".env.pack")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and v:
                os.environ.setdefault(k, v)


# ----------------------------------------------------------------------
# 抓取
# ----------------------------------------------------------------------

def http_json(url, timeout=20):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": SITE + "/search",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    if not raw:
        raise IOError("空响应")
    return json.loads(raw.decode("utf-8", "replace"))


def search(word, page):
    q = urllib.parse.quote(word)
    data = http_json("%s/api/search/result/?query=%s&page=%d" % (SITE, q, page))
    if data.get("code") not in (None, "ok", "0"):
        raise IOError(data.get("message") or data.get("code"))
    items = []
    for row in data.get("searchList") or []:
        vd = row.get("video_data") or {}
        sid = (vd.get("series_id") or "").strip()
        if not sid:
            continue
        items.append({
            "i": sid,
            "t": (vd.get("series_title") or row.get("name") or sid).strip(),
            "c": vd.get("series_cover") or "",
            "e": int(vd.get("episode_cnt") or 0),
            "g": [c.get("name") for c in (vd.get("category_list") or []) if c.get("name")],
            "k": "comic-drama" if str(vd.get("genre") or "") == "205" else "real-drama",
        })
    pages = int(data.get("totalPages") or page)
    return items, pages


# ---------------------------------------------------------------- id 穷举
#
# 为什么还要这一套：站内搜索是「抽样」——一次请求换 10 部完整信息，很便宜，
# 但覆盖面完全取决于词表。实测 2000 个词之后词表就干涸了，库卡在 1.8 万部。
# 而站点的 sitemap 直接给出全站 611,059 个 series_id（29 个分片，19 秒扫完），
# 每个 id 的 /detail 页 SSR 里就带着剧名/封面/集数/题材。
#   搜索 = 便宜但抽样；sitemap + detail = 贵但确定能走完。
# 两条腿一起跑：先用搜索把热门铺开，剩下的长尾按 id 一个个补。


def http_text(url, timeout=30):
    """抓 HTML。详情页是 gzip 压缩的，urllib 不会自动解，得自己来。"""
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Accept-Encoding": "gzip",
        "Referer": SITE + "/",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        enc = (r.headers.get("Content-Encoding") or "").lower()
    if enc == "gzip" and raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", "replace")


def _unesc(s):
    """详情页里的封面地址是 \\u002F 转义的，先还原"""
    try:
        return json.loads('"%s"' % s)
    except Exception:
        return s.replace("\\u002F", "/").replace("\\/", "/")


def detail_of(html, sid):
    """从详情页 SSR 里抠出一部剧。抠不到返回 None。

    页面是客户端渲染的，但服务端把 `seriesDetail` 整段塞进了 HTML，
    剧名/封面/集数/题材全在 —— 正是列表卡片要的四样。这是唯一能按 id
    取元数据的免签入口（2026-11-01 抽 5 个 id 试，5/5 命中）。
    """
    if '"seriesDetail"' not in html:
        return None
    name = re.search(r'"series_name":"((?:[^"\\]|\\.)*)"', html)
    if not name or not name.group(1).strip():
        return None
    cover = re.search(r'"series_cover":"((?:[^"\\]|\\.)*)"', html)
    ep = re.search(r'"episode_cnt":(\d+)', html)
    tags = re.search(r'"tags":\[([^\]]*)\]', html)
    return {
        "i": sid,
        "t": _unesc(name.group(1)).strip(),
        "c": _unesc(cover.group(1)) if cover else "",
        "e": int(ep.group(1)) if ep else 0,
        "g": re.findall(r'"([^"]+)"', tags.group(1)) if tags else [],
    }


def kind_model(db):
    """题材 -> (漫剧数, 总数)。用来反推一部剧是真人还是漫剧。

    详情页**没有** genre 字段（搜索接口里的 203/205 只有它给），但站点的
    题材词表本身就是按这两种内容分区长的：实测「战斗/种田/脑洞/宫廷/恋爱」
    100% 是漫剧，「家庭温情/商业联姻/读心术/古装传奇」100% 是真人。
    拿已抓到的样本做朴素贝叶斯，交叉验证准确率 99.5%。
    """
    stat = {}
    for kind, g in db.execute("SELECT k, g FROM item WHERE g != ''"):
        for t in g.split("\u0001"):
            if not t:
                continue
            a = stat.setdefault(t, [0, 0])
            a[0] += 1 if kind == "comic-drama" else 0
            a[1] += 1
    return {t: v for t, v in stat.items() if v[1] >= 8}


def kind_of(tags, model):
    """按题材投票。没见过的题材不投票，全都没把握时默认真人剧"""
    score = 0.0
    for t in tags:
        v = model.get(t)
        if not v:
            continue
        p = (v[0] + 1) / (v[1] + 2)          # 拉普拉斯平滑
        score += math.log(p / (1 - p))
    return "comic-drama" if score > 0 else "real-drama"


def _fetch_detail(sid):
    """纯网络操作，不碰数据库（SQLite 只能一个写者）"""
    try:
        return sid, detail_of(http_text("%s/detail?series_id=%s" % (SITE, sid)), sid), None
    except Exception as e:
        if _is_throttle(e):
            time.sleep(random.uniform(4, 9))
        return sid, None, e


def _ts(s):
    """sitemap 的 `<lastmod>`（`2026-11-11T14:15:09+08:00`）转 unix 秒。

    解析不了给 0 —— 0 表示「不知道」，既不会覆盖已记下的值，也不会被当成改动。
    """
    if not s:
        return 0
    try:
        return int(datetime.fromisoformat(s.strip()).timestamp())
    except Exception:
        return 0


# `{series_id}` 数字段。老格式 `/player/7612345678901234567`、新格式
# `/detail?series_id=7612345678901234567` 都能抠出来，不用分别写正则。
ID_IN_URL = re.compile(r"(\d{10,})")


def cmd_sitemap(args):
    """扫 sitemap，写 `sid` 表，并顺手做两件下架检测的前置活。

    ## 一、消失检测（方案 1 的第一道筛）

    全库六十四万部逐个探活要 60 小时，而 sitemap 全扫一遍只要 25 秒。
    本轮没出现的 id 记 `miss+1`，当作「疑似下架」候选，真伪交给 `vet` 终审。

    ## 二、改动检测（方案 2）

    `<lastmod>` 变了说明站方动过这部剧（改名 / 改集数 / 换封面 / 下架又上架），
    标 `stale=1` 交给 `fill` 重抓。这顺便把「库里的数据永远停在第一次见到的
    样子」那个老毛病修掉。

    ## 为什么必须认两种 URL 格式

    2026-11 起站点换过一版 sitemap：老分片是 `/player/<id>`，新分片是
    `/detail?series_id=<id>`。只认前者的话，第 1~14 片一条都读不到 —— 而那
    正是新剧所在的地方（实测新剧的 lastmod 全在新格式里）。
    """
    db = open_db()
    round_at = int(time.time())
    idx = http_text(SITE + "/sitemap.xml", timeout=60)
    shards = re.findall(r"<loc>([^<]+)</loc>", idx)
    if not shards:
        sys.exit("sitemap.xml 里没解析到分片，站点结构可能变了")
    log("sitemap 分片 %d 个" % len(shards))
    urls = 0
    bad = []
    for i, s in enumerate(shards, 1):
        try:
            x = http_text(s, timeout=120)
        except Exception as e:
            log("  分片 %d 失败：%s" % (i, e))
            bad.append(i)
            continue
        rows = []
        for m in re.finditer(r"<url><loc>([^<]+)</loc><lastmod>([^<]*)</lastmod></url>", x):
            mm = ID_IN_URL.search(m.group(1))
            if mm:
                rows.append((mm.group(1), _ts(m.group(2))))
        if not rows:
            # 分片拿到了但一条都解析不出来 = 结构又变了，当失败处理，别让它参与消失判定
            log("  分片 %d 解析出 0 条，跳过" % i)
            bad.append(i)
            continue
        # 按 id 排序再写：WAL 下随机主键更新会把 B-tree 页写得满盘跑，
        # 顺序写能省掉大半的页分裂。四万行排序本身只要几十毫秒。
        rows.sort()
        before = db.total_changes
        # **`stale` 必须排在 `lm` 前面**：SQLite 的 SET 是顺序求值的，
        # 放到后面读到的就是刚写进去的新值，永远相等，等于这个字段没用。
        #
        # 判定用 `excluded.lm > sid.lm`（**只认变新**），`lm` 也取最大值：
        # 同一部剧在 sitemap 里有两条 URL（`/player/` 和 `/detail?series_id=`），
        # 两条的 lastmod 往往不一样。如果用「不相等」判定，同一轮内两条会互相
        # 把对方标成改动，下一轮再互相标回来 —— 天天误报，永远收敛不了。
        db.executemany(
            "INSERT INTO sid (id, at, fail, seen, lm, miss, stale) "
            "VALUES (?, 0, 0, ?, ?, 0, 0) "
            "ON CONFLICT(id) DO UPDATE SET "
            "stale = CASE WHEN sid.lm > 0 AND excluded.lm > sid.lm "
            "              THEN 1 ELSE sid.stale END, "
            "seen = excluded.seen, "
            "lm = CASE WHEN excluded.lm > sid.lm THEN excluded.lm ELSE sid.lm END, "
            "miss = 0",
            [(sid, round_at, lm) for sid, lm in rows])
        db.commit()
        urls += len(rows)
        log("  分片 %2d/%d：%d 条 URL（库内变更 %d 行）"
            % (i, len(shards), len(rows), db.total_changes - before))
    n = db.execute("SELECT COUNT(*) FROM sid").fetchone()[0]
    stale = db.execute("SELECT COUNT(*) FROM sid WHERE stale = 1").fetchone()[0]
    log("id 宇宙：共 %d 个（本轮扫到 %d 条 URL）｜待重抓 %d 个" % (n, urls, stale))
    if bad:
        # 少抓了几片就做消失判定 = 把十几万个在架的剧一起标成疑似下架，
        # 下一轮 vet 会白跑几十小时、还会误伤。宁可这一轮不做。
        log("⚠ %d 片没抓到（第 %s 片），本轮跳过消失判定"
            % (len(bad), "、".join(str(x) for x in bad)))
        return
    db.execute("UPDATE sid SET miss = miss + 1 WHERE seen < ? AND miss >= 0", (round_at,))
    # 重新上架：上一轮已确认下架的，这一轮又出现在 sitemap 里了。
    # 只查 `gone = 1` 的那几万行，走 idx_item_gone，几十毫秒。
    db.execute(
        "UPDATE item SET gone = 0 WHERE gone = 1 "
        "AND EXISTS (SELECT 1 FROM sid s WHERE s.id = item.id AND s.seen = ?)",
        (round_at,))
    db.commit()
    lost = db.execute("SELECT COUNT(*) FROM sid WHERE miss = 1").fetchone()[0]
    back = db.execute("SELECT COUNT(*) FROM item WHERE gone = 1").fetchone()[0]
    log("消失候选：%d 个（连续 1 轮没出现，等 detail 终审）｜库里仍标失效 %d 部"
        % (lost, back))


def cmd_fill(args):
    """按 id 补齐长尾。每个 id 一次请求、约 96KB，实测 3 请求/秒。"""
    db = open_db()
    if db.execute("SELECT COUNT(*) FROM sid").fetchone()[0] == 0:
        log("sid 表还是空的，先扫 sitemap")
        cmd_sitemap(args)

    model = kind_model(db)
    log("题材判别模型：%d 个题材（已抓 %d 部样本）"
        % (len(model), db.execute("SELECT COUNT(*) FROM item").fetchone()[0]))

    workers = max(1, min(16, args.workers))
    deadline = time.time() + args.minutes * 60 if args.minutes else 0
    budget = args.ids or 0
    done = miss = err_n = 0
    req = 0
    started = time.time()
    pool = ThreadPoolExecutor(max_workers=workers)
    inflight = {}

    def dispatch():
        while len(inflight) < workers * 3:
            # 两条队列合一：**改过动的优先**（stale=1），其次是从没抓过的（at=0）。
            # 改过动的量小（实测每天三五千部），但它决定「库里的剧名/集数跟不跟得上」，
            # 所以排在补长尾前面。
            row = db.execute(
                "SELECT id FROM sid WHERE (at = 0 OR stale = 1) AND COALESCE(fail, 0) < 5 "
                "ORDER BY CASE WHEN stale = 1 THEN 0 ELSE 1 END, "
                "COALESCE(fail, 0), rowid LIMIT 1").fetchone()
            if row is None:
                return False
            sid = row[0]
            db.execute("UPDATE sid SET at = -1 WHERE id = ?", (sid,))  # 占坑，防重复派发
            db.commit()
            inflight[pool.submit(_fetch_detail, sid)] = sid
        return True

    log("开始按 id 补齐：%d 路并发%s" % (workers, "，预算 %d 分钟" % args.minutes if args.minutes else ""))
    # 上一轮被强杀时留在 -1 的占坑要放回去，否则那些 id 永远不补
    db.execute("UPDATE sid SET at = 0 WHERE at = -1")
    db.commit()
    dispatch()
    while inflight:
        got, _ = wait(list(inflight), return_when=FIRST_COMPLETED)
        for fut in got:
            sid = inflight.pop(fut)
            _sid, item, e = fut.result()
            req += 1
            if e is not None:
                err_n += 1
                db.execute("UPDATE sid SET at = 0, fail = fail + 1 WHERE id = ?", (sid,))
            elif item is None:
                miss += 1                     # 页面在但抠不到 → 下架/改版，别再来
                db.execute("UPDATE sid SET at = -2, miss = -1 WHERE id = ?", (sid,))
                # 抓不到就别再让它在 App 里被搜出来：detail 页没有数据 = 点进去也放不了
                db.execute("UPDATE item SET gone = 1 WHERE id = ?", (sid,))
            else:
                done += 1
                k = kind_of(item["g"], model)
                # `gone = 0` 不能少：这部剧可能是「下架又上架」，或者上回探活误判过
                db.execute(
                    "INSERT INTO item (id, t, c, e, g, k, gone) VALUES (?,?,?,?,?,?,0) "
                    "ON CONFLICT(id) DO UPDATE SET t=excluded.t, c=excluded.c, "
                    "e=excluded.e, k=excluded.k, gone=0, "
                    "g=CASE WHEN excluded.g != '' THEN excluded.g ELSE item.g END",
                    (item["i"], item["t"], item["c"], item["e"],
                     "\u0001".join(item["g"]), k))
                db.execute("UPDATE sid SET at = ?, fail = 0, stale = 0, miss = 0 WHERE id = ?",
                           (int(time.time()), sid))
            db.commit()
            if (done + miss + err_n) % 200 == 0:
                rate = req / max(1e-6, time.time() - started)
                left = db.execute("SELECT COUNT(*) FROM sid WHERE at = 0").fetchone()[0]
                log("  已补 %d 部 / 查无 %d / 失败 %d ｜ %.2f 请求/秒 ｜ 还剩 %d 个 id 待补"
                    % (done, miss, err_n, rate, left))
        dispatch()
        if budget and done + miss + err_n >= budget:
            log("已到本轮预算 %d 个" % budget)
            break
        if deadline and time.time() > deadline:
            log("已到本轮时间预算 %d 分钟" % args.minutes)
            break

    pending = db.execute("SELECT COUNT(*) FROM sid WHERE at = 0").fetchone()[0]
    stale_left = db.execute("SELECT COUNT(*) FROM sid WHERE stale = 1").fetchone()[0]
    log("补齐结束：新增 %d 部，查无 %d，失败 %d，%d 请求，用时 %.0f 分钟，"
        "还剩 %d 个没抓过 / %d 个改动待重抓"
        % (done, miss, err_n, req, (time.time() - started) / 60, pending, stale_left))


def _is_gone(e):
    """详情页 404 = 这个 id 真的没了。

    必须和 `_is_throttle`（429/503）分开：前者是「下架了」，后者是「别打了，
    等会儿再来」。混成一个分支，一次限流就能把几万部在架的剧标成下架。
    """
    return getattr(e, "code", None) == 404


def _probe_ids(db, ids, workers, label):
    """并发请求一批 id 的详情页，返回 (ok, gone, err)。

    纯网络 → 主线程写库，和 `cmd_fill` 同一套规矩（SQLite 只允许一个写者）。
    """
    ok = gone = err = 0
    pool = ThreadPoolExecutor(max_workers=workers)
    now = int(time.time())
    try:
        for sid, item, e in pool.map(_fetch_detail, ids):
            if item is not None:
                ok += 1
                db.execute("UPDATE item SET gone = 0 WHERE id = ?", (sid,))
                db.execute("UPDATE sid SET miss = 0, seen = ? WHERE id = ?", (now, sid))
                # orphan（库里但 sitemap 从没收录过）补一行，否则下一轮又被当成候选。
                # at=-9 表示「不在 sitemap、但确认在架」，fill 不会去补它。
                db.execute("INSERT OR IGNORE INTO sid "
                           "(id, at, fail, seen, lm, miss, stale) "
                           "VALUES (?, -9, 0, ?, 0, 0, 0)", (sid, now))
            elif e is None or _is_gone(e):
                # 页面能打开但没有 seriesDetail，或者直接 404 —— 都算真下架
                gone += 1
                db.execute("UPDATE item SET gone = 1 WHERE id = ?", (sid,))
                db.execute("UPDATE sid SET miss = -1, at = -2 WHERE id = ?", (sid,))
            else:
                # 超时 / 5xx / 限流：什么都没证明，保持原状等下一轮
                err += 1
        db.commit()
    finally:
        pool.shutdown(wait=True)
    if gone:
        log("  %s 判定下架 %d 部" % (label, gone))
    return ok, gone, err


def cmd_vet(args):
    """复核「疑似下架」的候选 —— 这是唯一能定论的一道。

    ## 为什么要终审，不能只看 sitemap

    实测 sitemap 只收录约 97.5% 的在架剧：从「不在清单里」的抽 12 个逐个请求，
    11 个还能正常打开。所以「没出现」只能算可疑；直接标下架等于一天误杀几千部
    在架的剧，用户搜不到比点进去放不了更难受。

    ## 队列

    - 默认：`sid.miss >= 1`（在 sitemap 里消失了），miss 大的优先 —— 连续缺席
      好几轮的基本能确定，先处理它们能让候选池迅速清空。
    - `--orphans`：库里存在、但 sitemap 从没收录过的。这批是首轮历史欠账
      （约一万多部），要单独跑一次把它们一次问清楚。
    """
    db = open_db()
    if db.execute("SELECT COUNT(*) FROM sid").fetchone()[0] == 0:
        sys.exit("sid 表是空的，先跑 sitemap")

    # 本轮已经问过的 id。**必须有**：请求失败（超时/503）时数据库里什么都没改，
    # 不记这一笔的话 `take()` 会一直把同一部剧捞回来，原地死循环到预算耗光。
    tried = set()

    def take(n):
        room = n + len(tried)
        if args.orphans:
            rows = db.execute(
                "SELECT i.id FROM item i LEFT JOIN sid s ON s.id = i.id "
                "WHERE s.id IS NULL AND i.gone = 0 ORDER BY i.rowid LIMIT ?", (room,))
        else:
            rows = db.execute(
                "SELECT id FROM sid WHERE miss >= 1 AND at != -2 "
                "ORDER BY miss DESC, rowid LIMIT ?", (room,))
        return [r[0] for r in rows if r[0] not in tried][:n]

    queue = "历史欠账（库里但 sitemap 从没收录）" if args.orphans else "sitemap 消失候选"
    if args.orphans:
        left = db.execute(
            "SELECT COUNT(*) FROM item i LEFT JOIN sid s ON s.id = i.id "
            "WHERE s.id IS NULL AND i.gone = 0").fetchone()[0]
    else:
        left = db.execute(
            "SELECT COUNT(*) FROM sid WHERE miss >= 1 AND at != -2").fetchone()[0]
    log("复核开始：%s，待办 %d 个" % (queue, left))
    if not left:
        log("没有待复核的，收工")
        return

    workers = max(1, min(16, args.workers))
    deadline = time.time() + args.minutes * 60 if args.minutes else 0
    budget = args.ids or 0
    ok = gone = err = 0
    started = time.time()
    while True:
        batch = take(workers * 3)
        if not batch:
            break
        tried.update(batch)
        a, b, c = _probe_ids(db, batch, workers, queue)
        ok, gone, err = ok + a, gone + b, err + c
        done = ok + gone + err
        if done % 1000 < workers * 3:
            log("  已复核 %d ｜在架 %d / 下架 %d / 失败 %d ｜%.1f 分钟"
                % (done, ok, gone, err, (time.time() - started) / 60))
        if budget and done >= budget:
            log("已到本轮预算 %d 个" % budget)
            break
        if deadline and time.time() > deadline:
            log("已到本轮时间预算 %d 分钟" % args.minutes)
            break
    log("复核结束：在架 %d、下架 %d、失败 %d，用时 %.1f 分钟"
        % (ok, gone, err, (time.time() - started) / 60))


def cmd_probe(args):
    """全库轮转探活 —— 低优先级兜底（方案 3）。

    兜的是 sitemap 覆盖不到的那一小撮（实测约 2.5%）。这些剧永远不会出现在
    sitemap 里，也就不会被方案 1 的差分发现；只能老老实实逐个问一次。

    排序看 `probe`（最后一次探活时间），最久没问的优先，所以天然是轮转：
    六十四万部按每天 2000 个算约 10 个月轮一遍。慢是慢，但它排在
    fill / vet 后面只吃剩下的预算，而且真正常用的剧早就被搜索和下载验证过了。
    """
    db = open_db()
    total = db.execute("SELECT COUNT(*) FROM item WHERE gone = 0").fetchone()[0]
    workers = max(1, min(16, args.workers))
    budget = args.ids or 2000
    log("轮转探活开始：库里在架 %d 部，本轮最多 %d 个（约 %.0f 分钟）"
        % (total, budget, budget / 3.0 / 60))
    started = time.time()
    done = ok = gone = err = 0
    while done < budget:
        n = min(workers * 3, budget - done)
        batch = [r[0] for r in db.execute(
            "SELECT id FROM item WHERE gone = 0 ORDER BY probe, rowid LIMIT ?", (n,))]
        if not batch:
            log("没有可探的了")
            break
        a, b, c = _probe_ids(db, batch, workers, "轮转探活")
        ok, gone, err = ok + a, gone + b, err + c
        done += len(batch)
        # 探过就记账，不管结果如何 —— 出错的那批 10 个月后再来，
        # 不能让一条永远 503 的剧把轮转卡在原地
        now = int(time.time())
        for sid in batch:
            db.execute("UPDATE item SET probe = ? WHERE id = ?", (now, sid))
        db.commit()
    log("轮转探活结束：%d 个 ｜在架 %d / 下架 %d / 失败 %d ｜%.1f 分钟"
        % (done, ok, gone, err, (time.time() - started) / 60))


def _try_add(db, table, col, decl):
    """幂等加列。

    抓取库是每轮从 `state/` 快照恢复的，**不是重建**，所以新加的列必须显式
    ALTER 补上；`CREATE TABLE IF NOT EXISTS` 对已存在的表一个字都不会动。
    """
    cols = {r[1] for r in db.execute("PRAGMA table_info(%s)" % table)}
    if col not in cols:
        db.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, col, decl))


def open_db():
    os.makedirs(WORK, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS item ("
               "id TEXT PRIMARY KEY, t TEXT, c TEXT, e INTEGER, g TEXT, k TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS word (w TEXT PRIMARY KEY, at INTEGER)")
    # fail：连续失败次数。被限流时要能把词退回待跑（at=0），
    # 但得有个上限，否则一条永远 503 的词会把这轮抓取卡死。
    try:
        db.execute("ALTER TABLE word ADD COLUMN fail INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # sid：sitemap 给出的全站 id 宇宙
    #   at=0   待补 ｜ at>0 已补 ｜ at=-1 派发中 ｜ at=-2 详情页查无（下架）
    #   at=-9  不在 sitemap、但 detail 页确认在架（兜底探活补进来的）
    db.execute("CREATE TABLE IF NOT EXISTS sid ("
               "id TEXT PRIMARY KEY, at INTEGER DEFAULT 0, fail INTEGER DEFAULT 0)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_item_k ON item (k)")

    # ---- 下架检测（2026-11-13）----
    # 站方每天在增删改，库里攒的六十四万部没法逐个探活（3 请求/秒要 60 小时）。
    # 抓手是 sitemap：全站 30 片扫一遍只要 25 秒，且每条 URL 带 `<lastmod>`。
    #   sid.seen   最后一次出现在 sitemap 的时间（0 = 从没出现过）
    #   sid.lm     上一轮记下的 lastmod（unix 秒），变了说明站方改过这部剧
    #   sid.miss   连续几轮没出现在 sitemap；**-1 = 已确认下架**
    #   sid.stale  1 = lastmod 变了、还没重抓
    #   item.probe 最后一次探活时间（方案 3 的全库轮转靠它排序）
    #   item.gone  1 = 已确认下架（detail 页 404 或无 seriesDetail）
    for col, decl in (("seen", "INTEGER DEFAULT 0"), ("lm", "INTEGER DEFAULT 0"),
                      ("miss", "INTEGER DEFAULT 0"), ("stale", "INTEGER DEFAULT 0")):
        _try_add(db, "sid", col, decl)
    for col, decl in (("probe", "INTEGER DEFAULT 0"), ("gone", "INTEGER DEFAULT 0")):
        _try_add(db, "item", col, decl)
    db.execute("CREATE INDEX IF NOT EXISTS idx_item_probe ON item (probe)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_item_gone ON item (gone)")
    return db


def mine_words(titles, used, limit=60):
    """从剧名里挖候选词（二字 + 三字）。这是「一个词换一池」的滚雪球核心，
    词表质量比抓取逻辑更重要。

    **只挖相邻二字会很快见底**：实测 1.1 万部剧名就把二字词池子挖干（421 个词），
    之后只能挖出「成反」「师第」这种跨词碎片，整轮抓取提前结束在 1.1 万部。
    三字更「具体」，命中率反而更高 —— 实测「白月光」2.3 部/请求、「爸」3.2 部/请求，
    而「女」「婚」「妻」这种泛词搜出来的全是已抓过的头部剧，翻到底也是 0 新增。

    排序按频次降序：频次高说明多部剧名都含它，一次搜索能带回一池。
    """
    from collections import Counter
    cnt = Counter()
    for t in titles:
        clean = "".join(ch for ch in t if "\u4e00" <= ch <= "\u9fff")
        for n in (2, 3):
            for i in range(len(clean) - n + 1):
                w = clean[i:i + n]
                # 首尾是虚词的多半是跨词碎片，直接丢
                if w[0] in DROP_CHARS or w[-1] in DROP_CHARS:
                    continue
                cnt[w] += 1
    out = []
    for w, n in cnt.most_common(2000):
        if n < 2 or w in used:
            continue
        if w.startswith(DROP_PREFIX) or w.endswith(DROP_SUFFIX):
            continue
        out.append(w)
        if len(out) >= limit:
            break
    return out


GERM = ["总裁", "重生", "穿越", "闪婚", "退婚", "逆袭", "复仇", "战神", "龙王", "神医",
        "王妃", "皇后", "千金", "豪门", "甜宠", "虐恋", "悬疑", "追妻", "万人迷", "年代",
        "家庭", "职场", "复仇", "萌宝", "马甲", "都市", "古装", "玄幻", "权谋", "神豪"]


def _is_throttle(e):
    """站点限流（503/429）和「这一页真的没数据」要分开对待 —— 前者退避重试，后者跳过。

    2026-11-01 实测：连跑 19 分钟、约 3000 个请求之后，接口开始整片回 503。
    退避之前每多打一次都是白打，还会把词表烧掉。
    """
    if getattr(e, "code", None) in (429, 503):
        return True
    s = str(e)
    return "429" in s or "503" in s


def _crawl_word(word, gap, slow):
    """抓一个词的所有页。

    纯网络操作、不碰数据库 —— 结果交回主线程统一写库，SQLite 只允许一个写者，
    多线程各自开连接写会直接 `database is locked`。
    """
    items, req, last, err = [], 0, 1, None
    try:
        for page in range(1, MAX_PAGES + 1):
            got, pages = search(word, page)
            req += 1
            last = page
            items.extend(got)
            if page >= pages or not got:
                break
            if slow and gap > 0:
                time.sleep(gap)
    except Exception as e:
        err = e
        # 被限流就让这个 worker 先歇一会儿，别立刻把下一轮也打出去
        if _is_throttle(e):
            time.sleep(random.uniform(4, 9))
    return items, req, last, err


def _eta(n, target, started):
    if n <= 0 or target <= 0:
        return ""
    rate = n / max(1e-6, time.time() - started)
    left = max(0, target - n) / max(1e-6, rate)
    return "｜%.0f 部/分｜预计还要 %.0f 分钟" % (rate * 60, left / 60)


def cmd_crawl(args):
    db = open_db()
    # 种子词：先跑一遍常见题材，后面的词从已抓到的剧名里挖
    for w in GERM:
        db.execute("INSERT OR IGNORE INTO word (w, at) VALUES (?, 0)", (w,))
    # 上次被强杀（关窗口 / Ctrl+C / 断电）时留下的占坑要放回去，否则那些词永远不跑
    db.execute("UPDATE word SET at = 0 WHERE at = -1")
    db.commit()

    workers = max(1, min(8, args.workers))
    target = args.target or 0
    started = time.time()
    # 关键词阶段**必须有墙钟上限**：它排在整条流水线最前面，而 fill 拿的是固定
    # 分钟预算。2026-11-02 那轮关键词跑了 52 分钟还没完（搜索接口对机房出口限流，
    # 每个词都在退避重试），差点把 350 分钟的 job 上限吃光 —— 一旦被超时杀掉，
    # 因为发布和 state-keep 都在最后一步，整轮六小时等于白跑。
    deadline = started + args.minutes * 60 if args.minutes else 0
    if deadline:
        log("关键词阶段预算 %d 分钟" % args.minutes)
    total_req = 0
    # 每轮 COUNT(*) 在十几万行时还要几十毫秒，自己维护省下来
    db_n = db.execute("SELECT COUNT(*) FROM item").fetchone()[0]
    start_n = db_n
    stop = False
    fail_streak = 0
    pool = ThreadPoolExecutor(max_workers=workers)
    inflight = {}

    log("开始抓取：%d 路并发，库里已有 %d 部%s"
        % (workers, db_n, "，目标 %d 部" % target if target else ""))

    def dispatch():
        """把待跑的词派下去，在途任务保持 2 倍线程数（网络等待期间 CPU 不空转）"""
        nonlocal stop
        if deadline and time.time() >= deadline:
            stop = True
            return
        while len(inflight) < workers * 2:
            row = db.execute("SELECT w FROM word WHERE at = 0 AND COALESCE(fail, 0) < 5 "
                             "ORDER BY COALESCE(fail, 0), rowid LIMIT 1").fetchone()
            if row is None:
                titles = [r[0] for r in db.execute(
                    "SELECT t FROM item ORDER BY RANDOM() LIMIT 20000")]
                used = {r[0] for r in db.execute("SELECT w FROM word")}
                fresh = mine_words(titles, used, 250)
                if not fresh:
                    log("没有新词可挖了（已跑 %d 个词），抓取结束" % len(used))
                    stop = True
                    return
                for w in fresh:
                    db.execute("INSERT OR IGNORE INTO word (w, at) VALUES (?, 0)", (w,))
                db.commit()
                log("挖到新词 %d 个（词表共 %d），例如 %s"
                    % (len(fresh), len(used) + len(fresh), "、".join(fresh[:6])))
                continue
            w = row[0]
            # 占坑：先标 -1 再派发，否则下一轮 dispatch 会把同一个词再派一次
            db.execute("UPDATE word SET at = -1 WHERE w = ?", (w,))
            db.commit()
            inflight[pool.submit(_crawl_word, w, args.gap, args.slow)] = w

    try:
        dispatch()
        while inflight:
            done, _ = wait(list(inflight), return_when=FIRST_COMPLETED)
            for fut in done:
                word = inflight.pop(fut)
                items, req, last, err = fut.result()
                total_req += req
                added = 0
                for it in items:
                    before = db.total_changes
                    db.execute(
                        "INSERT OR IGNORE INTO item (id, t, c, e, g, k) VALUES (?,?,?,?,?,?)",
                        (it["i"], it["t"], it["c"], it["e"], "\u0001".join(it["g"]), it["k"]))
                    if db.total_changes > before:
                        added += 1
                db.commit()
                db_n += added
                if err is None:
                    db.execute("UPDATE word SET at = ?, fail = 0 WHERE w = ?",
                               (int(time.time()), word))
                    fail_streak = 0
                else:
                    # 失败**不能**当成功记。原来这里无条件写 at=now，503 一来
                    # 上千个词被标成「跑过」就永远不再跑 —— 2026-11-01 那轮
                    # 2138 个词里有 1270 个是这么废掉的，库卡在 1.8 万部上不去。
                    db.execute("UPDATE word SET at = 0, "
                               "fail = COALESCE(fail, 0) + 1 WHERE w = ?", (word,))
                    fail_streak += 1
                    log("  词「%s」第 %d 页失败：%s" % (word, last, err))
                db.commit()
                log("词「%s」+%d 部 / 库 %d 部 / %d 请求%s"
                    % (word, added, db_n, total_req, _eta(db_n, target, started)))

            # 连续失败到一定数量 = 已经被限流，继续打只会把剩下的词一起烧掉
            if fail_streak >= 12:
                log("连续 %d 个词失败（多半被限流），暂停 60 秒再继续" % fail_streak)
                time.sleep(60)
                fail_streak = 0

            if target and db_n >= target:
                log("已到目标 %d 部，停止" % target)
                break
            if args.add and db_n - start_n >= args.add:
                log("本轮已新增 %d 部，停止" % (db_n - start_n))
                break
            if deadline and time.time() >= deadline:
                log("关键词阶段已到时间预算 %d 分钟，收工转入按 id 补齐" % args.minutes)
                break
            if args.words and total_req >= args.words:
                log("已发 %d 个请求，停止" % total_req)
                break
            if not stop:
                dispatch()
    except KeyboardInterrupt:
        log("收到中断，等正在跑的 %d 个词收尾（再按一次 Ctrl+C 强退）…" % len(inflight))
    finally:
        pool.shutdown(wait=True)
        # 没跑完的词退回待跑状态，下次接着来
        db.execute("UPDATE word SET at = 0 WHERE at = -1")
        db.commit()
        n = db.execute("SELECT COUNT(*) FROM item").fetchone()[0]
        log("本轮结束：库共 %d 部（本轮新增 %d 部），%d 请求，用时 %.0f 分钟"
            % (n, n - start_n, total_req, (time.time() - started) / 60))



# ----------------------------------------------------------------------
# 打包
# ----------------------------------------------------------------------

def cmd_build(args):
    db = open_db()
    base = (args.base or os.environ.get("PACK_PUBLIC_BASE")
            or os.environ.get("R2_PUBLIC_BASE") or "")
    if not base:
        sys.exit("缺少公开访问前缀：加 --base https://pack.example.com，"
                 "或者在 tools/.env.pack 里填 R2_PUBLIC_BASE")
    os.makedirs(OUT_DIR, exist_ok=True)
    # 清掉上一轮的分片：Pages 部署是「全量快照」，残留的旧文件会被一起传上去，
    # 白白占空间，清单里又没有它们。state/ 不能删，那是下一轮要用的抓取库。
    for name in os.listdir(OUT_DIR):
        if name.endswith(".jsonl.gz") or name.endswith(".txt.gz") or name == "catalog.json":
            os.remove(os.path.join(OUT_DIR, name))
    manifest = {"version": int(time.time()), "built": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "total": 0, "shards": [], "gone": None}
    total = 0
    for kind in ("real-drama", "comic-drama"):
        rows = db.execute("SELECT id, t, c, e, g FROM item WHERE k = ? ORDER BY id", (kind,))
        idx, buf = 0, []
        for r in rows:
            buf.append({"i": r[0], "t": r[1], "c": r[2], "e": r[3],
                        "g": [x for x in (r[4] or "").split("\u0001") if x]})
            if len(buf) >= SHARD_SIZE:
                manifest["shards"].append(write_shard(kind, idx, buf, base))
                total += len(buf)
                idx, buf = idx + 1, []
        if buf:
            manifest["shards"].append(write_shard(kind, idx, buf, base))
            total += len(buf)
    manifest["total"] = total
    # 失效名单单独一片。**不从主分片里剔除**：分片是按 id 升序切的，中间抽掉
    # 几条会让每一片的边界全变、12 个指纹全变，所有老用户一夜之间要重下 37MB。
    # 主包保持全量，失效只作为一份小名单下发，App 端搜索时过滤掉。
    gone = [r[0] for r in db.execute("SELECT id FROM item WHERE gone = 1 ORDER BY id")]
    if gone:
        manifest["gone"] = write_gone(gone, base)
    path = os.path.join(OUT_DIR, "catalog.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)
    size = sum(s["bytes"] for s in manifest["shards"])
    log("打包完成：%d 部，%d 片，压缩后 %.1f MB（其中失效 %d 部）-> %s"
        % (total, len(manifest["shards"]), size / 1024 / 1024, len(gone), path))

    # 离 50MB 上限还有多远。`::warning::` / `::error::` 是 GitHub Actions 的
    # 注解语法，会在这轮 run 的摘要里单独标出来，不至于埋进几百行日志。
    n_shards = len(manifest["shards"])
    if n_shards >= SPLIT_WARN_SHARDS or size >= SPLIT_WARN_BYTES:
        log("::error::分片已到 %d 个 / %.1f MB，逼近 jsDelivr 的 50MB 单仓库上限"
            % (n_shards, size / 1024 / 1024))
        log("  该拆仓库了：清单里每个分片的 url 是绝对地址，拆出第二个仓库"
            "（比如 media-vault-pack-b）客户端不用改一行代码。")
        log("  改法见 REFERENCE.md「数据包分发」——同时把上一代分片也留着，"
            "免得 12 小时的分支缓存把旧清单指向已被删的文件。")
    elif n_shards >= SPLIT_WARN_SHARDS - 2 or size >= SPLIT_WARN_BYTES * 0.88:
        log("::warning::分片 %d 个 / %.1f MB，距 50MB 上限约剩 %d 片"
            % (n_shards, size / 1024 / 1024, 17 - n_shards))


def write_gone(ids, base):
    """失效名单：一行一个 series_id。

    为什么用纯文本不用 JSON：App 端是流式逐行读进一张 `gone` 表，一行一个
    id 最省事，gzip 后一万条也就几十 KB，比主包小两个数量级。
    """
    raw = ("\n".join(ids) + "\n").encode("utf-8")
    fp = hashlib.sha256(raw).hexdigest()[:8]
    name = "gone-%s.txt.gz" % fp
    path = os.path.join(OUT_DIR, name)
    with gzip.open(path, "wb", compresslevel=9) as f:
        f.write(raw)
    kb = os.path.getsize(path) / 1024.0
    log("  %s：%d 条失效（%.1f KB）" % (name, len(ids), kb))
    return {"id": name[:-7],  # 去掉 .txt.gz，App 端拿它记「这片装过了」
            "url": base.rstrip("/") + "/" + name,
            "bytes": os.path.getsize(path), "count": len(ids)}


def write_shard(kind, idx, rows, base):
    # id 带上内容指纹：App 端按 id 记「这一片装过了」，不带指纹的话补抓以后
    # 重新 build 出来的分片内容变了、id 没变，老用户会永远收不到新数据
    data = "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n" for r in rows)
    raw = data.encode("utf-8")
    fp = hashlib.sha256(data.encode("utf-8")).hexdigest()[:8]
    sid = "%s-%02d-%s" % (kind, idx, fp)
    # 文件名也带指纹：走 CDN 时同名文件内容变了会被缓存住，换个名字才能立刻生效
    name = sid + ".jsonl.gz"
    with gzip.open(os.path.join(OUT_DIR, name), "wb", compresslevel=9) as f:
        f.write(raw)
    path = os.path.join(OUT_DIR, name)
    log("  %s：%d 部，%.2f MB（原始 %.1f MB）"
        % (name, len(rows), os.path.getsize(path) / 1024 / 1024, len(raw) / 1024 / 1024))
    return {"id": sid, "kind": kind, "url": base.rstrip("/") + "/" + name,
            "bytes": os.path.getsize(path), "count": len(rows)}


# ----------------------------------------------------------------------
# 上传（R2 走 S3 兼容接口，SigV4 手签，不额外装 boto3）
# ----------------------------------------------------------------------

def _sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def r2_put(path, body, ctype="application/octet-stream"):
    account = os.environ["R2_ACCOUNT_ID"]
    bucket = os.environ["R2_BUCKET"]
    ak = os.environ["R2_ACCESS_KEY"]
    sk = os.environ["R2_SECRET_KEY"]
    host = "%s.r2.cloudflarestorage.com" % account
    url = "https://%s/%s/%s" % (host, bucket, urllib.parse.quote(path))
    now = datetime.now(timezone.utc)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amzdate}
    signed = ";".join(sorted(headers))
    canon_headers = "".join("%s:%s\n" % (k, headers[k]) for k in sorted(headers))
    canon = "PUT\n/%s/%s\n\n%s\n%s\n%s" % (
        bucket, urllib.parse.quote(path), canon_headers, signed, payload_hash)
    scope = "%s/%s/s3/aws4_request" % (datestamp, "auto")
    to_sign = "AWS4-HMAC-SHA256\n%s\n%s\n%s" % (
        amzdate, scope, hashlib.sha256(canon.encode()).hexdigest())
    k = _sign(("AWS4" + sk).encode("utf-8"), datestamp)
    k = _sign(k, "auto")
    k = _sign(k, "s3")
    k = _sign(k, "aws4_request")
    sig = hmac.new(k, to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    auth = ("AWS4-HMAC-SHA256 Credential=%s/%s,SignedHeaders=%s,Signature=%s"
            % (ak, scope, signed, sig))
    req = urllib.request.Request(url, data=body, method="PUT", headers={
        "Host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amzdate,
        "Authorization": auth,
        "Content-Type": ctype,
    })
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.status


def cmd_upload(args):
    files = sorted(os.listdir(OUT_DIR))
    for name in files:
        if not (name.endswith(".gz") or name == "catalog.json"):
            continue
        ctype = "application/json" if name.endswith(".json") else "application/gzip"
        with open(os.path.join(OUT_DIR, name), "rb") as f:
            body = f.read()
        code = r2_put(name, body, ctype)
        log("  %s -> %s（%d）" % (name, code, len(body)))
    log("上传完成。记得清单地址就是 <公开域名>/catalog.json")


def cmd_snapshot(args):
    """把抓取库 checkpoint 后压进 out/state/，供下一轮（Actions）恢复。

    WAL 模式下直接拷 catalog.db 会丢掉还在 -wal 里的那部分，所以先 checkpoint。
    """
    db = open_db()
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    db.close()
    dst = os.path.join(OUT_DIR, "state")
    os.makedirs(dst, exist_ok=True)
    out = os.path.join(dst, "catalog.db.gz")
    with open(DB_PATH, "rb") as f, gzip.open(out, "wb", compresslevel=9) as g:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            g.write(chunk)
    mb = os.path.getsize(out) / 1024 / 1024
    log("抓取库快照：%.1f MB -> %s" % (mb, out))
    # 快照只进 `state-keep` 分支（main 里 50MB 上限，放不下）。GitHub 单文件硬限 100MB，
    # 所以真正的红线在这里，不是以前那个 Pages 的 25MB。
    if mb > 95:
        log("  ⚠ 超过 95MB，接近 GitHub 单文件 100MB 硬限，该给快照切片了")


def main():
    ap = argparse.ArgumentParser(description="剧库数据包：抓取 / 打包 / 上传")
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("crawl", help="抓取全站剧名（断点续传）")
    c.add_argument("--words", type=int, default=0, help="最多发多少个请求，0 = 不限")
    c.add_argument("--minutes", type=int, default=0, help="本轮最多跑多少分钟，0 = 不限")
    c.add_argument("--target", type=int, default=0, help="抓够多少部就停，0 = 抓干净")
    c.add_argument("--add", type=int, default=0, help="本轮最多新增多少部，0 = 不限")
    c.add_argument("--workers", type=int, default=4, help="并发词数，1~8（默认 4）")
    c.add_argument("--gap", type=float, default=0.15, help="每页间隔秒数")
    c.add_argument("--slow", action="store_true", help="按 --gap 限速")

    b = sub.add_parser("build", help="切分片 + 生成 catalog.json")
    b.add_argument("--base", default=None,
                   help="分片的公开访问前缀；不给就读 .env.pack 里的 R2_PUBLIC_BASE")

    sub.add_parser("sitemap", help="扫 sitemap，把全站 series_id 灌进 sid 表")

    f = sub.add_parser("fill", help="按 id 补齐长尾（sitemap 里的每个 id 抓一次详情页）")
    f.add_argument("--ids", type=int, default=0, help="本轮最多补多少部，0 = 不限")
    f.add_argument("--minutes", type=int, default=0, help="本轮最多跑多少分钟，0 = 不限")
    f.add_argument("--workers", type=int, default=6, help="并发数，1~16（默认 6）")

    v = sub.add_parser("vet", help="复核疑似下架的候选（detail 页 404 才算数）")
    v.add_argument("--ids", type=int, default=0, help="本轮最多复核多少个，0 = 不限")
    v.add_argument("--minutes", type=int, default=0, help="本轮最多跑多少分钟，0 = 不限")
    v.add_argument("--workers", type=int, default=6, help="并发数，1~16（默认 6）")
    v.add_argument("--orphans", action="store_true",
                   help="改跑「库里存在但 sitemap 从没收录」的历史欠账")

    p = sub.add_parser("probe", help="全库轮转探活（低优先级兜底）")
    p.add_argument("--ids", type=int, default=2000, help="本轮探多少个（默认 2000）")
    p.add_argument("--workers", type=int, default=6, help="并发数，1~16（默认 6）")

    sub.add_parser("upload", help="传到 R2（需 R2_* 环境变量）")
    sub.add_parser("snapshot", help="把抓取库压进 out/state/（Actions 跨轮恢复用）")

    a = sub.add_parser("all", help="打包 + 上传")
    a.add_argument("--base", default=None, help="同上，不给就读 .env.pack")

    args = ap.parse_args()
    load_env()
    if args.cmd == "crawl":
        cmd_crawl(args)
    elif args.cmd == "sitemap":
        cmd_sitemap(args)
    elif args.cmd == "fill":
        cmd_fill(args)
    elif args.cmd == "vet":
        cmd_vet(args)
    elif args.cmd == "probe":
        cmd_probe(args)
    elif args.cmd == "build":
        cmd_build(args)
    elif args.cmd == "upload":
        cmd_upload(args)
    elif args.cmd == "snapshot":
        cmd_snapshot(args)
    else:
        cmd_build(args)
        cmd_upload(args)


if __name__ == "__main__":
    main()
