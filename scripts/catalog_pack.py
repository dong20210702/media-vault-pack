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


def cmd_sitemap(args):
    """把 sitemap 里的全站 id 灌进 sid 表。29 个请求、约 143MB，20 秒。"""
    db = open_db()
    idx = http_text(SITE + "/sitemap.xml", timeout=60)
    shards = re.findall(r"<loc>([^<]+)</loc>", idx)
    if not shards:
        sys.exit("sitemap.xml 里没解析到分片，站点结构可能变了")
    log("sitemap 分片 %d 个" % len(shards))
    added = 0
    for i, s in enumerate(shards, 1):
        try:
            x = http_text(s, timeout=120)
        except Exception as e:
            log("  分片 %d 失败：%s" % (i, e))
            continue
        ids = set(re.findall(r"/player/(\d{10,})", x))
        before = db.total_changes
        for sid in ids:
            db.execute("INSERT OR IGNORE INTO sid (id, at) VALUES (?, 0)", (sid,))
        db.commit()
        d = db.total_changes - before
        added += d
        log("  分片 %2d/%d：%d 个 id，新入库 %d" % (i, len(shards), len(ids), d))
    n = db.execute("SELECT COUNT(*) FROM sid").fetchone()[0]
    log("id 宇宙：共 %d 个（本轮新增 %d）" % (n, added))


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
            row = db.execute("SELECT id FROM sid WHERE at = 0 AND fail < 5 "
                             "ORDER BY fail, rowid LIMIT 1").fetchone()
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
                db.execute("UPDATE sid SET at = -2 WHERE id = ?", (sid,))
            else:
                done += 1
                k = kind_of(item["g"], model)
                db.execute(
                    "INSERT INTO item (id, t, c, e, g, k) VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(id) DO UPDATE SET t=excluded.t, c=excluded.c, "
                    "e=excluded.e, k=excluded.k, "
                    "g=CASE WHEN excluded.g != '' THEN excluded.g ELSE item.g END",
                    (item["i"], item["t"], item["c"], item["e"],
                     "\u0001".join(item["g"]), k))
                db.execute("UPDATE sid SET at = ?, fail = 0 WHERE id = ?",
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
    log("补齐结束：新增 %d 部，查无 %d，失败 %d，%d 请求，用时 %.0f 分钟，还剩 %d 个 id"
        % (done, miss, err_n, req, (time.time() - started) / 60, pending))


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
    # sid：sitemap 给出的全站 id 宇宙（at=0 待补 / at>0 已补 / at=-1 查无此剧）
    db.execute("CREATE TABLE IF NOT EXISTS sid ("
               "id TEXT PRIMARY KEY, at INTEGER DEFAULT 0, fail INTEGER DEFAULT 0)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_item_k ON item (k)")
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
        if name.endswith(".jsonl.gz") or name == "catalog.json":
            os.remove(os.path.join(OUT_DIR, name))
    manifest = {"version": int(time.time()), "built": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "total": 0, "shards": []}
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
    path = os.path.join(OUT_DIR, "catalog.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)
    size = sum(s["bytes"] for s in manifest["shards"])
    log("打包完成：%d 部，%d 片，压缩后 %.1f MB -> %s"
        % (total, len(manifest["shards"]), size / 1024 / 1024, path))


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
    if mb > 24:
        log("  ⚠ 超过 24MB，Pages 单文件上限 25MB，该给快照切片了")


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
