#!/usr/bin/env python3
"""X5 per-run analysis. usage: analyze.py RUN_DIR...  -> one TSV line per run (header first).

Windows (from summary.tsv): pre = [t_w0+1, t_ins0) (writers, no capture trigger yet),
build = [t_apply, t_live], post = (t_live, t_wstop] (capture on, no build).
Hold-up: a Trellis session waiting on a heavyweight lock whose root blocker (following
pg_blocking_pids) is a Trellis session whose transaction is >= 0.5 s old (the build).
"""
import gzip, io, os, re, sys, tarfile, statistics as st


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def load_summary(d):
    lines = open(os.path.join(d, "summary.tsv")).read().split("\n")
    h, v = lines[0].split("\t"), lines[1].split("\t")
    return dict(zip(h, v))


def writers(d):
    p = os.path.join(d, "writers.tgz")
    rows = []
    if not os.path.exists(p):
        return rows
    with tarfile.open(p) as tf:
        for m in tf.getmembers():
            if m.isfile():
                for line in tf.extractfile(m).read().decode().splitlines():
                    t, dur, n, e = line.split("\t")
                    rows.append((float(t), float(dur), int(n), int(e)))
    return rows


def samples(d):
    p = os.path.join(d, "samples.tsv.gz")
    acts, ring = {}, []
    with gzip.open(p, "rt") as f:
        for line in f:
            c = line.rstrip("\n").split("\t")
            if c[0] == "act" and len(c) >= 10:
                t = round(float(c[1]), 1)
                acts.setdefault(t, []).append(dict(pid=c[2], state=c[3], wt=c[4], we=c[5], qage=float(c[6]),
                                                   xage=float(c[7]), blk=[b for b in c[8].split(",") if b], q=c[9],
                                                   app=c[10] if len(c) > 10 else ""))
            elif c[0] == "ring":
                ring.append((float(c[1]), int(c[2]), int(c[3]), int(c[4]), float(c[5]),
                             int(c[6]) if len(c) > 6 else None))
    return acts, ring


def holdup(acts, t0, t1):
    """per-sample blocked-on-build sessions in [t0,t1]"""
    out = []  # (t, n_blocked, max_wait_s)
    for t in sorted(acts):
        if t < t0 or t > t1:
            continue
        ss = {a["pid"]: a for a in acts[t]}
        nb, mw = 0, 0.0
        for a in ss.values():
            if a["wt"] != "Lock" or a["app"].startswith("x5-"):
                continue
            seen, cur, root = set(), a, None
            while cur and cur["blk"] and cur["pid"] not in seen:
                seen.add(cur["pid"])
                nxt = cur["blk"][0]
                root = nxt
                cur = ss.get(nxt)
            r = ss.get(root)
            if r is not None and r["xage"] >= 0.5 and r["pid"] != a["pid"] and not r["app"].startswith("x5-"):
                nb += 1
                mw = max(mw, a["qage"])
        out.append((t, nb, mw))
    return out


def longest_run(hs, iv=0.25):
    best, start, prev = 0.0, None, None
    for t, nb, _ in hs:
        if nb > 0:
            if start is None or (prev is not None and t - prev > 2.5 * iv):
                start = t
            best = max(best, t - start + iv)
            prev = t
        else:
            start = None
    return best


def pgss_step(d):
    """step-2 (proto) / rebuild update (base) statement from pgss-build-full.txt: ms, rows, kind"""
    p = os.path.join(d, "pgss-build-full.txt")
    if not os.path.exists(p):
        return {}
    txt = open(p).read()
    res = {}
    for m in re.finditer(r"-- calls=(\d+) ms=(\d+) rows=(\d+) wal=(\d+) fpi=(\d+) temp_mb=(\d+)\n(.*?)(?=\n-- calls=|\Z)", txt, re.S):
        calls, ms, rows, wal, fpi, tmp, q = m.groups()
        ql = q.lower()
        if not ql.startswith("with snap as materialized"):
            continue
        if re.search(r"do update set __n = excluded.__n where (false|\$\d+)", ql):
            k = "step1"
        elif re.search(r"rg as \(select distinct [^)]*\), a as \(", ql) and "update " in ql:
            k = "abs_update_groupby"
        elif "rg as (select distinct" in ql and "update " in ql:
            k = "abs_update_probe"
        else:
            continue
        r = res.setdefault(k, [0, 0, 0, 0])
        r[0] += int(calls); r[1] += int(ms); r[2] += int(rows); r[3] += int(wal)
    return res


def bases(d):
    p = os.path.join(d, "bases.txt")
    out = []
    for line in open(p).read().splitlines():
        if "|" in line:
            b, n = line.rsplit("|", 1)
            out.append((b, int(n)))
    return out


def analyze(d):
    s = load_summary(d)
    f = lambda k: float(s[k])
    tw0, ti0, ta, tl, tws = f("t_w0"), f("t_ins0"), f("t_apply"), f("t_live"), f("t_wstop")
    w = writers(d)
    acts, ring = samples(d)
    # the build transaction: the oldest transaction among sessions running a build statement in [t_apply, t_live]
    bx = []
    for t in sorted(acts):
        if ta <= t <= tl + 1:
            for a in acts[t]:
                if a["q"].startswith("with snap as materialized") or a["q"].startswith('insert into "trellis"."agg_'):
                    if a["xage"] >= 0.3:
                        bx.append((t - a["xage"], t, a["pid"]))
    if bx:
        b0 = min(x[0] for x in bx)
        pid = [x for x in bx if x[0] == b0][0][2]
        b1 = max(x[1] for x in bx if x[2] == pid and abs(x[0] - b0) < 0.05)
    else:
        b0, b1 = ta, tl
    res = dict(run=d.split("results/", 1)[-1], build_s=f("build_s"), ins_s=f("ins_s"),
               ratio=f("build_s") / f("ins_s"), drain_s=f("drain_s"), exact=s["exact"],
               build_wal_mb=int(s["build_wal"]) // 2**20, build_tx_s=round(b1 - b0, 2), install_s=round(b0 - ta, 2))
    if w:
        ok = [x for x in w if x[3] == 0]
        span = max(x[0] for x in w) - min(x[0] for x in w)
        res["rows_changed"] = sum(x[2] for x in ok)
        res["rate"] = round(res["rows_changed"] / span) if span > 0 else 0
        res["tx_err"] = sum(1 for x in w if x[3] != 0)
        for nm, a, b in (("pre", tw0 + 1, ti0), ("install", ta, b0), ("build", b0, b1), ("post", tl, tws)):
            lat = [x[1] for x in ok if a <= x[0] < b]
            rws = sum(x[2] for x in ok if a <= x[0] < b)
            res[f"lat_{nm}_p50"] = round(pct(lat, 50), 1)
            res[f"lat_{nm}_p99"] = round(pct(lat, 99), 1)
            res[f"rate_{nm}"] = round(rws / (b - a)) if b > a else 0
    else:
        res["rows_changed"] = 0
    hb = holdup(acts, ta, tl + 1.0)
    hp = holdup(acts, tl + 1.0, tws)
    res["hold_longest_s"] = round(longest_run(hb), 2)
    res["hold_max_wait_s"] = round(max([x[2] for x in hb] or [0]), 2)
    res["hold_sess_s"] = round(sum(x[1] for x in hb) * 0.25, 2)
    res["post_hold_max_wait_s"] = round(max([x[2] for x in hp] or [0]), 2)
    rb = [r for r in ring if ta <= r[0] <= tws]
    res["peak_backlog_rows"] = max([r[2] + r[3] for r in rb] or [0])
    res["peak_pending_age_s"] = round(max([r[4] for r in rb] or [0]), 1)
    rl = [r for r in ring if r[0] >= tl]
    res["backlog_at_live"] = (rl[0][2] + rl[0][3]) if rl else ""
    # seals: active_seq increases (or, in runs sampled before it was logged, the active slot's rows drop)
    seals, prev = [], None
    first = next((r[0] for r in ring if r[3] > 0 or r[1] > 0), None)  # capture began (no earlier seal to see)
    if first is not None:
        seals.append(first - 0.25)
    for r in ring:
        if prev is not None:
            if (r[5] is not None and prev[5] is not None and r[5] > prev[5]) or \
               (r[5] is None and prev[3] > 50 and r[3] < prev[3] * 0.5):
                seals.append(r[0])
        prev = r
    gaps = [(a, b2) for a, b2 in zip(seals, seals[1:])]
    over = [b2 - a for a, b2 in gaps if b2 > b0 and a < b1]
    res["seal_stall_s"] = round(max(over), 1) if over else ""
    pg = sorted(b2 - a for a, b2 in gaps if a > tl + 1 and b2 < tws)
    res["seal_gap_post_med_s"] = round(pg[len(pg) // 2], 1) if pg else ""
    b = bases(d)
    res["bases"] = ";".join(f"{n}" + ("(null)" if k == "NULL" else "") for k, n in b)
    snaps = [n for k, n in b if k != "NULL"]
    res["stragglers_by_basis"] = (sorted(snaps)[0] if len(snaps) == 2 else (0 if len(snaps) == 1 else "?"))
    for k, v in pgss_step(d).items():
        res[f"{k}_calls"], res[f"{k}_ms"], res[f"{k}_rows"] = v[0], v[1], v[2]
    return res


KEYS = ["run", "exact", "rate", "rows_changed", "tx_err", "ins_s", "build_s", "ratio", "build_wal_mb", "install_s", "build_tx_s", "drain_s",
        "stragglers_by_basis", "bases", "step1_ms", "step1_rows", "abs_update_probe_calls", "abs_update_probe_ms",
        "abs_update_probe_rows", "abs_update_groupby_calls", "abs_update_groupby_ms", "abs_update_groupby_rows",
        "seal_stall_s", "seal_gap_post_med_s", "hold_longest_s", "hold_max_wait_s", "hold_sess_s", "post_hold_max_wait_s", "peak_backlog_rows",
        "peak_pending_age_s", "backlog_at_live", "rate_pre", "rate_build", "rate_post", "lat_pre_p50", "lat_pre_p99",
        "lat_install_p50", "lat_install_p99", "lat_build_p50", "lat_build_p99", "lat_post_p50", "lat_post_p99"]
if __name__ == "__main__":
    print("\t".join(KEYS))
    dirs = []
    for d in sys.argv[1:]:
        d = d.rstrip("/")
        if os.path.exists(os.path.join(d, "summary.tsv")):
            dirs.append(d)
        elif os.path.isdir(d):
            dirs += sorted(os.path.join(d, x) for x in os.listdir(d) if os.path.exists(os.path.join(d, x, "summary.tsv")))
    for d in dirs:
        try:
            r = analyze(d.rstrip("/"))
            print("\t".join(str(r.get(k, "")) for k in KEYS))
        except Exception as e:
            print(f"{d}\tERROR {type(e).__name__}: {e}")
