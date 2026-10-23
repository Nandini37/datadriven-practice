import json
import re
import unicodedata
from datetime import datetime, timezone

_INF = float("inf")
_UTC = timezone.utc

_MID2 = re.compile(r"M[-_]?([0-9]{7})\Z")
_NUM = re.compile(r"([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?\s*(kwh|wh)?\Z", re.I)

_Q = {
    "A": "actual", "ACT": "actual", "ACTUAL": "actual",
    "E": "estimated", "EST": "estimated", "ESTIMATED": "estimated",
    "SUB": "estimated", "SUBSTITUTED": "estimated",
}

_DELOPS = {"d", "delete", "deleted", "remove"}
_WRAP = ("after", "new", "payload", "data", "record", "row", "current")

_TS_MIN = 946684800      # 2000-01-01
_TS_MAX = 4102444800     # 2100-01-01


def _from_str(s):
    s = s.strip()
    m = _NUM.match(s)
    if not m:
        if "," in s:
            if "." in s or s.count(",") > 1:
                s2 = s.replace(",", "")
            else:
                s2 = s.replace(",", ".")
            m = _NUM.match(s2.strip())
        if not m:
            return None
    sign, ip, fp, ex, unit = m.groups()
    fp = fp or ""
    if not ip and not fp:
        return None
    digits = ip + fp
    if len(digits) > 30:
        return None
    ex = int(ex) if ex else 0
    if ex > 30 or ex < -30:
        return None
    n = int(digits)
    e = ex - len(fp) + 3
    if unit and unit.lower() == "wh":
        e -= 3
    if e >= 0:
        v = n * 10 ** e
    else:
        d = 10 ** (-e) if -e <= 40 else 0
        if d == 0:
            v = 0
        else:
            v, rem = divmod(n, d)
            if rem * 2 >= d:
                v += 1
    if v > 10 ** 15:
        return None
    return -v if sign == "-" else v


def _milli(v):
    t = type(v)
    if t is float:
        if v != v or v == _INF or v == -_INF or abs(v) > 1e12:
            return None
        x = v * 1000.0
        r = round(x)
        if abs(x - r) < 1e-6:
            return int(r)
        return _from_str(repr(v))
    if t is int:
        if abs(v) > 10 ** 12:
            return None
        return v * 1000
    if t is str:
        return _from_str(v)
    return None


_FORMATS = ("%Y/%m/%d %H:%M:%S", "%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z")


def _iso(s):
    d = None
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        for f in _FORMATS:
            try:
                d = datetime.strptime(s, f)
                break
            except ValueError:
                pass
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return int(d.timestamp() // 1)


def _ts(v):
    t = type(v)
    if t is bool:
        return None
    if t is int or t is float:
        x = v
    elif t is str:
        s = v.strip()
        if not s:
            return None
        if len(s) == 14 and s.isdigit():
            ts = _iso(s)
        elif s.lstrip("+-").replace(".", "", 1).isdigit():
            try:
                x = float(s)
            except ValueError:
                return None
            ts = None
            return _snap(_epoch(x))
        else:
            ts = _iso(s)
        return _snap(ts)
    else:
        return None
    return _snap(_epoch(x))


def _epoch(x):
    if x != x or abs(x) > 1e20:
        return None
    ax = abs(x)
    if ax >= 1e17:
        x /= 1e9
    elif ax >= 1e14:
        x /= 1e6
    elif ax >= 1e11:
        x /= 1e3
    return int(round(x))


def _snap(ts):
    if ts is None or ts < _TS_MIN or ts > _TS_MAX:
        return None
    if ts % 1800:
        if ts % 60 and (ts // 60) % 30 == 0:
            ts -= ts % 60
        else:
            return None
    return ts


def _norm_mid(s):
    s = unicodedata.normalize("NFKC", s)
    s = "".join(c for c in s if not c.isspace() and c not in "\u200b\u200c\u200d\ufeff").upper()
    m = _MID2.match(s)
    return "M-" + m.group(1) if m else None


def _unwrap(rec, depth):
    op = rec.get("op")
    if type(op) is str and op.lower() in _DELOPS:
        return None
    for k in _WRAP:
        v = rec.get(k)
        if type(v) is dict:
            if "meter_id" in v:
                return v
            if depth < 3:
                r = _unwrap(v, depth + 1)
                if r is not None:
                    return r
    return None


class Handler:
    def __init__(self):
        self.emitted = {}      # mid -> set(ts) already output
        self.held = {}         # (mid, ts) -> (u, r, q): estimates / empty rows, output at finalize
        self.waiting = {}      # (mid, prev_ts) -> (ts, r, q): register-only rows awaiting predecessor
        self.wkeys = set()     # keys sitting in waiting
        self.last = {}         # mid -> (ts, register) of newest actual register seen
        self.regonly = set()   # meters that report registers only
        self.regs = {}         # mid -> {ts: register} for regonly meters
        self.mc = {}
        self.tc = {}
        self.qc = {}
        self.sc = {}

    # ---- helpers -------------------------------------------------------
    def _tstr(self, ts):
        s = self.sc.get(ts)
        if s is None:
            s = datetime.fromtimestamp(ts, _UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self.sc[ts] = s
        return s

    def _emit(self, mid, ts, u, r, q, out):
        self.emitted.setdefault(mid, set()).add(ts)
        out.append({
            "meter_id": mid,
            "interval_end": self._tstr(ts),
            "usage_kwh": None if u is None else "%d.%03d" % divmod(u, 1000),
            "register_kwh": None if r is None else "%d.%03d" % divmod(r, 1000),
            "quality": q,
        })

    def _prev_reg(self, mid, ts):
        p = ts - 1800
        d = self.regs.get(mid)
        if d is not None:
            r = d.get(p)
            if r is not None:
                return r
        lr = self.last.get(mid)
        if lr is not None and lr[0] == p:
            return lr[1]
        return None

    def _record(self, mid, ts, rg):
        lr = self.last.get(mid)
        if lr is None or ts > lr[0]:
            self.last[mid] = (ts, rg)
        if mid in self.regonly:
            self.regs[mid][ts] = rg

    def _seen(self, mid, ts, rg, out):
        self._record(mid, ts, rg)
        if not self.waiting:
            return
        w = self.waiting.pop((mid, ts), None)
        while w is not None:
            ts2, rg2, q2 = w
            self.wkeys.discard((mid, ts2))
            u2 = rg2 - rg
            if u2 < 0:
                u2 = None
            self._emit(mid, ts2, u2, rg2, q2, out)
            self._record(mid, ts2, rg2)
            rg, ts = rg2, ts2
            w = self.waiting.pop((mid, ts), None)

    # ---- stream --------------------------------------------------------
    def process(self, raw):
        try:
            return self._process(raw)
        except Exception:
            return []

    def _process(self, raw):
        try:
            rec = json.loads(raw)
        except Exception:
            return []
        if type(rec) is not dict:
            return []
        mv = rec.get("meter_id")
        if mv is None:
            rec = _unwrap(rec, 0)
            if rec is None:
                return []
            mv = rec.get("meter_id")
        elif "op" in rec:
            op = rec.get("op")
            if type(op) is str and op.lower() in _DELOPS:
                return []
        if type(mv) is not str:
            return []
        mid = self.mc.get(mv, 0)
        if mid == 0:
            mid = _norm_mid(mv)
            if len(self.mc) > 200000:
                self.mc.clear()
            self.mc[mv] = mid
        if mid is None:
            return []

        tv = rec.get("interval_end")
        tt = type(tv)
        if tt is str:
            ts = self.tc.get(tv, 0)
            if ts == 0:
                ts = _ts(tv)
                if len(self.tc) > 200000:
                    self.tc.clear()
                self.tc[tv] = ts
        elif tt is int or tt is float:
            ts = _ts(tv)
        else:
            return []
        if ts is None:
            return []

        qv = rec.get("quality")
        if type(qv) is str:
            q = self.qc.get(qv, 0)
            if q == 0:
                q = _Q.get(qv.strip().upper())
                self.qc[qv] = q
        else:
            q = None

        kv = rec.get("kwh")
        if kv is None:
            kv = rec.get("usage_kwh")
        u = None if kv is None else _milli(kv)
        if u is not None and u < 0:
            u = None
        rv = rec.get("register")
        if rv is None:
            rv = rec.get("register_kwh")
        rg = None if rv is None else _milli(rv)
        if rg is not None and rg < 0:
            rg = None

        em = self.emitted.get(mid)
        if em is None:
            em = self.emitted[mid] = set()
        if ts in em:
            return []
        key = (mid, ts)
        has = u is not None or rg is not None

        if q == "estimated" or not has:
            if key in self.wkeys:
                return []
            old = self.held.get(key)
            if old is not None and not has and (old[0] is not None or old[1] is not None):
                return []
            self.held[key] = (u, rg, q)
            return []

        # an actual (or unlabelled) read with data
        if self.held:
            self.held.pop(key, None)
        if self.wkeys and key in self.wkeys:
            self.waiting.pop((mid, ts - 1800), None)
            self.wkeys.discard(key)

        out = []
        if u is None:
            if mid not in self.regonly:
                self.regonly.add(mid)
                d = self.regs[mid] = {}
                lr = self.last.get(mid)
                if lr is not None:
                    d[lr[0]] = lr[1]
            pr = self._prev_reg(mid, ts)
            if pr is None:
                self.waiting[(mid, ts - 1800)] = (ts, rg, q)
                self.wkeys.add(key)
                self._seen(mid, ts, rg, out)
                return out
            u = rg - pr
            if u < 0:
                u = None
        self._emit(mid, ts, u, rg, q, out)
        if rg is not None:
            self._seen(mid, ts, rg, out)
        return out

    def finalize(self):
        try:
            return self._finalize()
        except Exception:
            return []

    def _finalize(self):
        out = []
        for (mid, ts), (u, rg, q) in self.held.items():
            if ts in self.emitted.get(mid, ()) or (mid, ts) in self.wkeys:
                continue
            if u is None and rg is not None:
                pr = self._prev_reg(mid, ts)
                if pr is not None and rg >= pr:
                    u = rg - pr
            self._emit(mid, ts, u, rg, q, out)
        self.held = {}
        for (mid, _prev), (ts, rg, q) in self.waiting.items():
            if ts in self.emitted.get(mid, ()):
                continue
            self._emit(mid, ts, None, rg, q, out)
        self.waiting = {}
        self.wkeys = set()
        return out
