/* dag.yml
dag: the_basket
schedule: hourly                 # order_events keeps only 6 h; a nightly run would see 6 of every 24 hours
catchup: true
tasks:
  order_events_history:                # every event ever seen, deduped by event_id; hourly so nothing ages out unseen
    materialized: incremental
    incremental_strategy: merge
    unique_key: [event_id]
    depends_on: [order_events]
  scans_history:                 # every send of every day; the landing keeps 3 nights, this keeps all
    materialized: incremental
    incremental_strategy: merge
    unique_key: [batch, loaded, scan_id]
    schedule: "0 5,6 * * *"
    depends_on: [scans]
  prices_history:                # the landing keeps only the latest list; this keeps every copy
    materialized: incremental
    incremental_strategy: merge
    unique_key: [ckey, loaded]
    schedule: "0 */6 * * *"      # each copy lives ~24 h, so 6-hourly can't miss one
    depends_on: [prices]
  order_charges:                 # the scored table; only needs building at the end of each night
    materialized: table
    schedule: "0 5,6 * * *"
    trigger_rule: none_failed    # scans/prices tasks are skipped on most runs; that's fine
    depends_on: [order_events_history, scans_history, prices_history]

*/

-- each file is 1 SELECT, built into a table of the file's name; dag.yml says what it runs after

-- task: order_events_history
WITH r AS (
  SELECT nullif(trim(event_id), '')   AS eid,
         trim(order_id)               AS oid,
         lower(trim(event))           AS e,
         trim(event_at)               AS ea,
         trim(written_at)             AS wa,
         CASE WHEN upper(trim(detail)) IN ('', 'NULL') THEN NULL ELSE trim(detail) END AS detail
  FROM order_events
),
n AS (
  SELECT eid, ea, detail,
         -- 8-digit text key; re-pad ids that lost leading zeros
         CASE WHEN length(oid) BETWEEN 1 AND 7 THEN substr('00000000' || oid, length(oid) + 1, 8) ELSE oid END AS order_id,
         -- each app has its own words for the same event
         CASE WHEN e LIKE '%declin%' OR e LIKE '%reject%'                         THEN 'replacement_declined'
              WHEN e LIKE '%approv%' OR e LIKE '%accept%'                         THEN 'replacement_approved'
              WHEN e LIKE '%tip%'                                                 THEN 'tip_changed'
              WHEN e LIKE '%reinstat%' OR e LIKE '%restor%' OR e LIKE '%uncancel%' THEN 'reinstated'
              WHEN e LIKE '%cancel%'                                              THEN 'cancelled'
              WHEN e LIKE '%delet%'                                               THEN 'deleted'
              WHEN e LIKE '%deliver%' OR e LIKE 'complete%'                       THEN 'delivered'
              WHEN e LIKE '%placed%' OR e LIKE 'creat%'                           THEN 'placed'
              ELSE e END AS kind,
         -- 'YYYY-MM-DD HH:MM:SS' (UTC) or NULL when it isn't a timestamp
         CASE WHEN substr(ea,5,1) = '-' AND substr(ea,8,1) = '-' AND substr(ea,11,1) IN ('T',' ') AND substr(ea,14,1) = ':'
              THEN substr(ea,1,10) || ' ' || substr(ea,12,8) END AS ev_ts,
         CASE WHEN substr(wa,5,1) = '-' AND substr(wa,8,1) = '-' AND substr(wa,11,1) IN ('T',' ') AND substr(wa,14,1) = ':'
              THEN substr(wa,1,10) || ' ' || substr(wa,12,8) END AS wr_ts,
         replace(detail, '$', '') AS d1
  FROM r
),
m AS (
  SELECT coalesce(eid, order_id || '|' || kind || '|' || coalesce(ea, '')) AS event_id,
         order_id, kind, ev_ts, wr_ts, detail,
         CASE WHEN d1 <> '' AND trim(d1, '0123456789.-') = '' AND trim(d1, '.-') <> ''
              THEN CAST(d1 AS DOUBLE) END AS amt
  FROM n
  WHERE length(order_id) = 8 AND trim(order_id, '0123456789') = ''
),
k AS (
  SELECT m.*, row_number() OVER (PARTITION BY event_id ORDER BY wr_ts DESC, ev_ts DESC) AS rn
  FROM m
)
SELECT order_id, event_id, kind, ev_ts, wr_ts, detail, amt
FROM k
WHERE rn = 1;

-- task: scans_history
WITH r AS (
  SELECT trim(_batch_id)    AS b,
         trim(_loaded_at)   AS la,
         trim(scan_id)      AS scan_id,
         trim(order_id)     AS oid,
         trim(code)         AS code,
         trim(qty)          AS q,
         trim(weight_lb)    AS w
  FROM scans
),
n AS (
  SELECT scan_id, code,
         CASE WHEN length(b) = 8 AND trim(b, '0123456789') = ''
              THEN substr(b,1,4) || '-' || substr(b,5,2) || '-' || substr(b,7,2)
              ELSE substr(b,1,10) END AS batch,
         coalesce(CASE WHEN substr(la,5,1) = '-' AND substr(la,8,1) = '-' AND substr(la,11,1) IN ('T',' ') AND substr(la,14,1) = ':'
                       THEN substr(la,1,10) || ' ' || substr(la,12,8) END,
                  '0000-00-00 00:00:00') AS loaded,
         CASE WHEN length(oid) BETWEEN 1 AND 7 THEN substr('00000000' || oid, length(oid) + 1, 8) ELSE oid END AS order_id,
         ltrim(code, '0') AS ckey,
         -- numbers: 'NULL', '' and other junk become NULL instead of an error
         CASE WHEN q <> '' AND trim(q, '0123456789.-') = '' AND trim(q, '.-') <> '' THEN CAST(q AS DOUBLE) END AS qty_n,
         CASE WHEN w <> '' AND trim(w, '0123456789.-') = '' AND trim(w, '.-') <> '' THEN CAST(w AS DOUBLE) END AS wt_n
  FROM r
),
k AS (
  SELECT n.*, row_number() OVER (PARTITION BY batch, loaded, scan_id ORDER BY order_id) AS rn   -- exact copies collapse
  FROM n
  WHERE scan_id <> '' AND length(order_id) = 8 AND trim(order_id, '0123456789') = ''
)
SELECT batch, loaded, scan_id, order_id, code, ckey, qty_n, wt_n
FROM k
WHERE rn = 1;

-- task: prices_history
WITH r AS (
  SELECT ltrim(trim(code), '0')             AS ckey,
         trim(_loaded_at)                   AS la,
         replace(trim(price), '$', '')      AS p,
         lower(trim(sold_by))               AS sb
  FROM prices
),
n AS (
  SELECT ckey,
         coalesce(CASE WHEN substr(la,5,1) = '-' AND substr(la,8,1) = '-' AND substr(la,11,1) IN ('T',' ') AND substr(la,14,1) = ':'
                       THEN substr(la,1,10) || ' ' || substr(la,12,8) END,
                  '0000-00-00 00:00:00') AS loaded,
         CASE WHEN p <> '' AND trim(p, '0123456789.') = '' AND trim(p, '.') <> ''
              THEN CAST(round(CAST(p AS DOUBLE) * 1000) AS INTEGER) END AS price_m,   -- price in mills, so no float drift
         CASE WHEN sb LIKE '%lb%' OR sb LIKE '%pound%' THEN 1 ELSE 0 END AS lb
  FROM r
),
k AS (
  SELECT n.*, row_number() OVER (PARTITION BY ckey, loaded ORDER BY price_m) AS rn
  FROM n
  WHERE ckey <> '' AND price_m IS NOT NULL
)
SELECT ckey, loaded, price_m, lb
FROM k
WHERE rn = 1;

-- task: order_charges
WITH
ev AS (
  SELECT order_id, event_id, kind, detail, amt, ev_ts, wr_ts,
         coalesce(ev_ts, wr_ts) AS ord_ts          -- ordering key; falls back to the order service's clock
  FROM events_history
),
placed AS (
  SELECT order_id, min(ord_ts) AS placed_ts
  FROM ev WHERE kind = 'placed'
  GROUP BY order_id
),
deleted AS (
  SELECT DISTINCT order_id FROM ev WHERE kind = 'deleted'
),
cr AS (                                            -- last cancel / reinstate decides
  SELECT order_id, kind FROM (
    SELECT order_id, kind,
           row_number() OVER (PARTITION BY order_id ORDER BY ord_ts DESC, wr_ts DESC, event_id DESC) AS rn
    FROM ev WHERE kind IN ('cancelled', 'reinstated')
  ) x WHERE rn = 1
),
tip AS (                                           -- the tip set last (placed carries the first one)
  SELECT order_id, amt FROM (
    SELECT order_id, amt,
           row_number() OVER (PARTITION BY order_id
                              ORDER BY ord_ts DESC, CASE WHEN kind = 'tip_changed' THEN 1 ELSE 0 END DESC, wr_ts DESC, event_id DESC) AS rn
    FROM ev WHERE kind IN ('placed', 'tip_changed') AND amt IS NOT NULL
  ) x WHERE rn = 1
),
deliv AS (                                         -- event_at, not written_at: the driver's phone syncs late
  SELECT order_id, min(ev_ts) AS delivered_ts
  FROM ev WHERE kind = 'delivered' AND ev_ts >= '2026-01-01'
  GROUP BY order_id
),
orders AS (
  SELECT p.order_id, p.placed_ts,
         CASE WHEN cr.kind = 'cancelled' THEN 'cancelled' ELSE 'delivered' END AS status,
         d.delivered_ts,
         coalesce(t.amt, 0.0) AS tip
  FROM placed p
  LEFT JOIN deleted x ON x.order_id = p.order_id
  LEFT JOIN cr        ON cr.order_id = p.order_id
  LEFT JOIN deliv d   ON d.order_id = p.order_id
  LEFT JOIN tip t     ON t.order_id = p.order_id
  WHERE x.order_id IS NULL
    AND substr(p.placed_ts, 1, 10) BETWEEN '2026-10-16' AND '2026-11-14'
),

-- scans: the latest send of each day replaces earlier sends of that day, then 1 row per scan_id
sends AS (
  SELECT batch, max(loaded) AS loaded FROM scans_history GROUP BY batch
),
sc AS (
  SELECT scan_id, order_id, code, ckey, qty_n, wt_n FROM (
    SELECT h.*, row_number() OVER (PARTITION BY h.scan_id ORDER BY h.loaded DESC, h.batch DESC) AS rn
    FROM scans_history h
    JOIN sends s ON s.batch = h.batch AND s.loaded = h.loaded
  ) x WHERE rn = 1
),
rep AS (                                           -- a replacement is charged unless the customer last declined it
  SELECT detail AS scan_id, kind FROM (
    SELECT detail, kind,
           row_number() OVER (PARTITION BY detail ORDER BY ord_ts DESC, wr_ts DESC, event_id DESC) AS rn
    FROM ev WHERE kind IN ('replacement_declined', 'replacement_approved') AND detail IS NOT NULL
  ) x WHERE rn = 1
),

-- prices: keep only the copies where an item's price (or unit) changed, with the span each one covers
p1 AS (
  SELECT ckey, loaded, price_m, lb,
         lag(price_m) OVER (PARTITION BY ckey ORDER BY loaded) AS pp,
         lag(lb)      OVER (PARTITION BY ckey ORDER BY loaded) AS pl
  FROM prices_history
),
pv AS (
  SELECT ckey, loaded, price_m, lb,
         lead(loaded)    OVER (PARTITION BY ckey ORDER BY loaded) AS next_loaded,
         row_number()    OVER (PARTITION BY ckey ORDER BY loaded) AS vn
  FROM p1
  WHERE pp IS NULL OR pp <> price_m OR pl <> lb
),

base AS (
  SELECT s.order_id, s.code, s.ckey, s.qty_n, s.wt_n, o.placed_ts
  FROM sc s
  JOIN orders o ON o.order_id = s.order_id AND o.status <> 'cancelled'
  LEFT JOIN rep r ON r.scan_id = s.scan_id
  WHERE r.kind IS NULL OR r.kind <> 'replacement_declined'
),
priced AS (
  -- price in force when placed = latest copy before the order; if none yet, the first price ever listed
  SELECT b.order_id, b.code, b.qty_n, b.wt_n,
         coalesce(v.price_m, f.price_m) AS price_m,
         coalesce(v.lb, f.lb)           AS lb
  FROM base b
  LEFT JOIN pv v ON v.ckey = b.ckey AND v.loaded < b.placed_ts AND (v.next_loaded IS NULL OR v.next_loaded >= b.placed_ts)
  LEFT JOIN pv f ON f.ckey = b.ckey AND f.vn = 1
),
l1 AS (
  SELECT order_id, qty_n, wt_n, price_m,
         -- store label: 12 digits starting with 2, digits 7-11 are the price in cents
         CASE WHEN length(code) = 12 AND substr(code,1,1) = '2' AND trim(substr(code,7,5), '0123456789') = ''
              THEN CAST(substr(code,7,5) AS INTEGER) END AS label_c,
         coalesce(lb, CASE WHEN wt_n IS NOT NULL THEN 1 ELSE 0 END) AS lb,
         CASE WHEN qty_n < 0 OR wt_n < 0 THEN -1 ELSE 1 END AS sgn   -- a take-back scans negative
  FROM priced
),
l2 AS (
  SELECT order_id,
         CASE WHEN label_c IS NULL AND lb = 1 THEN sgn                    -- by the pound: 1 item per scan
              ELSE CAST(round(qty_n) AS INTEGER) END AS n_items,
         CASE WHEN label_c IS NOT NULL THEN CAST(round(qty_n) AS INTEGER) * label_c
              WHEN lb = 1 THEN sgn * CAST(round(round(abs(coalesce(wt_n, qty_n)) * 1000) * price_m / 10000.0) AS INTEGER)   -- weight x price, rounded per line
              ELSE CAST(round(round(qty_n) * price_m / 10.0) AS INTEGER) END AS cents
  FROM l1
),
tot AS (
  SELECT order_id, sum(n_items) AS items, sum(cents) AS cents
  FROM l2 GROUP BY order_id
)
SELECT o.order_id,
       o.status,
       CASE WHEN o.status = 'cancelled' OR o.delivered_ts IS NULL THEN NULL
            ELSE replace(o.delivered_ts, ' ', 'T') || 'Z' END AS delivered_at,
       CAST(coalesce(t.items, 0) AS INTEGER) AS items,
       CAST(coalesce(t.cents, 0) AS DOUBLE) / 100.0 AS subtotal,
       CASE WHEN o.status = 'cancelled' THEN 0.0 ELSE round(o.tip, 2) END AS tip
FROM orders o
LEFT JOIN tot t ON t.order_id = o.order_id;
