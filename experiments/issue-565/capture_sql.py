"""Capture-trigger DDL for #565 experiments, generated per source table (the shape a real
installer would emit): the image encoding is per-column `col::text`, which is what pgoutput's
text mode sends and what `tuple_to_json` renders, so the ring rows match today's shape.

Variants:
  row   AFTER INSERT OR UPDATE OR DELETE ... FOR EACH ROW, one INSERT per changed row
  stmt  AFTER <event> ... FOR EACH STATEMENT with transition tables, one INSERT ... SELECT
        per statement (three triggers: transition tables allow one event per trigger)

Both resolve the active slot with an unlocked read of segment_pointer and a CASE over static
INSERTs, one per ring slot, so every branch keeps a cached plan.
"""

RING = 4
SCHEMA = "trellis"


def img(rec, cols):
    if ENC["mode"] == "format":
        return "jsonb_build_object(" + ", ".join(
            f"'{c}', CASE WHEN {rec}.{c} IS NULL THEN NULL ELSE format('%s', {rec}.{c}) END" for c in cols) + ")"
    if ENC["mode"] == "hstore":
        return f"public.hstore_to_json(public.hstore({rec}))::jsonb"
    return "jsonb_build_object(" + ", ".join(f"'{c}', {rec}.{c}::text" for c in cols) + ")"


def key(rec, pk):
    return f"{rec}.{pk[0]}::text" if len(pk) == 1 else " || chr(31) || ".join(f"{rec}.{c}::text" for c in pk)


def lsn_expr(mode):
    # 'insert_lsn' keeps a per-key commit-monotone `lsn` without a global sequence;
    # 'null' leaves lsn/origin_lsn unset (the spike would decide).
    return "pg_current_wal_insert_lsn()" if mode == "insert_lsn" else "NULL::pg_lsn"


def case_insert(select_sql_for_slot):
    arms = "\n".join(f"    WHEN {s} THEN {select_sql_for_slot(s)};" for s in range(RING))
    return (f"  CASE slot\n{arms}\n    ELSE RAISE EXCEPTION 'trellis capture: no ring slot (read %)', slot;\n  END CASE;")


COLS = "src_table, key, op, lsn, old_image, new_image, origin_lsn, src_changed"


PTR = {"mode": "table"}
# Phase-2 trigger shapes (all off = the phase-1 statement trigger):
#   new_only   no OLD image in the ring (#558's ledger holds each row's applied contribution)
#   skip_noop  drop UPDATE rows whose read columns are unchanged (a statement trigger can't use
#              WHEN or UPDATE OF with transition tables, so the filter goes in the SELECT)
#   reread     images are the live row at fire time, joined on the key; op is delete when the key is
#              gone. The fix candidate for nested same-key writes (app AFTER triggers), whose
#              capture fires before the outer statement's.
SHAPE = {"new_only": False, "skip_noop": False, "reread": False}
# Image encoding: 'coltext' (per-column ::text, generated) or 'hstore' (column-agnostic, output
# functions; parity with tuple_to_json per E3). PIN adds the SET clauses pinning Trellis's output GUCs.
ENC = {"mode": "coltext", "pin": False}
PIN_SQL = ("SET datestyle = 'ISO, YMD' SET bytea_output = 'hex' SET extra_float_digits = 1 "
           "SET intervalstyle = 'postgres' SET timezone = 'UTC'")


def ptr_read():
    if PTR["mode"] == "mirror":
        # #597's read: xid assigned in the same expression, before the snapshot-independent read
        return (f"  slot := CASE WHEN pg_current_xact_id() IS NOT NULL THEN "
                f"pg_sequence_last_value('{SCHEMA}.ring_slot_mirror'::regclass)::smallint END;")
    if PTR["mode"] == "seq":
        return f"  slot := pg_sequence_last_value('{SCHEMA}.active_slot_seq'::regclass);"
    return f"  SELECT ring_slot INTO slot FROM {SCHEMA}.segment_pointer;"


def row_trigger(table, cols, pk, lsn_mode="insert_lsn", definer=True):
    fn = f"{SCHEMA}.capture_row_{table}"
    lsn = lsn_expr(lsn_mode)

    def ins(s):
        return (f"INSERT INTO {SCHEMA}.seg_{s} ({COLS}) VALUES ('public.{table}', k, op, l, oi, ni, l, clock_timestamp())")

    return f"""
CREATE OR REPLACE FUNCTION {fn}() RETURNS trigger LANGUAGE plpgsql
  {'SECURITY DEFINER' if definer else ''} SET search_path = pg_catalog, pg_temp {PIN_SQL if ENC["pin"] else ''} AS $f$
DECLARE slot smallint; k text; op text; oi jsonb; ni jsonb; l pg_lsn := {lsn};
BEGIN
{ptr_read()}
  IF TG_OP = 'INSERT' THEN
    op := 'insert'; ni := {img('NEW', cols)}; k := {key('NEW', pk)};
  ELSIF TG_OP = 'UPDATE' THEN
    op := 'update'; oi := {img('OLD', cols)}; ni := {img('NEW', cols)}; k := {key('NEW', pk)};
  ELSE
    op := 'delete'; oi := {img('OLD', cols)}; k := {key('OLD', pk)};
  END IF;
{case_insert(ins)}
  RETURN NULL;
END $f$;
CREATE TRIGGER trellis_capture AFTER INSERT OR UPDATE OR DELETE ON public.{table}
  FOR EACH ROW EXECUTE FUNCTION {fn}();
"""


def stmt_trigger(table, cols, pk, lsn_mode="insert_lsn", definer=True):
    lsn = lsn_expr(lsn_mode)
    out = []
    on = " AND ".join(f"o.{c} = n.{c}" for c in pk)
    oimg = (lambda r: "NULL::jsonb") if SHAPE["new_only"] else (lambda r: img(r, cols))
    if SHAPE["reread"]:
        cur_on = lambda r: " AND ".join(f"cur.{c} = {r}.{c}" for c in pk)
        live = f"LEFT JOIN public.{table} cur ON {{}}"
        ins = lambda s: (f"INSERT INTO {SCHEMA}.seg_{s} ({COLS}) SELECT 'public.{table}', {key('n', pk)}, "
                         f"CASE WHEN cur.{pk[0]} IS NULL THEN 'delete' ELSE 'insert' END, l, NULL, "
                         f"CASE WHEN cur.{pk[0]} IS NOT NULL THEN {img('cur', cols)} END, l, clock_timestamp() "
                         f"FROM n_rows n " + live.format(cur_on("n")))
        dele = lambda s: (f"INSERT INTO {SCHEMA}.seg_{s} ({COLS}) SELECT 'public.{table}', {key('o', pk)}, "
                          f"CASE WHEN cur.{pk[0]} IS NULL THEN 'delete' ELSE 'update' END, l, NULL, "
                          f"CASE WHEN cur.{pk[0]} IS NOT NULL THEN {img('cur', cols)} END, l, clock_timestamp() "
                          f"FROM o_rows o " + live.format(cur_on("o")))
        filt = (" WHERE o.{0} IS NULL OR n.{0} IS NULL OR ".format(pk[0]) +
                " OR ".join(f"o.{c} IS DISTINCT FROM n.{c}" for c in cols if c not in pk)) if SHAPE["skip_noop"] else ""
        upd = lambda s: (f"INSERT INTO {SCHEMA}.seg_{s} ({COLS}) SELECT 'public.{table}', "
                         f"coalesce({key('n', pk)}, {key('o', pk)}), "
                         f"CASE WHEN cur.{pk[0]} IS NULL THEN 'delete' ELSE 'update' END, l, NULL, "
                         f"CASE WHEN cur.{pk[0]} IS NOT NULL THEN {img('cur', cols)} END, l, clock_timestamp() "
                         f"FROM o_rows o FULL JOIN n_rows n ON {on} "
                         + live.format(" AND ".join(f"cur.{c} = coalesce(n.{c}, o.{c})" for c in pk)) + filt)
    else:
        ins = lambda s: f"INSERT INTO {SCHEMA}.seg_{s} ({COLS}) SELECT 'public.{table}', {key('n', pk)}, 'insert', l, NULL, {img('n', cols)}, l, clock_timestamp() FROM n_rows n"
        dele = lambda s: f"INSERT INTO {SCHEMA}.seg_{s} ({COLS}) SELECT 'public.{table}', {key('o', pk)}, 'delete', l, {oimg('o')}, NULL, l, clock_timestamp() FROM o_rows o"
        filt = (" WHERE o.{0} IS NULL OR n.{0} IS NULL OR ".format(pk[0]) +
                " OR ".join(f"o.{c} IS DISTINCT FROM n.{c}" for c in cols if c not in pk)) if SHAPE["skip_noop"] else ""
        # A statement-level UPDATE has no row pairing; join old and new on the primary key.
        # A key move surfaces as a delete of the old key plus an insert of the new one.
        upd = lambda s: (f"INSERT INTO {SCHEMA}.seg_{s} ({COLS}) SELECT 'public.{table}', "
                         f"coalesce({key('n', pk)}, {key('o', pk)}), "
                         f"CASE WHEN n.{pk[0]} IS NULL THEN 'delete' WHEN o.{pk[0]} IS NULL THEN 'insert' ELSE 'update' END, l, "
                         f"CASE WHEN o.{pk[0]} IS NOT NULL THEN {oimg('o')} END, "
                         f"CASE WHEN n.{pk[0]} IS NOT NULL THEN {img('n', cols)} END, l, clock_timestamp() "
                         f"FROM o_rows o FULL JOIN n_rows n ON {on}{filt}")
    for ev, refs, body in [
        ("INSERT", "NEW TABLE AS n_rows", ins),
        ("DELETE", "OLD TABLE AS o_rows", dele),
        ("UPDATE", "OLD TABLE AS o_rows NEW TABLE AS n_rows", upd),
    ]:
        fn = f"{SCHEMA}.capture_stmt_{ev.lower()}_{table}"
        out.append(f"""
CREATE OR REPLACE FUNCTION {fn}() RETURNS trigger LANGUAGE plpgsql
  {'SECURITY DEFINER' if definer else ''} SET search_path = pg_catalog, pg_temp {PIN_SQL if ENC["pin"] else ''} AS $f$
DECLARE slot smallint; l pg_lsn := {lsn};
BEGIN
{ptr_read()}
{case_insert(body)}
  RETURN NULL;
END $f$;
CREATE TRIGGER trellis_capture_{ev.lower()} AFTER {ev} ON public.{table}
  REFERENCING {refs} FOR EACH STATEMENT EXECUTE FUNCTION {fn}();
""")
    return "\n".join(out)


if __name__ == "__main__":
    import sys
    v = sys.argv[1] if len(sys.argv) > 1 else "row"
    f = row_trigger if v == "row" else stmt_trigger
    print(f("gen", ["id", "val"], ["id"]))
