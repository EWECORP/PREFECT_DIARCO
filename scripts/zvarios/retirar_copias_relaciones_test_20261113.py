"""Remove replicas of the same five TEST-only relations; never access FDW tables."""
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path

import psycopg2
from psycopg2 import sql
import reparar_maestros_test_20261113 as repair

TABLES = [
    ('inventory', 'inv_product_supplier'),
    ('procurement_and_sourcing', 'pas_product_supplier'),
    ('procurement_and_sourcing', 'pas_product_supplier_replicator'),
    ('supply_planning', 'spl_product_supplier'),
    ('cost_management', 'cst_product_supplier'),
    ('stock_management', 'stk_product_supplier'),
    ('commercial_agreements', 'cag_product_supplier'),
    ('public', 'fnd_product_supplier'),
]
IDS = repair.INVALID_RELATIONS + repair.DUPLICATE_RELATIONS


def main():
    settings = repair.load_platform_settings(repair.ROOT.parent / 'CNX_DIARCO_SYNC_V2' / '.env')
    test = settings.destinations['test'].database
    assert test.dbname == 'connexa_platform_test'
    prod = settings.destinations['prod'].database
    assert prod.dbname == 'connexa_platform_ms'
    # Same UUIDs must not represent valid PROD relations in any native replica.
    with closing(repair.connect_readonly(prod, 10000)) as conn:
        with conn.cursor() as cur:
            for schema, table in TABLES:
                cur.execute('SELECT relkind FROM pg_class WHERE oid=to_regclass(%s)', (f'{schema}.{table}',))
                kind = cur.fetchone()
                if not kind:
                    continue
                assert kind[0] in ('r', 'p'), 'Never query a foreign table'
                cur.execute(sql.SQL('SELECT count(*) FROM {}.{} WHERE id::text=ANY(%s)').format(
                    sql.Identifier(schema), sql.Identifier(table)), (IDS,))
                assert cur.fetchone()[0] == 0, f'UUID exists in PROD {schema}.{table}'
    conn = psycopg2.connect(host=test.host, port=test.port, dbname=test.dbname,
                           user=test.user, password=test.password, connect_timeout=10,
                           application_name='etl_diarco:retirar_copias_test_20261113',
                           options='-c lock_timeout=3000 -c statement_timeout=30000')
    conn.set_session(isolation_level='SERIALIZABLE')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    path = repair.ROOT / 'data' / 'repairs' / f'maestros_test_replicas_{stamp}.json'
    before = {}; deleted = {}
    try:
        with conn.cursor() as cur:
            identifiers = sql.SQL(', ').join(sql.Identifier(schema, table) for schema, table in TABLES)
            cur.execute(sql.SQL('LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE').format(identifiers))
            cur.execute('SELECT current_database()')
            assert cur.fetchone()[0] == 'connexa_platform_test'
            for schema, table in TABLES:
                name = f'{schema}.{table}'
                cur.execute('SELECT relkind FROM pg_class WHERE oid=to_regclass(%s)', (name,))
                assert cur.fetchone()[0] in ('r', 'p'), 'Foreign tables are forbidden'
                cur.execute("""SELECT count(*) FROM pg_constraint
                    WHERE contype='f' AND confrelid=to_regclass(%s)""", (name,))
                assert cur.fetchone()[0] == 0, f'Unexpected dependency: {name}'
                cur.execute(sql.SQL('SELECT to_jsonb(t) FROM {}.{} t WHERE id::text=ANY(%s) FOR UPDATE').format(
                    sql.Identifier(schema), sql.Identifier(table)), (IDS,))
                before[name] = [r[0] for r in cur.fetchall()]
                # All known copies must reference the diagnosed product(s).
                assert all(r.get('product_id') in (repair.PRODUCT, '1c569c4d-cf19-4a00-a909-bad33068e704')
                           for r in before[name])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({'database': test.dbname, 'host': test.host,
                                       'before': before, 'ids': IDS}, indent=2), encoding='utf-8')
            print('BACKUP', path)
            for schema, table in TABLES:
                cur.execute(sql.SQL('DELETE FROM {}.{} WHERE id::text=ANY(%s)').format(
                    sql.Identifier(schema), sql.Identifier(table)), (IDS,))
                deleted[f'{schema}.{table}'] = cur.rowcount
        validation = repair.validate(conn)
        conn.commit()
        path.with_suffix('.result.json').write_text(json.dumps(
            {'committed': True, 'deleted': deleted, 'validation': validation}, indent=2), encoding='utf-8')
        print('COMMITTED', json.dumps({'deleted': deleted, 'validation': validation}))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
