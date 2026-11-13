"""Repair the seven diagnosed TEST-only rows; PROD is strictly read-only."""
import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent / 'CNX_DIARCO_SYNC_V2' / 'src'))
from cnx_diarco_sync.config.settings import load_platform_settings
from cnx_diarco_sync.targets.postgres import (
    connect_readonly, discover_profile, fetch_target_barcodes,
    fetch_target_product_suppliers,
)
from cnx_diarco_sync.domain.barcodes import normalize_target_barcode
from cnx_diarco_sync.domain.product_suppliers import normalize_target_product_supplier

BARCODES = ['9b877850-fa84-43a5-b949-9dd707b21a61',
            'a7e4b6a1-2446-4c4c-9695-5afc0850c19b']
INVALID_RELATIONS = ['680d2ff7-8335-4c69-a41c-0fa7733e45ed',
                     '52d4a510-e75d-407a-8075-5f63920f2e59']
DUPLICATE_RELATIONS = ['99635b07-a09c-4396-9691-685eb0aa78bd',
                       'ef43c814-3caf-4982-9811-9d3fc5ac7bc9',
                       '0b0381c1-001d-40db-b641-11947714a667']
PRODUCT = '8ac90f23-e46e-44ad-a005-9bfd021e46c9'


def validate(conn):
    profile = discover_profile(conn, 'modular_inventory_v1')
    barcodes, _, _ = fetch_target_barcodes(conn, profile)
    normalized = [normalize_target_barcode(row) for row in barcodes]
    assert len({r.barcode for r in normalized}) == len(normalized), 'Duplicate barcode'
    relations, _, _ = fetch_target_product_suppliers(conn, profile)
    normalized_relations = [normalize_target_product_supplier(r) for r in relations]
    counts = Counter(r.key for r in normalized_relations)
    assert not any(n > 1 for n in counts.values()), 'Duplicate product/supplier'
    return {'barcodes_normalized': len(normalized),
            'relations_normalized': len(normalized_relations),
            'invalid_barcodes': 0, 'invalid_relations': 0, 'duplicate_relation_groups': 0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    settings = load_platform_settings(ROOT.parent / 'CNX_DIARCO_SYNC_V2' / '.env')
    test = settings.destinations['test'].database
    prod = settings.destinations['prod'].database
    assert test.dbname == 'connexa_platform_test'
    assert prod.dbname == 'connexa_platform_ms'
    assert (test.host, test.port, test.dbname) != (prod.host, prod.port, prod.dbname)
    reference = {}
    with closing(connect_readonly(prod, 15000)) as source:
        with source.cursor() as cur:
            cur.execute("SELECT id FROM inventory.inv_product WHERE id=%s OR sku='22142215'", (PRODUCT,))
            assert not cur.fetchall(), 'TEST product now has a PROD counterpart: re-evaluate'
            cur.execute("SELECT barcode FROM inventory.inv_barcode WHERE barcode=ANY(%s)",
                        (['7789778977896', '7789778978976'],))
            assert not cur.fetchall(), 'Barcodes now exist in PROD: re-evaluate'
            cur.execute("""SELECT to_jsonb(ps) FROM inventory.inv_product_supplier ps
                JOIN inventory.inv_product p ON p.id=ps.product_id
                JOIN inventory.inv_supplier s ON s.id=ps.supplier_id
                WHERE p.ext_code='40' AND s.ext_code='4647'""")
            assert not cur.fetchall(), '40/4647 now exists in PROD: re-evaluate'
            cur.execute("""SELECT to_jsonb(ps) FROM inventory.inv_product_supplier ps
                JOIN inventory.inv_product p ON p.id=ps.product_id
                JOIN inventory.inv_supplier s ON s.id=ps.supplier_id
                WHERE p.ext_code='40' AND s.ext_code='3379'""")
            reference['prod_40_3379'] = [r[0] for r in cur.fetchall()]
            assert len(reference['prod_40_3379']) == 1
    conn = psycopg2.connect(host=test.host, port=test.port, dbname=test.dbname,
                           user=test.user, password=test.password, connect_timeout=10,
                           application_name='etl_diarco:reparar_maestros_test_20261113',
                           options='-c statement_timeout=30000 -c lock_timeout=3000')
    conn.set_session(isolation_level='SERIALIZABLE', readonly=not args.apply)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    backup = ROOT / 'data' / 'repairs' / f'maestros_test_{timestamp}.json'
    report = {'timestamp_utc': timestamp, 'database': test.dbname,
              'host': test.host, 'reference': reference, 'before': {}}
    try:
        with conn.cursor() as cur:
            if args.apply:
                # Acquire writer locks BEFORE the serializable snapshot. Readers
                # remain allowed; lock_timeout prevents waiting behind busy writers.
                cur.execute('LOCK TABLE inventory.inv_barcode, inventory.inv_product_supplier '
                            'IN SHARE ROW EXCLUSIVE MODE')
            cur.execute('SELECT current_database()')
            assert cur.fetchone()[0] == 'connexa_platform_test'
            for table, ids in [('inv_barcode', BARCODES),
                               ('inv_product_supplier', INVALID_RELATIONS + DUPLICATE_RELATIONS)]:
                lock = ' FOR UPDATE' if args.apply else ''
                cur.execute(f'SELECT to_jsonb(t) FROM inventory.{table} t '
                            f'WHERE id=ANY(%s::uuid[]) ORDER BY id{lock}', (ids,))
                rows = [r[0] for r in cur.fetchall()]
                assert {r['id'] for r in rows} == set(ids), f'Unexpected row set: {table}'
                report['before'][table] = rows
            cur.execute('SELECT to_jsonb(p) FROM inventory.inv_product p WHERE id=%s', (PRODUCT,))
            product = cur.fetchone()[0]
            assert not (product.get('ext_code') or '').strip()
            report['test_product_reference'] = product
            assert all(r['product_id'] == PRODUCT for r in report['before']['inv_barcode'])
            assert all(r['product_id'] == PRODUCT for r in report['before']['inv_product_supplier']
                       if r['id'] in INVALID_RELATIONS)
            cur.execute("""SELECT ps.id::text FROM inventory.inv_product_supplier ps
                JOIN inventory.inv_product p ON p.id=ps.product_id
                JOIN inventory.inv_supplier s ON s.id=ps.supplier_id
                WHERE p.ext_code='40' AND s.ext_code='4647'""")
            assert {r[0] for r in cur.fetchall()} == set(DUPLICATE_RELATIONS)
            cur.execute('SELECT id FROM inventory.inv_barcode WHERE parent_id=ANY(%s::uuid[])', (BARCODES,))
            assert not cur.fetchall(), 'Unexpected dependent barcodes; do not cascade'
            cur.execute("""SELECT ns.nspname, c.relname, con.conname
                FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
                JOIN pg_namespace ns ON ns.oid=c.relnamespace
                WHERE con.contype='f' AND con.confrelid IN
                  ('inventory.inv_barcode'::regclass,'inventory.inv_product_supplier'::regclass)
                AND NOT (con.confrelid='inventory.inv_barcode'::regclass
                         AND con.conrelid='inventory.inv_barcode'::regclass)""")
            assert not cur.fetchall(), 'New inbound FK: review dependencies'
            cur.execute("""SELECT count(*) FROM inventory.inv_product_supplier ps
                JOIN inventory.inv_product p ON p.id=ps.product_id
                JOIN inventory.inv_supplier s ON s.id=ps.supplier_id
                WHERE p.ext_code='40' AND s.ext_code='3379'""")
            assert cur.fetchone()[0] == 1, 'Missing canonical PROD relation in TEST'
            backup.parent.mkdir(parents=True, exist_ok=True)
            backup.write_text(json.dumps(report, indent=2, ensure_ascii=True, default=str), encoding='utf-8')
            print('BACKUP', backup)
            if not args.apply:
                conn.rollback()
                print('PLAN_ONLY: 2 barcodes and 5 relations; no changes')
                return
            cur.execute('DELETE FROM inventory.inv_barcode WHERE id=ANY(%s::uuid[])', (BARCODES,))
            assert cur.rowcount == 2
            cur.execute('DELETE FROM inventory.inv_product_supplier WHERE id=ANY(%s::uuid[])',
                        (INVALID_RELATIONS + DUPLICATE_RELATIONS,))
            assert cur.rowcount == 5
        result = validate(conn)
        conn.commit()
        report['committed'] = True
        report['deleted'] = {'inv_barcode': 2, 'inv_product_supplier': 5}
        report['validation'] = result
        backup.with_suffix('.result.json').write_text(
            json.dumps(report, indent=2, ensure_ascii=True, default=str), encoding='utf-8')
        print('COMMITTED', json.dumps(result))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
