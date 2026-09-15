"""Rellenar fechas faltantes del histórico desde T710, sin sobrescribir historia.

Plan por defecto. --apply confirma por año/mes/sucursal y exige capacidad verificada.
Conexión explícita por archivo .env/prefijo; no importa flujos Prefect.
"""
import argparse
import calendar
from datetime import date, timedelta, datetime, timezone
import json
from pathlib import Path

import psycopg2
from dotenv import dotenv_values


def source_plan(cur, start, end):
    cur.execute("""SELECT c_anio::integer,c_mes::integer,c_sucu_empr::integer,count(*)
                   FROM src.t710_estadis_stock
                   WHERE c_anio * 100 + c_mes BETWEEN %s AND %s
                   GROUP BY 1,2,3 ORDER BY 1,2,3""",
                (start.year * 100 + start.month, end.year * 100 + end.month))
    return cur.fetchall()


def reconstruct_batch(conn, year, month, site, start, end):
    columns = ','.join(f's.q_dia{i}' for i in range(1,32))
    first = date(year,month,1)
    last = date(year,month,calendar.monthrange(year,month)[1])
    with conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout='10s'")
        cur.execute("SET LOCAL statement_timeout='15min'")
        # Permite lectores y serializa escritores durante el lote.
        cur.execute('LOCK TABLE src.historico_stock_sucursal IN SHARE ROW EXCLUSIVE MODE')
        cur.execute(f"""CREATE TEMP TABLE stock_rebuild_batch ON COMMIT DROP AS
            SELECT s.c_articulo AS articulo,s.c_sucu_empr AS sucursal,
                   u.cantidad,%s::date + (u.d::integer-1) AS fecha_stock
            FROM src.t710_estadis_stock s
            CROSS JOIN LATERAL unnest(ARRAY[{columns}]) WITH ORDINALITY u(cantidad,d)
            WHERE s.c_anio=%s AND s.c_mes=%s AND s.c_sucu_empr=%s
              AND u.d <= %s
              AND %s::date + (u.d::integer-1) BETWEEN %s AND %s""",
                    (first,year,month,site,last.day,first,start,end))
        cur.execute('SELECT count(*) FROM stock_rebuild_batch')
        expected = cur.fetchone()[0]
        if not expected:
            raise ValueError('La fuente perdio el lote planificado')
        cur.execute('SELECT 1 FROM stock_rebuild_batch GROUP BY fecha_stock,articulo,sucursal HAVING count(*)>1 LIMIT 1')
        if cur.fetchone():
            raise ValueError('Claves duplicadas en origen')
        cur.execute("""SELECT 1 FROM stock_rebuild_batch
                       WHERE cantidad IS NULL OR cantidad::text IN ('NaN','Infinity','-Infinity') LIMIT 1""")
        if cur.fetchone():
            raise ValueError('Cantidad faltante/no finita')
        cur.execute('CREATE UNIQUE INDEX ON stock_rebuild_batch(fecha_stock,articulo,sucursal)')
        cur.execute('ANALYZE stock_rebuild_batch')
        cur.execute("""INSERT INTO src.historico_stock_sucursal
            (anio,mes,dia,sucursal,articulo,cantidad,fecha_stock,fecha_procesos,procesado)
            SELECT %s,%s,extract(day from s.fecha_stock)::integer,s.sucursal,s.articulo,
                   s.cantidad,s.fecha_stock,clock_timestamp(),false
            FROM stock_rebuild_batch s
            ON CONFLICT (fecha_stock,articulo,sucursal) DO NOTHING""",(year,month))
        inserted = cur.rowcount
        cur.execute("""SELECT count(*),count(*) FILTER (
                           WHERE h.articulo IS NULL OR h.cantidad IS DISTINCT FROM s.cantidad
                           OR h.anio IS DISTINCT FROM %s OR h.mes IS DISTINCT FROM %s
                           OR h.dia IS DISTINCT FROM extract(day from s.fecha_stock)::integer)
                       FROM stock_rebuild_batch s
                       LEFT JOIN src.historico_stock_sucursal h
                         ON h.fecha_stock=s.fecha_stock AND h.articulo=s.articulo AND h.sucursal=s.sucursal""",
                    (year,month))
        checked,differences=cur.fetchone()
        if checked!=expected or differences:
            raise ValueError('Diferencias en registros historicos: rollback del lote')
    conn.commit()
    return dict(year=year,month=month,site=site,source_rows=expected,inserted=inserted,
                existing_equal=expected-inserted,differences=0)


def ensure_index(conn):
    # Preparación explícita para idempotencia y acceso por fecha; no elimina datos.
    conn.commit()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET lock_timeout='10s'")
        cur.execute("SET statement_timeout='30min'")
        cur.execute('''CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS
                       ux_historico_stock_fecha_articulo_sucursal
                       ON src.historico_stock_sucursal(fecha_stock,articulo,sucursal)''')
        cur.execute('''SELECT i.indisvalid,i.indisunique,
                       ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY k(attnum,ord)
                             JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
                             ORDER BY k.ord), i.indpred IS NULL
                       FROM pg_index i WHERE i.indexrelid=
                       'src.ux_historico_stock_fecha_articulo_sucursal'::regclass
                       AND i.indrelid='src.historico_stock_sucursal'::regclass''')
        if cur.fetchone() != (True, True, ['fecha_stock','articulo','sucursal'], True):
            raise ValueError("Índice incompatible o inválido; requiere revisión, no se elimina automáticamente")
    conn.autocommit = False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env', type=Path, required=True)
    parser.add_argument('--prefix', default='PG')
    parser.add_argument('--from-date', type=date.fromisoformat, required=True)
    parser.add_argument('--through-date', type=date.fromisoformat, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--capacity-verified', action='store_true')
    args = parser.parse_args()
    if args.from_date > args.through_date or args.through_date >= date.today():
        parser.error('Rango inválido: corte cerrado explícito anterior a hoy')
    if args.apply and not args.capacity_verified:
        parser.error('Verificar capacidad de disco para datos, índice, temporales y WAL antes de aplicar')
    if args.report.exists():
        parser.error('Usar un reporte nuevo; los anteriores se conservan')
    values = dotenv_values(args.env, interpolate=False)
    kwargs = {name: values.get(args.prefix+'_'+key) for name,key in
              [('host','HOST'),('port','PORT'),('dbname','DB'),('user','USER'),('password','PASSWORD')]}
    if not all(kwargs.values()):
        parser.error('Bloque de conexión incompleto')
    conn = None
    report = dict(start=args.from_date.isoformat(), cutoff=args.through_date.isoformat(),
                  apply=args.apply, started_at=datetime.now(timezone.utc).isoformat(),
                  status='RUNNING' if args.apply else 'PLANNING', batches=[])

    def save():
        args.report.parent.mkdir(parents=True, exist_ok=True)
        tmp = args.report.with_suffix(args.report.suffix+'.tmp')
        tmp.write_text(json.dumps(report,indent=2),encoding='utf-8')
        tmp.replace(args.report)

    try:
        conn = psycopg2.connect(**kwargs,connect_timeout=10,application_name='stock_history_rebuild')
        conn.set_session(readonly=not args.apply)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='60s'")
            batches = source_plan(cur,args.from_date,args.through_date)
        conn.commit()
        present = set()
        expected_rows=0
        for year,month,site,rows in batches:
            first=max(args.from_date,date(year,month,1))
            last=min(args.through_date,date(year,month,calendar.monthrange(year,month)[1]))
            span=(last-first).days+1
            expected_rows+=rows*span
            present.update(first+timedelta(days=i) for i in range(span))
        requested = [args.from_date+timedelta(days=i) for i in range((args.through_date-args.from_date).days+1)]
        report.update(source_daily_rows=expected_rows,
                      missing_source_dates=[d.isoformat() for d in requested if d not in present],
                      source_dates=len(present), planned_batches=len(batches))
        save()
        print(json.dumps({k:report[k] for k in ('source_daily_rows','source_dates','missing_source_dates')}),flush=True)
        if args.apply:
            ensure_index(conn)
            for year,month,site,count in batches:
                result = reconstruct_batch(conn,year,month,site,args.from_date,args.through_date)
                report['batches'].append(result)
                save()
                print(json.dumps(result),flush=True)
        report['status']='COMPLETED' if args.apply else 'PLANNED'
        save()
    except KeyboardInterrupt:
        if conn is not None:
            conn.rollback()
        report.update(status='INTERRUPTED',stopped_at=datetime.now(timezone.utc).isoformat())
        save()
        parser.exit(130,'Carga interrumpida; lotes confirmados conservados.\n')
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        report.update(status='FAILED',error_type=type(exc).__name__)
        save()
        # No imprimir excepción del driver: puede contener datos de conexión.
        parser.exit(2, f'Proceso detenido ({type(exc).__name__}); revisar reporte y lotes confirmados.\n')
    finally:
        if conn is not None:
            conn.close()


if __name__=='__main__':
    main()
