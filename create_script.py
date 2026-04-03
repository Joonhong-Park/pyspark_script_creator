"""
ETL Script Generator v2.1
- 메타 DB(PostgreSQL) 기반 PySpark ETL 스크립트 자동 생성
- 타입 정의 · 판별 · 빌드 로직을 SCRIPT_TYPES 한 곳에서 관리
"""

import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dataclasses import dataclass
from typing import Callable

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# [0] 설정
# ──────────────────────────────────────────────
DB_CONFIG = {
    "host"    : "localhost",
    "database": "meta_db",
    "user"    : "admin",
    "password": "password",
    "port"    : 5432,
}

DOMAIN_MAP = {
    "domain1": "hdfs://abc.def.com",
    "domain2": "hdfs://ghi.jkl.com",
    "domain3": "hdfs://mno.pqr.com",
}


# =============================================================================
# [1] DB 조회
# =============================================================================

def execute_query(sql):
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(row) for row in cur.fetchall()]
    except psycopg2.Error as e:
        log.error(f"DB Error: {e}")
        sys.exit(1)


def get_metadata(table_name):
    rows = execute_query(
        f"""
        SELECT table_id, db_name, table_name, sub_name,
               data_domain, data_path, data_subpath, data_filename,
               data_delimiter, header_yn, save_domain, save_path,
               partition_name, part_cnt, date_column, add_column, date_replace
          FROM meta_tables
         WHERE table_name = '{table_name}'
         ORDER BY table_id
        """
    )

    if not rows:
        log.error(f"'{table_name}' 을 찾을 수 없습니다.")
        sys.exit(1)

    if len(rows) == 1:
        row = rows[0]
    else:
        print(f"\n'{table_name}' 에 해당하는 테이블이 {len(rows)}개 있습니다:\n")
        print(f"  {'No':<5} {'table_id':<12} {'db_name':<15} {'table_name':<20} {'sub_name'}")
        print(f"  {'-'*5} {'-'*12} {'-'*15} {'-'*20} {'-'*15}")
        for i, r in enumerate(rows, 1):
            print(f"  {i:<5} {str(r['table_id']):<12} {str(r['db_name']):<15}"
                  f" {str(r['table_name']):<20} {str(r['sub_name'])}")
        while True:
            try:
                sel = int(input("\n선택 (번호): "))
                if 1 <= sel <= len(rows):
                    row = rows[sel - 1]
                    break
                print(f"  1 ~ {len(rows)} 사이의 번호를 입력하세요.")
            except ValueError:
                print("  숫자를 입력하세요.")

    # 필수 컬럼 검증
    required = ["table_id", "table_name"]
    missing = [c for c in required if not row.get(c)]
    if missing:
        log.error(f"필수 메타 정보 누락: {missing}")
        sys.exit(1)

    # 선택 컬럼 정규화 (NULL / 빈값 → None)
    for key in ["add_column", "date_replace", "date_column", "partition_name"]:
        row[key] = row[key] or None

    # 파티션 컬럼 파싱
    part_cnt  = int(row["part_cnt"]) if row.get("part_cnt") else 0
    part_list = [p.strip() for p in (row["partition_name"] or "").split(",") if p.strip()]
    row["part1"] = part_list[0] if part_cnt >= 1 and len(part_list) >= 1 else None
    row["part2"] = part_list[1] if part_cnt >= 2 and len(part_list) >= 2 else None

    # 경로 조합
    source_domain = DOMAIN_MAP.get(row["data_domain"])
    target_domain = DOMAIN_MAP.get(row["save_domain"])
    row["source_path"] = "/".join(
        p.strip("/") for p in [
            source_domain, row["data_path"], row["data_subpath"], row["data_filename"],
        ] if p
    )
    row["target_path"] = "/".join(
        p.strip("/") for p in [
            target_domain, row["save_path"], row["table_name"],
        ] if p
    )

    log.info(f"테이블 로드 완료 | table_id={row['table_id']} | part1={row['part1']} | part2={row['part2']}")
    return row


def get_columns(table_id):
    cols = execute_query(
        f"SELECT col_name, col_type FROM meta_columns WHERE table_id = '{table_id}' ORDER BY sort_idx"
    )
    if not cols:
        log.error(f"table_id={table_id} 에 해당하는 컬럼이 없습니다.")
        sys.exit(1)

    log.info(f"컬럼 {len(cols)}개 로드 완료")
    return [{"col_name": c["col_name"], "col_type": c["col_type"]} for c in cols]


# =============================================================================
# [2] 공통 섹션 빌더
# =============================================================================

def section_import(meta):
    lines = [
        "from pyspark.sql.functions import col, lit, when, regexp_replace, to_timestamp, to_date, regexp_extract, input_file_name",
        "", "",
        "def null_replace(df):",
        "    exprs = [",
        "        when(col(c).isin('null', 'NULL'), None).otherwise(col(c)).alias(c)",
        "        for c in df.columns",
        "    ]",
        "    return df.select(*exprs)",
        "", "",
        "def time_replace(c):",
        "    return regexp_replace(c, '/', '-')",
    ]
    return "\n".join(lines)


def section_vars(meta):
    header_val = "true" if (meta.get("header_yn") or "").lower() == "y" else "false"
    lines = [
        f"source_path = '{meta['source_path']}'",
        f"target_path = '{meta['target_path']}'",
        f"header      = '{header_val}'",
        f"delimiter   = '{meta['data_delimiter']}'",
    ]
    if meta.get("part1"):
        lines.append(f"part1       = '{meta['part1']}'")
    if meta.get("part2"):
        lines.append(f"part2       = '{meta['part2']}'")
    if meta.get("date_column"):
        lines.append(f"date_col    = '{meta['date_column']}'")
    return "\n".join(lines)


def section_schema(cols):
    lines = []
    for i, c in enumerate(cols):
        comma = "," if i < len(cols) - 1 else ""
        lines.append(f'    "{c["col_name"]} string{comma}"')
    return "schema = (\n" + "\n".join(lines) + "\n)"


def section_read():
    return (
        "df = (\n"
        "    spark.read\n"
        "    .format('csv')\n"
        "    .option('header', header)\n"
        "    .option('delimiter', delimiter)\n"
        "    .option('inferSchema', 'false')\n"
        "    .schema(schema)\n"
        "    .load(source_path)\n"
        ")\n"
        "df2 = null_replace(df)"
    )


def section_write(meta):
    lines = ["(", "    df2.write", "    .mode('append')"]
    if meta.get("part2"):
        lines.append("    .partitionBy(part1, part2)")
    elif meta.get("part1"):
        lines.append("    .partitionBy(part1)")
    lines.append("    .parquet(target_path)")
    lines.append(")")
    return "\n".join(lines)


def _cast_exprs(cols):
    """string 제외, 타입별 캐스팅 표현식 → .withColumn 문자열 리스트 반환"""
    result = []
    for c in cols:
        if c["col_type"] == "string":
            continue
        logic = f"col('{c['col_name']}')"
        if c["col_type"] == "timestamp":
            logic = f"time_replace({logic})"
        result.append(f"    .withColumn('{c['col_name']}', {logic}.cast('{c['col_type']}'))")
    return result


def assemble_script(meta, schema_cols, extra_with_columns):
    """공통 스크립트 조립. 타입별 차이는 schema_cols와 extra_with_columns만."""
    with_columns = _cast_exprs(schema_cols) + extra_with_columns
    sections = [
        section_import(meta),
        section_schema(schema_cols),
        section_vars(meta),
        section_read(),
    ]
    if with_columns:
        cast_block = ["df2 = (", "    df2"] + with_columns + [")"]
        sections.append("\n".join(cast_block))
    sections.append(section_write(meta))
    return "\n\n\n".join(sections) + "\n"


# =============================================================================
# [3] 타입별 조건(condition) · 변환(transformer) 함수
#
# condition   : (meta, cols) → bool         — 이 타입으로 판별할 조건
# transformer : (meta, cols) → (schema_cols, extra_with_columns)
#                schema_cols        : 스키마·캐스팅에 사용할 컬럼 목록
#                extra_with_columns : _cast_exprs 이후에 붙을 .withColumn 문자열 목록
# =============================================================================

# ── AREA ──────────────────────────────────────────────────────────────────────
def _cond_area(m, c):
    return bool(m["add_column"]) and c[0]["col_name"] == "area"

def _trans_area(m, c):
    col_names = ", ".join(f"'{col['col_name']}'" for col in c[1:])
    extra = [
        f"    .withColumn('area', lit('{m['add_column']}'))",
        "    .withColumn(part1, col(date_col).cast('date'))",
        f"    .select('area', {col_names}, part1)",
    ]
    return c[1:], extra


# ── AREA_OLD ──────────────────────────────────────────────────────────────────
def _cond_area_old(m, c):
    return bool(m["add_column"]) and c[0]["col_name"] != "area"

def _trans_area_old(m, c):
    extra = [
        f"    .withColumn('area', lit('{m['add_column']}'))",
        "    .withColumn(part1, col(date_col).cast('date'))",
    ]
    return c, extra


# ── STRING_PART_Y ─────────────────────────────────────────────────────────────
def _cond_string_part_y(m, c):
    return m["date_replace"] == "y"

def _trans_string_part_y(m, c):
    extra = [
        "    .withColumn(part1, to_timestamp(col(date_col), 'yyyyMMdd HHmmss').cast('date'))",
    ]
    return c, extra


# ── STRING_PART_W ─────────────────────────────────────────────────────────────
def _cond_string_part_w(m, c):
    return m["date_replace"] == "w"

def _trans_string_part_w(m, c):
    extra = [
        "    .withColumn(part1, to_date(col(date_col), 'yyyyMMdd'))",
    ]
    return c, extra


# ── NOW_OLD ───────────────────────────────────────────────────────────────────
def _cond_now_old(m, c):
    return m["date_column"] == "now"

def _trans_now_old(m, c):
    extra = [
        "    .withColumn('part1', to_date(regexp_extract(input_file_name(), r'_(\\d{8})_', 1), 'yyyyMMdd'))",
    ]
    return c[:-1], extra


# ── NOW ───────────────────────────────────────────────────────────────────────
def _cond_now(m, c):
    return m["date_column"] == "data_insert_time"

def _trans_now(m, c):
    col_names = ", ".join(f"'{col['col_name']}'" for col in c[1:])
    extra = [
        "    .withColumn('data_insert_time', to_date(regexp_extract(input_file_name(), r'_(\\d{8})_', 1), 'yyyyMMdd'))",
        "    .withColumn(part1, col('data_insert_time').cast('date'))",
        f"    .select('data_insert_time', {col_names}, part1)",
    ]
    return c[1:], extra


# ── PARTITIONED ───────────────────────────────────────────────────────────────
def _cond_partitioned(m, c):
    return bool(m["part1"])

def _trans_partitioned(m, c):
    extra = [
        "    .withColumn(part1, col(date_col).cast('date'))",
    ]
    return c, extra


# ── NO_PARTITIONED (fallback) ─────────────────────────────────────────────────
def _cond_no_partitioned(m, c):
    return True

def _trans_no_partitioned(m, c):
    return c, []


# =============================================================================
# [4] 타입 정의 — 새 타입 추가 시 SCRIPT_TYPES 에 항목만 추가
# =============================================================================

@dataclass
class ScriptType:
    name:        str
    description: str
    condition:   Callable[[dict, list], bool]
    transformer: Callable[[dict, list], tuple]


SCRIPT_TYPES = [
    ScriptType(
        name="AREA",
        description="첫 컬럼(area) 제외 후 스키마 구성, add_column 값을 area 컬럼으로 추가, date_col → part1",
        condition=_cond_area,
        transformer=_trans_area,
    ),
    ScriptType(
        name="AREA_OLD",
        description="전체 컬럼 스키마, add_column 값으로 기존 area 컬럼 덮어씀, date_col → part1",
        condition=_cond_area_old,
        transformer=_trans_area_old,
    ),
    ScriptType(
        name="STRING_PART_Y",
        description="날짜 컬럼이 'yyyyMMdd HHmmss' 문자열 형식, to_timestamp 파싱 → part1",
        condition=_cond_string_part_y,
        transformer=_trans_string_part_y,
    ),
    ScriptType(
        name="STRING_PART_W",
        description="날짜 컬럼이 'yyyyMMdd' 문자열 형식, to_date 파싱 → part1",
        condition=_cond_string_part_w,
        transformer=_trans_string_part_w,
    ),
    ScriptType(
        name="NOW_OLD",
        description="마지막 컬럼(part1) 제외 후 스키마 구성, 파일명에서 날짜(yyyyMMdd) 추출 → part1",
        condition=_cond_now_old,
        transformer=_trans_now_old,
    ),
    ScriptType(
        name="NOW",
        description="첫 컬럼(data_insert_time) 제외 후 스키마 구성, 파일명 날짜 → data_insert_time → part1",
        condition=_cond_now,
        transformer=_trans_now,
    ),
    ScriptType(
        name="PARTITIONED",
        description="파티션 컬럼 있음, date_col을 date 타입으로 캐스팅 → part1",
        condition=_cond_partitioned,
        transformer=_trans_partitioned,
    ),
    ScriptType(
        name="NO_PARTITIONED",
        description="파티션 없음, 타입 캐스팅만 수행 (fallback)",
        condition=_cond_no_partitioned,
        transformer=_trans_no_partitioned,
    ),
]


# =============================================================================
# [5] 타입 판별 및 스크립트 생성
# =============================================================================

def build_script(meta, cols):
    """SCRIPT_TYPES 순서대로 조건을 검사해 첫 번째 일치 타입으로 스크립트 생성.
    반환값: (type_name, script_body)
    """
    for script_type in SCRIPT_TYPES:
        if script_type.condition(meta, cols):
            schema_cols, extra_wc = script_type.transformer(meta, cols)
            return script_type.name, assemble_script(meta, schema_cols, extra_wc)
    raise ValueError("판별 가능한 타입이 없습니다.")  # NO_PARTITIONED가 fallback이므로 실제론 도달 불가


# =============================================================================
# [6] 실행
# =============================================================================

def main():
    if len(sys.argv) < 2:
        table_name = input("table_name: ").strip()
        if not table_name:
            log.error("테이블명을 입력하세요.")
            sys.exit(1)
    else:
        table_name = sys.argv[1]
    meta                = get_metadata(table_name)
    cols                = get_columns(meta["table_id"])
    script_type, script = build_script(meta, cols)

    print("\n" + "=" * 80)
    print(f"  table_id    : {meta['table_id']}")
    print(f"  db.name     : {meta['db_name']}.{meta['table_name']}")
    print(f"  sub_name    : {meta['sub_name']}")
    print(f"  source_path : {meta['source_path']}")
    print(f"  target_path : {meta['target_path']}")
    print(f"  type        : {script_type}")
    print("=" * 80 + "\n")
    print(script)
    print("=" * 80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted.")
    except Exception as e:
        log.exception(f"Unexpected Error: {e}")
