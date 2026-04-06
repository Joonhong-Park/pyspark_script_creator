# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

ETL Script Generator — PostgreSQL 메타 DB를 읽어 PySpark ETL 스크립트를 자동 생성하는 CLI 도구.

## Running

```bash
python create_script.py <table_name>
# 인자 없이 실행 시 table_name 입력 프롬프트 표시
python create_script.py
```

## Dependencies

- `psycopg2` (PostgreSQL 연결, `RealDictCursor` 사용)
- Python 3.10+

## Architecture

단일 파일(`create_script.py`) 구조로, 6개 섹션이 순서대로 실행됨:

1. **DB 조회** (`execute_query`, `get_metadata`, `get_columns`) — `meta_tables`, `meta_columns` 두 테이블에서 메타 정보 로드. 동일 `table_name`이 여러 행이면 대화형 선택 프롬프트 표시.

2. **공통 섹션 빌더** (`section_import`, `section_schema`, `section_vars`, `section_read`, `section_write`, `assemble_script`) — 모든 타입에서 공유하는 PySpark 코드 블록 생성. 생성 순서: import → schema → vars → read → cast → write. `assemble_script(meta, schema_cols, extra_with_columns)`가 최종 조립을 담당.

3. **타입 정의** (`SCRIPT_TYPES`) — `ScriptType` 데이터클래스 리스트. 각 항목이 판별 조건(`condition`)과 변환 로직(`transformer`)을 함께 보유. **새 타입 추가 시 이 리스트에만 항목을 추가하면 됨.**

4. **타입 판별 및 생성** (`build_script`) — `SCRIPT_TYPES`를 순서대로 검사해 첫 번째 일치 타입의 `transformer`를 실행 후 `assemble_script`로 조립. `(type_name, script_body)` 반환.

5. **출력** (`main`) — 생성된 스크립트를 메타 정보 요약과 함께 stdout 출력. 파일 저장 없음.

## 생성 스크립트 구조

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import ...

spark = SparkSession.builder.appName('db_name.table_name').getOrCreate()

def null_replace(df): ...
def time_replace(c): ...

schema = (
"col1 string,"
"col2 string"
)

source_path = '...'
target_path = '...'
header      = '...'
delimiter   = '...'
[part1, part2, date_col — 해당 시]

df = (spark.read.format('csv')...)
df2 = null_replace(df)

df2 = (df2 .withColumn(...) ...)  # 타입별 캐스팅

(df2.write.mode('append').partitionBy(...).parquet(target_path))
```

## 타입 목록 및 판별 조건

`SCRIPT_TYPES` 리스트 순서대로 검사되며 첫 번째 일치 타입이 사용됨.

| 타입명 | 판별 조건 | transformer 특징 |
|--------|-----------|-----------------|
| `AREA` | `add_column` 있음 + 첫 컬럼 = `area` | `cols[1:]` 스키마, area 컬럼 추가 후 `.select('area', ..., part1)` |
| `AREA_OLD` | `add_column` 있음 + 첫 컬럼 ≠ `area` | 전체 컬럼 스키마, area 덮어씀 |
| `STRING_PART_Y` | `date_replace = 'y'` | `to_timestamp('yyyyMMdd HHmmss').cast('date')` → part1 |
| `STRING_PART_W` | `date_replace = 'w'` | `to_date('yyyyMMdd')` → part1 |
| `NOW_OLD` | `date_column = 'now'` | `cols[:-1]` 스키마, 파일명 날짜 → part1 |
| `NOW` | `date_column = 'data_insert_time'` | `cols[1:]` 스키마, 파일명 날짜+시분 → `data_insert_time`(timestamp) → part1. `.select('data_insert_time', ..., part1)` |
| `PARTITIONED` | `part1` 있음 | `date_col.cast('date')` → part1 |
| `NO_PARTITIONED` | (fallback) | 타입 캐스팅만 수행 |

## 파일명 규칙 (NOW 타입)

`table_name_xxxx_20260101_1330.csv.deflate` 형식에서 날짜+시분 추출:

```python
to_timestamp(concat(
    regexp_extract(input_file_name(), r'_(\d{8})_(\d{4})\.', 1),  # 20260101
    lit(' '),
    regexp_extract(input_file_name(), r'_(\d{8})_(\d{4})\.', 2)   # 1330
), 'yyyyMMdd HHmm')
```

## 새 타입 추가 방법

`SCRIPT_TYPES` 리스트에 `ScriptType` 항목 하나를 추가한다. 판별 순서가 중요하므로 기존 조건과 겹치는 경우 리스트 앞쪽에 배치.

```python
ScriptType(
    name="NEW_TYPE",
    description="설명",
    condition=lambda m, c: <판별 조건>,
    transformer=lambda m, c: (
        <schema_cols>,       # cols 또는 cols[1:] 등
        [<extra .withColumn 문자열들>],
    ),
),
```

## Configuration

`create_script.py` 상단 두 상수를 환경에 맞게 수정:

- `DB_CONFIG` — PostgreSQL 접속 정보
- `DOMAIN_MAP` — `data_domain` / `save_domain` 값을 HDFS URL로 매핑

## Meta DB Schema (참조)

```sql
-- meta_tables
table_id, db_name, table_name, sub_name,
data_domain, data_path, data_subpath, data_filename,
data_delimiter, header_yn,
save_domain, save_path,
partition_name, part_cnt,
date_column, add_column, date_replace

-- meta_columns
table_id, col_name, col_type, sort_idx
```
