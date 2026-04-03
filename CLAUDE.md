# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

ETL Script Generator — PostgreSQL 메타 DB를 읽어 PySpark ETL 스크립트를 자동 생성하는 CLI 도구.

## Running

```bash
python create_script.py <table_name>
```

## Dependencies

- `psycopg2` (PostgreSQL 연결)
- Python 3.10+ (`list[dict]` 타입 힌트 사용)

## Architecture

단일 파일(`create_script.py`) 구조로, 5개 섹션이 순서대로 실행됨:

1. **DB 조회** (`execute_query`, `get_metadata`, `get_columns`) — `meta_tables`, `meta_columns` 두 테이블에서 메타 정보 로드. 동일 `table_name`이 여러 행이면 대화형 선택 프롬프트 표시.

2. **공통 섹션 빌더** (`section_import`, `section_schema`, `section_read`, `section_write`, `assemble_script`) — 모든 타입에서 공유하는 PySpark 코드 블록 생성. `assemble_script(meta, schema_cols, extra_with_columns)`가 최종 조립을 담당.

3. **타입 정의** (`SCRIPT_TYPES`) — `ScriptType` 데이터클래스 리스트. 각 항목이 판별 조건(`condition`)과 변환 로직(`transformer`)을 함께 보유. **새 타입 추가 시 이 리스트에만 항목을 추가하면 됨.**

4. **타입 판별 및 생성** (`build_script`) — `SCRIPT_TYPES`를 순서대로 검사해 첫 번째 일치 타입의 `transformer`를 실행 후 `assemble_script`로 조립. `(type_name, script_body)` 반환.

5. **출력** (`main`) — 생성된 스크립트를 메타 정보 요약과 함께 stdout 출력. 파일 저장 없음.

## 타입 목록 및 판별 조건

`SCRIPT_TYPES` 리스트 순서대로 검사되며 첫 번째 일치 타입이 사용됨.

| 타입명 | 판별 조건 | transformer 특징 |
|--------|-----------|-----------------|
| `AREA` | `add_column` 있음 + 첫 컬럼 = `area` | `cols[1:]` 스키마, area 컬럼 추가 |
| `AREA_OLD` | `add_column` 있음 + 첫 컬럼 ≠ `area` | 전체 컬럼 스키마, area 덮어씀 |
| `STRING_PART_Y` | `date_replace = 'y'` | `to_timestamp('yyyyMMdd HHmmss')` → part1 |
| `STRING_PART_W` | `date_replace = 'w'` | `to_timestamp('yyyyMMdd')` → part1 |
| `NOW_OLD` | `date_column = 'now'` | `cols[:-1]` 스키마, 파일명 날짜 → part1 |
| `NOW` | `date_column = 'data_insert_time'` | `cols[1:]` 스키마, 파일명 날짜 → data_insert_time → part1 |
| `PARTITIONED` | `part1` 있음 | `date_col.cast('date')` → part1 |
| `NO_PARTITIONED` | (fallback) | 타입 캐스팅만 수행 |

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
