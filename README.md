# PySpark ETL Script Creator

PostgreSQL 메타 DB를 읽어 PySpark ETL 스크립트를 자동 생성하는 CLI 도구.

## Requirements

- Python 3.10+
- `psycopg2`

```bash
pip install psycopg2-binary
```

## Configuration

`create_script.py` 상단의 두 상수를 환경에 맞게 수정한다.

```python
DB_CONFIG = {
    "host"    : "localhost",
    "database": "meta_db",
    "user"    : "admin",
    "password": "password",
    "port"    : 5432,
}

DOMAIN_MAP = {
    "domain1": "hdfs://abc.def.com",
    # data_domain / save_domain 값 → HDFS URL
}
```

## Usage

```bash
# 테이블명을 인자로 전달
python create_script.py <table_name>

# 인자 없이 실행하면 프롬프트 표시
python create_script.py
```

동일한 `table_name`이 메타 DB에 여러 개 존재하면 번호 선택 프롬프트가 표시된다.

## Output

stdout에 메타 정보 요약과 생성된 PySpark 스크립트를 출력한다. 파일로 저장하려면 리다이렉트를 사용한다.

```bash
python create_script.py my_table > my_table_etl.py
```

출력 스크립트 구조:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import ...

spark = SparkSession.builder.appName('db_name.table_name').getOrCreate()

def null_replace(df): ...
def time_replace(c): ...

schema = ("col1 string," "col2 string")

source_path = '...'
target_path = '...'
header      = '...'
delimiter   = '...'

df = (spark.read.format('csv')...)
df2 = null_replace(df)

df2 = (df2.withColumn(...) ...)   # 타입별 캐스팅

(df2.write.mode('append').partitionBy(...).parquet(target_path))
```

## Script Types

메타 정보에 따라 아래 타입 중 하나가 자동 선택된다 (위에서부터 우선 검사).

| 타입 | 판별 조건 | 특징 |
|------|-----------|------|
| `AREA` | `add_column` 있음 + 첫 컬럼 = `area` | area 컬럼 추가, date_col → part1 |
| `AREA_OLD` | `add_column` 있음 + 첫 컬럼 ≠ `area` | 기존 area 컬럼 덮어씀 |
| `STRING_PART_Y` | `date_replace = 'y'` | `yyyyMMdd HHmmss` 문자열 → part1 |
| `STRING_PART_W` | `date_replace = 'w'` | `yyyyMMdd` 문자열 → part1 |
| `NOW_OLD` | `date_column = 'now'` | 파일명에서 날짜(8자리) 추출 → part1 |
| `NOW` | `date_column = 'data_insert_time'` | 파일명에서 날짜+시분 추출 → data_insert_time → part1 |
| `PARTITIONED` | `part1` 있음 | date_col.cast('date') → part1 |
| `NO_PARTITIONED` | (fallback) | 타입 캐스팅만 수행 |

## Meta DB Schema

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
