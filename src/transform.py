import duckdb
from pandas import DataFrame


def remove_garbage(relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    duckdb.sql("""
            WITH duplicatedRows AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY infracao, hora, ano, mes) as n_row 
                FROM relation
                WHERE infracao IS NOT NULL)
                
            SELECT * EXCLUDE (n_row)
            FROM duplicatedRows
            WHERE n_row = 1;
            """)

    return relation


def fix_columns(relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    return duckdb.sql("""
        SELECT 
            AgenteEquipamento,
            Infracao,
            LocalCometimento,
            YEAR( CAST(datainfracao AS DATE) ) AS ano,
            MONTH( CAST(datainfracao AS DATE) ) AS mes,
            HOUR( CAST(horainfracao AS TIME) ) AS hora,
            ISODOW( CAST(datainfracao AS DATE)) > 5 AS is_feriado 
        FROM relation
    """)


def transform(df: DataFrame) -> duckdb.DuckDBPyRelation:
    relation = duckdb.from_df(df)
    relation = fix_columns(relation)
    relation = remove_garbage(relation)

    return relation