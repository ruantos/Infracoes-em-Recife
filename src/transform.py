import pandas as pd
import duckdb


def remove_garbage(df: duckdb.DuckDBPyRelation) -> pd.DataFrame:
    df = df.drop_duplicates(
        subset=["infracao", "hora", "ano", "mes"])

    df = df.dropna(axis=0, subset=["infracao"])
    return df


def fix_columns(df: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    return duckdb.sql("""
        SELECT 
            AgenteEquipamento,
            Infracao,
            LocalCometimento,
            YEAR( CAST(datainfracao AS DATE) ) AS ano,
            MONTH( CAST(datainfracao AS DATE) ) AS mes,
            HOUR( CAST(horainfracao AS TIME) ) AS hora,
            ISODOW( CAST(datainfracao AS DATE)) > 5 AS is_feriado 
        FROM df
    """)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = fix_columns(df)
    df = remove_garbage(df)
    return df