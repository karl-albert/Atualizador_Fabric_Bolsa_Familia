# ==================================================================================================
# PROJETO: BOLSA FAMÍLIA ANALYTICS — MICROSOFT FABRIC
# NOTEBOOK: NT_UPDATE_Fatos_Mensal (CARGA INCREMENTAL RESILIENTE DIRECT LAKE)
# ==================================================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import unicodedata
import os

# 1. Parâmetros de Governança
LAKEHOUSE_NAME = "LH_Bolsa_Familia"
DELTA_TABLE_REDZ = "fato_bolsa_familia_redz"

print(f"[*] Iniciando rotina incremental resiliente no Lakehouse: {LAKEHOUSE_NAME}")

def normalizar_coluna(col_name):
    n = unicodedata.normalize('NFKD', col_name).encode('ASCII', 'ignore').decode('ASCII')
    return n.strip().lower().replace(' ', '_').replace('-', '_')

# 2. Busca Dinâmica e Inteligente de Arquivos de FATO (Ignora pastas de Dimensões)
arquivos_fato = []

# Tentativa via mssparkutils (OneLake nativo do Fabric)
try:
    from notebookutils import mssparkutils
    def scan_pasta(caminho):
        for item in mssparkutils.fs.ls(caminho):
            nome_low = item.name.lower()
            # Ignora pastas de dimensões, backups e metadados
            if any(ign in nome_low for ign in ["dimens", "dim_", "populacao", "unificado", "_delta_log", ".checkpoint"]):
                continue
            if item.isDir:
                scan_pasta(item.path)
            elif item.name.endswith(".parquet"):
                arquivos_fato.append(item.path)
    scan_pasta("Files")
except Exception:
    pass

# Fallback via sistema de arquivos local
if not arquivos_fato and os.path.exists("/lakehouse/default/Files"):
    for root, dirs, files in os.walk("/lakehouse/default/Files"):
        r_low = root.lower()
        if any(ign in r_low for ign in ["dimens", "dim_", "populacao", "unificado", "_delta_log"]):
            continue
        for f in files:
            f_low = f.lower()
            if f_low.endswith(".parquet") and not any(ign in f_low for ign in ["dimens", "dim_", "populacao"]):
                full_p = os.path.join(root, f)
                rel_p = os.path.relpath(full_p, "/lakehouse/default").replace("\\", "/")
                arquivos_fato.append(rel_p)

# Priorização: se houver arquivo reduzido (redz), usa apenas ele (processa em 10 segundos!)
arquivos_redz = [a for a in arquivos_fato if "redz" in a.lower()]
if arquivos_redz:
    print(f"[⚡ PRIORIDADE] Arquivo reduzido detectado! Usando versão pré-agregada: {arquivos_redz}")
    arquivos_para_ler = arquivos_redz
else:
    arquivos_para_ler = arquivos_fato

print(f"[*] Arquivo(s) de FATO selecionado(s) para carga: {arquivos_para_ler}")

if not arquivos_para_ler:
    print("\n" + "="*80)
    print("❌ [ALERTA] Nenhum arquivo .parquet de FATOS encontrado na pasta 'Files'!")
    print("Certifique-se de enviar o arquivo (ex: 202608_fato_bolsa_familia_redz.parquet) para a pasta Files.")
    print("="*80 + "\n")
    raise FileNotFoundError("Nenhum arquivo Parquet de fatos encontrado!")

# 3. Leitura dos Arquivos de Fato
df_raw = spark.read.parquet(*arquivos_para_ler)

# Normalizar colunas (imune a acentos do governo)
print(f"[*] Colunas originais encontradas no arquivo:")
print("   ", df_raw.columns)

df_norm = df_raw
for c in df_raw.columns:
    df_norm = df_norm.withColumnRenamed(c, normalizar_coluna(c))

colunas_normalizadas = df_norm.columns
print(f"[*] Colunas normalizadas (sem acentos/espaços):")
print("   ", colunas_normalizadas)

if "codigo_municipio_siafi" in colunas_normalizadas and "cod_municipio_siafi" not in colunas_normalizadas:
    df_norm = df_norm.withColumnRenamed("codigo_municipio_siafi", "cod_municipio_siafi")

if "mes_referencia" in colunas_normalizadas and "mes_competencia" not in colunas_normalizadas:
    df_norm = df_norm.withColumnRenamed("mes_referencia", "mes_competencia")

# 4. Transformação Analítica (Resiliente: Detecta se é Reduzido ou Bruto)
if "total_beneficiarios" in df_norm.columns:
    print("[INFO] Arquivo detectado como PARQUET REDUZIDO (já pré-agregado).")
    
    df_fato_redz_mensal = df_norm.select(
        F.col("data").cast(DateType()).alias("Data") if "data" in df_norm.columns else F.to_date(F.concat(F.col("mes_competencia").cast(StringType()), F.lit("01")), "yyyyMMdd").alias("Data"),
        F.col("genero").cast(StringType()).alias("Genero"),
        F.col("mes_competencia").cast(IntegerType()).alias("mes_competencia"),
        F.col("nome_municipio").cast(StringType()).alias("nome_municipio"),
        F.coalesce(F.col("cod_municipio_siafi"), F.col("codigo_municipio_siafi")).cast(StringType()).cast(IntegerType()).alias("Cod_Municipio_Siafi"),
        F.col("total_beneficiarios").cast(LongType()).alias("total_beneficiarios"),
        F.col("uf").cast(StringType()).alias("uf"),
        F.col("valor_total_pago").cast(DecimalType(18, 2)).alias("valor_total_pago"),
        F.col("valor_total_pago_novo").cast(DecimalType(18, 2)).alias("valor_total_pago_novo") if "valor_total_pago_novo" in df_norm.columns else F.when(F.col("mes_competencia") >= 202607, F.round(F.col("valor_total_pago") * 1.1504, 2)).otherwise(F.col("valor_total_pago")).alias("valor_total_pago_novo")
    )
else:
    print("[INFO] Arquivo detectado como PARQUET BRUTO NOMINAL. Executando agregação analítica no Spark...")
    
    df_transformed = df_norm.select(
        F.col("mes_competencia").cast(IntegerType()).alias("mes_competencia"),
        F.col("uf").cast(StringType()).alias("uf"),
        F.col("nome_municipio").cast(StringType()).alias("nome_municipio"),
        F.col("cod_municipio_siafi").cast(StringType()).cast(IntegerType()).alias("Cod_Municipio_Siafi"),
        F.split(F.trim(F.col("nome_favorecido")), " ").getItem(0).alias("primeiro_nome"),
        F.col("nis_favorecido"),
        F.col("valor_parcela").cast(DoubleType()).alias("valor_parcela")
    )
    
    df_enriched = df_transformed.withColumn(
        "Data", 
        F.to_date(F.concat(F.col("mes_competencia").cast(StringType()), F.lit("01")), "yyyyMMdd")
    ).withColumn(
        "Genero",
        F.when(F.upper(F.trim(F.col("primeiro_nome"))).rlike("A$"), F.lit("Feminino")).otherwise(F.lit("Masculino"))
    )
    
    df_fato_redz_mensal = df_enriched.groupBy(
        "Data",
        "Genero",
        "mes_competencia",
        "nome_municipio",
        "Cod_Municipio_Siafi",
        "uf"
    ).agg(
        F.count("nis_favorecido").cast(LongType()).alias("total_beneficiarios"),
        F.round(F.sum("valor_parcela"), 2).cast(DecimalType(18, 2)).alias("valor_total_pago")
    ).withColumn(
        "valor_total_pago_novo",
        F.when(F.col("mes_competencia") >= 202607, F.round(F.col("valor_total_pago") * 1.1504, 2))
         .otherwise(F.col("valor_total_pago"))
    )

print("[OK] Dados analíticos consolidados prontos para carga Delta!")
df_fato_redz_mensal.show(5, truncate=False)

# 5. Carga Incremental Idempotente na Tabela Delta Lakehouse (com Alinhamento de Schema Automático)
meses_para_gravar = [r.mes_competencia for r in df_fato_redz_mensal.select("mes_competencia").distinct().collect()]
print(f"[*] Meses a serem gravados/atualizados: {meses_para_gravar}")

if spark.catalog.tableExists(DELTA_TABLE_REDZ):
    delta_table = DeltaTable.forName(spark, DELTA_TABLE_REDZ)
    
    # Alinhamento estrito com os tipos exatos da tabela Delta existente (evita DELTA_FAILED_TO_MERGE_FIELDS)
    target_table = spark.table(DELTA_TABLE_REDZ)
    target_fields = {f.name.lower(): (f.name, f.dataType) for f in target_table.schema.fields}
    print(f"[*] Schema existente na tabela Delta: {[f'{f.name} ({f.dataType})' for f in target_table.schema.fields]}")
    
    col_selects = []
    for c in df_fato_redz_mensal.columns:
        c_low = c.lower()
        if c_low in target_fields:
            orig_name, orig_type = target_fields[c_low]
            col_selects.append(F.col(c).cast(orig_type).alias(orig_name))
        else:
            col_selects.append(F.col(c))
    
    df_fato_redz_mensal = df_fato_redz_mensal.select(*col_selects)
    
    for mes in meses_para_gravar:
        print(f"[*] Limpando mês existente {mes} da tabela Delta (idempotência)...")
        delta_table.delete(f"mes_competencia = {mes}")
    
    print("[*] Gravando novos registros incrementais via mode('append')...")
    df_fato_redz_mensal.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(DELTA_TABLE_REDZ)
else:
    print(f"[*] Criando primeira versão da tabela Delta '{DELTA_TABLE_REDZ}'...")
    df_fato_redz_mensal.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(DELTA_TABLE_REDZ)

print(f"[OK] Carga incremental na tabela '{DELTA_TABLE_REDZ}' finalizada com sucesso!")

# 6. Manutenção Delta & Otimização Direct Lake (V-Order / Optimize)
print("[*] Executando OPTIMIZE e Z-ORDER para máxima performance no Power BI...")
spark.sql(f"OPTIMIZE {DELTA_TABLE_REDZ} ZORDER BY (mes_competencia, Cod_Municipio_Siafi, Genero)")
print("[OK] Manutenção concluída com sucesso!")

# Validação final de totais gravados
spark.sql(f"""
    SELECT 
        mes_competencia,
        COUNT(*) AS total_linhas_agregadas,
        SUM(total_beneficiarios) AS soma_beneficiarios,
        SUM(valor_total_pago) AS soma_valor_pago
    FROM {DELTA_TABLE_REDZ}
    WHERE mes_competencia IN ({','.join([str(m) for m in meses_para_gravar])})
    GROUP BY mes_competencia
    ORDER BY mes_competencia DESC
""").show()
