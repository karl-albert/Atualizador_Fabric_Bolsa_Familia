# ==================================================================================================
# PROJETO: BOLSA FAMÍLIA ANALYTICS
# AMBIENTE: Microsoft Fabric (Lakehouse LH_Bolsa_Familia)
# NOTEBOOK: NT_UPDATE_Fatos_Mensal (STATUS: PRODUÇÃO / CARGA INCREMENTAL MENSAL)
# AUTOR: Karl
# DATA DE CRIAÇÃO: 23/Setembro/2026
#
# OBJETIVO OPERACIONAL:
#   Notebook de carga incremental mensal automatizada.
#   Processa o novo arquivo Parquet mensal do Bolsa Família, aplica as regras analíticas
#   de negócio (SIAFI, Derivação de Gênero do Favorecido, Chave Temporal) e realiza
#   o MERGE / Carga idempotente na tabela Delta 'fato_bolsa_familia_redz' do Lakehouse.
#
# REGRAS DE TRANSFORMAÇÃO:
#   1. Derivação de Gênero: Primeiro nome do favorecido terminado em 'A' -> 'Feminino', senão 'Masculino'.
#   2. Chave Temporal (Data): Primeiro dia do mês de competência (YYYYMM -> YYYY-MM-01).
#   3. Agregação Analítica: Agrupamento por (Data, Genero, mes_competencia, nome_municipio, Cod_Municipio_Siafi, uf).
#   4. Idempotência Delta: Substituição segura do mês sem duplicar registros históricos.
#   5. Otimização Delta: Execução de OPTIMIZE e V-Order para máxima velocidade no Direct Lake do Power BI.
# ==================================================================================================

# %% [markdown]
# ### 1. Importação de Bibliotecas & Parâmetros

# %%
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import os

# Parâmetros de Governança
LAKEHOUSE_NAME = "LH_Bolsa_Familia"
DELTA_TABLE_REDZ = "fato_bolsa_familia_redz"
DELTA_TABLE_BRUTA = "fato_bolsa_familia_bruta"

# Caminho relativo no OneLake do Lakehouse
# Os novos arquivos mensais devem ser colocados em Files/novos_meses/ ou Files/detalhado/
PATH_INPUT = "Files/novos_meses"

print(f"[*] Iniciando rotina incremental no Lakehouse: {LAKEHOUSE_NAME}")

# %% [markdown]
# ### 2. Identificação de Meses Pendentes & Leitura dos Arquivos

# %%
# Obter meses já carregados na tabela Delta Reduzida (se já existir)
meses_existentes = set()
try:
    if spark.catalog.tableExists(DELTA_TABLE_REDZ):
        df_existente = spark.sql(f"SELECT DISTINCT mes_competencia FROM {DELTA_TABLE_REDZ}")
        meses_existentes = set([row.mes_competencia for row in df_existente.collect()])
        print(f"[OK] Tabela '{DELTA_TABLE_REDZ}' encontrada. Meses já gravados ({len(meses_existentes)}): {sorted(list(meses_existentes))}")
    else:
        print(f"[INFO] Tabela '{DELTA_TABLE_REDZ}' ainda não existe. Será criada nesta primeira execução.")
except Exception as e:
    print(f"[AVISO] Verificação de catálogo: {e}")

# Ler os arquivos disponíveis
try:
    df_raw = spark.read.parquet(f"{PATH_INPUT}/*.parquet")
    total_linhas_lidas = df_raw.count()
    print(f"[OK] Total de linhas brutas lidas dos arquivos de entrada: {total_linhas_lidas:,}")
except Exception as e:
    print(f"[ERRO] Falha ao ler arquivos em '{PATH_INPUT}': {e}")
    # Fallback para caminho de Files/detalhado se necessário
    df_raw = spark.read.parquet("Files/detalhado/*.parquet")
    print(f"[FALLBACK] Linhas lidas de Files/detalhado: {df_raw.count():,}")

# %% [markdown]
# ### 3. Transformação Analítica (Regras de Negócio SIAFI + Gênero + Data)

# %%
# Normalização dos nomes das colunas e regras analíticas
df_transformed = df_raw.select(
    F.col("mes_competencia").cast(IntegerType()).alias("mes_competencia"),
    F.col("uf").cast(StringType()).alias("uf"),
    F.col("nome_municipio").cast(StringType()).alias("nome_municipio"),
    F.col("cod_municipio_siafi").cast(IntegerType()).alias("Cod_Municipio_Siafi"),
    F.split(F.trim(F.col("nome_favorecido")), " ").getItem(0).alias("primeiro_nome"),
    F.col("nis_favorecido"),
    F.col("valor_parcela").cast(DoubleType()).alias("valor_parcela")
)

# Derivação de Data e Gênero
df_enriched = df_transformed.withColumn(
    "Data",
    F.to_date(F.concat(F.col("mes_competencia").cast(StringType()), F.lit("01")), "yyyyMMdd")
).withColumn(
    "Genero",
    F.when(F.upper(F.trim(F.col("primeiro_nome"))).rlike("A$"), F.lit("Feminino")).otherwise(F.lit("Masculino"))
)

# Agregação na granularidade exata da FATO_REDUZIDA (Espelho Direct Lake Power BI)
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
).orderBy("mes_competencia", "uf", "nome_municipio", "Genero")

print("[OK] Agregação analítica concluída com sucesso!")
df_fato_redz_mensal.show(5, truncate=False)

# %% [markdown]
# ### 4. Carga Incremental Idempotente na Tabela Delta Lakehouse

# %%
meses_para_gravar = [r.mes_competencia for r in df_fato_redz_mensal.select("mes_competencia").distinct().collect()]
print(f"[*] Meses a serem gravados/atualizados: {meses_para_gravar}")

if spark.catalog.tableExists(DELTA_TABLE_REDZ):
    delta_table = DeltaTable.forName(spark, DELTA_TABLE_REDZ)
    
    for mes in meses_para_gravar:
        print(f"[*] Limpando mês existente {mes} da tabela Delta (idempotência)...")
        delta_table.delete(f"mes_competencia = {mes}")
    
    print("[*] Gravando novos registros incrementais via mode('append')...")
    df_fato_redz_mensal.write.format("delta").mode("append").saveAsTable(DELTA_TABLE_REDZ)
else:
    print(f"[*] Criando primeira versão da tabela Delta '{DELTA_TABLE_REDZ}'...")
    df_fato_redz_mensal.write.format("delta").mode("overwrite").saveAsTable(DELTA_TABLE_REDZ)

print(f"[OK] Carga incremental na tabela '{DELTA_TABLE_REDZ}' finalizada!")

# %% [markdown]
# ### 5. Manutenção Delta & Otimização Direct Lake (V-Order / Optimize)

# %%
print("[*] Executando OPTIMIZE e Z-ORDER para máxima performance no Power BI...")
spark.sql(f"OPTIMIZE {DELTA_TABLE_REDZ} ZORDER BY (mes_competencia, Cod_Municipio_Siafi, Genero)")
print("[OK] Manutenção concluída com sucesso!")

# Validação final de totais
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
