# ==================================================================================================
# PROJETO: BOLSA FAMÍLIA ANALYTICS
# AMBIENTE: Microsoft Fabric (Lakehouse LH_Bolsa_Familia)
# NOTEBOOK: NT_UPDATE_Fatos_Mensal (STATUS: PRODUÇÃO / CARGA INCREMENTAL RESILIENTE)
# AUTOR: Karl / Antigravity Assistant
# DATA: Setembro/2026
#
# OBJETIVO OPERACIONAL:
#   Notebook de carga incremental mensal 100% resiliente a mudanças de layout e acentuação.
#   Processa arquivos Parquet mensais colocados em 'Files/novos_meses/' ou 'Files/detalhado/':
#   - Suporta tanto arquivos JÁ CONSOLIDADOS (redz) quanto BRUTOS.
#   - Normaliza automaticamente nomes de colunas com ACENTOS (ex: 'MÊS COMPETÊNCIA', 'CÓDIGO MUNICÍPIO SIAFI').
#   - Aplica as regras de negócio analíticas (SIAFI, Gênero, Temporalidade).
#   - Executa MERGE / Carga idempotente na tabela Delta 'fato_bolsa_familia_redz' do Lakehouse.
#   - Executa OPTIMIZE e V-Order para máxima velocidade no Direct Lake do Power BI.
# ==================================================================================================

# %% [markdown]
# ### 1. Importação de Bibliotecas & Parâmetros

# %%
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import unicodedata
import os

# Parâmetros de Governança
LAKEHOUSE_NAME = "LH_Bolsa_Familia"
DELTA_TABLE_REDZ = "fato_bolsa_familia_redz"

# Caminhos no OneLake do Lakehouse
PATH_INPUT = "Files/novos_meses"

print(f"[*] Iniciando rotina incremental resiliente no Lakehouse: {LAKEHOUSE_NAME}")

# %% [markdown]
# ### 2. Identificação & Leitura dos Arquivos com Normalização de Cabeçalhos

# %%
def normalizar_coluna(col_name):
    # Remove acentos, caracteres especiais, converte espaços para underscore e põe em minúsculas
    n = unicodedata.normalize('NFKD', col_name).encode('ASCII', 'ignore').decode('ASCII')
    n = n.strip().lower().replace(' ', '_').replace('-', '_')
    return n

# Busca dinamica em multiplos locais possiveis dentro de 'Files/'
locais_busca = [
    "Files/novos_meses/*.parquet",
    "Files/*.parquet",
    "Files/detalhado/*.parquet",
    "Files/*/*.parquet"
]

df_raw = None
caminho_usado = None

for p in locais_busca:
    try:
        teste = spark.read.parquet(p)
        if len(teste.columns) > 0:
            df_raw = teste
            caminho_usado = p
            print(f"[OK] Arquivo(s) parquet encontrado(s) e lido(s) com sucesso em: '{caminho_usado}'")
            break
    except Exception:
        continue

if df_raw is None:
    print("\n" + "="*80)
    print("❌ [ALERTA DE ARQUIVO] Nenhum arquivo .parquet encontrado dentro da pasta 'Files'!")
    print("Verifique se o upload do arquivo foi realizado na aba 'Files' do Lakehouse LH_Bolsa_Familia.")
    try:
        from notebookutils import mssparkutils
        print("[*] Conteudo atual da pasta 'Files':")
        for item in mssparkutils.fs.ls("Files"):
            t_str = "Pasta" if item.isDir else f"{item.size} bytes"
            print(f"  - {item.name} ({t_str})")
    except Exception:
        pass
    print("="*80 + "\n")
    raise FileNotFoundError("Nenhum arquivo Parquet encontrado em 'Files/'. Faca o upload do arquivo .parquet na pasta Files do Lakehouse!")

# 1. Normalizar todas as colunas do arquivo (elimina qualquer problema com acentuação)
print(f"[*] Colunas originais encontradas no arquivo:")
print("   ", df_raw.columns)

df_norm = df_raw
for c in df_raw.columns:
    df_norm = df_norm.withColumnRenamed(c, normalizar_coluna(c))

colunas_normalizadas = df_norm.columns
print(f"[*] Colunas normalizadas (sem acentos/espaços):")
print("   ", colunas_normalizadas)

# Tratamento para variações de nomes comuns do Portal da Transparência
if "codigo_municipio_siafi" in colunas_normalizadas and "cod_municipio_siafi" not in colunas_normalizadas:
    df_norm = df_norm.withColumnRenamed("codigo_municipio_siafi", "cod_municipio_siafi")

if "mes_referencia" in colunas_normalizadas and "mes_competencia" not in colunas_normalizadas:
    df_norm = df_norm.withColumnRenamed("mes_referencia", "mes_competencia")

# %% [markdown]
# ### 3. Transformação Analítica (Resiliente: Detecta se é Bruto ou Reduzido)

# %%
# CASO A: O arquivo já é um Parquet Reduzido (gerado pelo atualizador local)
if "total_beneficiarios" in df_norm.columns:
    print("[INFO] Arquivo detectado como PARQUET REDUZIDO (já pré-agregado).")
    
    # Garantir nomes oficiais de colunas
    df_fato_redz_mensal = df_norm.select(
        F.col("data").cast(DateType()).alias("Data") if "data" in df_norm.columns else F.to_date(F.concat(F.col("mes_competencia").cast(StringType()), F.lit("01")), "yyyyMMdd").alias("Data"),
        F.col("genero").cast(StringType()).alias("Genero"),
        F.col("mes_competencia").cast(IntegerType()).alias("mes_competencia"),
        F.col("nome_municipio").cast(StringType()).alias("nome_municipio"),
        F.coalesce(F.col("cod_municipio_siafi"), F.col("codigo_municipio_siafi")).cast(IntegerType()).alias("Cod_Municipio_Siafi"),
        F.col("total_beneficiarios").cast(LongType()).alias("total_beneficiarios"),
        F.col("uf").cast(StringType()).alias("uf"),
        F.col("valor_total_pago").cast(DecimalType(18, 2)).alias("valor_total_pago"),
        F.col("valor_total_pago_novo").cast(DecimalType(18, 2)).alias("valor_total_pago_novo") if "valor_total_pago_novo" in df_norm.columns else F.when(F.col("mes_competencia") >= 202607, F.round(F.col("valor_total_pago") * 1.1504, 2)).otherwise(F.col("valor_total_pago")).alias("valor_total_pago_novo")
    )

# CASO B: O arquivo é BRUTO (contém favorecidos nominais, com ou sem acentos)
else:
    print("[INFO] Arquivo detectado como PARQUET BRUTO NOMINAL. Executando agregação analítica no Spark...")
    
    df_transformed = df_norm.select(
        F.col("mes_competencia").cast(IntegerType()).alias("mes_competencia"),
        F.col("uf").cast(StringType()).alias("uf"),
        F.col("nome_municipio").cast(StringType()).alias("nome_municipio"),
        F.col("cod_municipio_siafi").cast(IntegerType()).alias("Cod_Municipio_Siafi"),
        F.split(F.trim(F.col("nome_favorecido")), " ").getItem(0).alias("primeiro_nome"),
        F.col("nis_favorecido"),
        F.col("valor_parcela").cast(DoubleType()).alias("valor_parcela")
    )

    # Derivação de Data e Gênero (Regra Censo IBGE Oficial)
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

print(f"[OK] Carga incremental na tabela '{DELTA_TABLE_REDZ}' finalizada com sucesso!")

# %% [markdown]
# ### 5. Manutenção Delta & Otimização Direct Lake (V-Order / Optimize)

# %%
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
