#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================================================================
PROJETO: BOLSA FAMÍLIA ANALYTICS — ATUALIZADOR MENSAL MICROSOFT FABRIC
==================================================================================================
Ambiente Destino: Microsoft Fabric (Lakehouse LH_Bolsa_Familia / OneLake)
Módulo: Desktop Local Prep & Incremental Packager
Autor: Karl / Antigravity Assistant
Data: Setembro/2026
==================================================================================================
"""

import os
import sys
import glob
import time
import re
import duckdb

if hasattr(sys.stdout, 'reconfigure'): sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'): sys.stderr.reconfigure(encoding='utf-8', errors='replace')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
DETALHADO_DIR = os.path.join(BASE_DIR, "0_Bases_de_Dados", "detalhado")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "staging_mensal_fabric")

def extract_yyyymm(filename):
    m = re.search(r'(\d{6})', filename)
    return int(m.group(1)) if m else None

def processar_mes_para_fabric(fpath, output_dir=OUTPUT_DIR):
    fname = os.path.basename(fpath)
    ym = extract_yyyymm(fname)
    if not ym:
        print(f"[AVISO] Não foi possível extrair YYYYMM de {fname}")
        return None

    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, f"{ym}_fato_bolsa_familia_redz.parquet")

    print(f"\n[*] Processando mês {ym} a partir de: {fname}")
    t0 = time.time()

    con = duckdb.connect()
    sql_path = fpath.replace("\\", "/")
    sql_out = out_file.replace("\\", "/")

    sql = f"""
    COPY (
        WITH raw_data AS (
            SELECT 
                mes_competencia,
                uf,
                nome_municipio,
                cod_municipio_siafi,
                SPLIT_PART(nome_favorecido, ' ', 1) AS primeiro_nome,
                nis_favorecido,
                valor_parcela
            FROM read_parquet('{sql_path}')
        )
        SELECT 
            TRY_STRPTIME(r.mes_competencia::VARCHAR || '01', '%Y%m%d')::DATE AS Data,
            IF(UPPER(TRIM(r.primeiro_nome)) LIKE '%A', 'Feminino', 'Masculino')::VARCHAR AS Genero,
            r.mes_competencia::INTEGER AS mes_competencia,
            r.nome_municipio::VARCHAR AS nome_municipio,
            r.cod_municipio_siafi::INTEGER AS Cod_Municipio_Siafi,
            COUNT(r.nis_favorecido)::BIGINT AS total_beneficiarios,
            r.uf::VARCHAR AS uf,
            ROUND(SUM(r.valor_parcela), 2)::DECIMAL(18,2) AS valor_total_pago,
            ROUND(CASE WHEN r.mes_competencia >= 202607 THEN SUM(r.valor_parcela) * 1.1504 ELSE SUM(r.valor_parcela) END, 2)::DECIMAL(18,2) AS valor_total_pago_novo
        FROM raw_data r
        GROUP BY 1, 2, 3, 4, 5, 7
        ORDER BY uf, nome_municipio, Genero
    ) TO '{sql_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """

    con.execute(sql)
    con.close()

    elapsed = time.time() - t0
    fsize_mb = os.path.getsize(out_file) / (1024 * 1024)
    print(f"[OK] Mês {ym} processado com sucesso em {elapsed:.2f}s!")
    print(f"     -> Arquivo gerado: {out_file} ({fsize_mb:.2f} MB)")
    return out_file

def main():
    print("=" * 80)
    print(" >>> ATUALIZADOR MENSAL — MICROSOFT FABRIC (BOLSA FAMÍLIA) <<<")
    print("=" * 80)

    arquivos = sorted(glob.glob(os.path.join(DETALHADO_DIR, "*_NovoBolsaFamilia_fato.parquet")))
    if not arquivos:
        print(f"[ERRO] Nenhum arquivo Parquet detalhado encontrado em: {DETALHADO_DIR}")
        sys.exit(1)

    print(f"[OK] Total de arquivos mensais disponíveis localmente: {len(arquivos)}")
    ultimo_arquivo = arquivos[-1]
    ultimo_mes = extract_yyyymm(ultimo_arquivo)
    print(f"[*] Último mês disponível: {ultimo_mes} ({os.path.basename(ultimo_arquivo)})")

    processar_mes_para_fabric(ultimo_arquivo)

    print("\n" + "=" * 80)
    print("[SUCESSO] Slice gerado e pronto para envio ao Microsoft Fabric Lakehouse!")
    print("Para executar a carga no Fabric, execute o notebook:")
    print(" -> NT_UPDATE_Fatos_Mensal.py (no Lakehouse LH_Bolsa_Familia)")
    print("=" * 80)

if __name__ == "__main__":
    main()
