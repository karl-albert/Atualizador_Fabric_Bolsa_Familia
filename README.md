# Atualizador Mensal Bolsa Família — Microsoft Fabric Lakehouse

Repositório oficial para automação do pipeline mensal do **Bolsa Família Analytics** no **Microsoft Fabric (Lakehouse LH_Bolsa_Familia)**.

## 🎯 Arquitetura da Solução
* **Lakehouse**: `LH_Bolsa_Familia`
* **Tabela Delta Principal**: `fato_bolsa_familia_redz` (Direct Lake com Power BI)
* **Notebook de Produção**: `NT_UPDATE_Fatos_Mensal.py`
* **Preparador Local**: `atualizador_mensal_fabric.py` (Processamento de 19M linhas em 2.9s com DuckDB)

## 📌 Regras de Negócio & Governança
1. **Idempotência**: Substituição atômica por partição de mês (`mes_competencia`), evitando duplicidades históricas.
2. **Gênero**: Derivação por heurística no primeiro nome do favorecido (terminação em 'A' = Feminino, senão Masculino).
3. **SIAFI & Geografia**: Alinhamento das chaves de município SIAFI e UF com a tabela de dimensões IBGE.
4. **Performance**: Execução de `OPTIMIZE` e `Z-ORDER` para aceleração imediata no Direct Lake.

## 🚀 Como Executar
1. **Localmente**: Execute `Subir_Mes_Fabric.bat` para processar e gerar o slice mensal em Parquet.
2. **No Fabric**: Importe `NT_UPDATE_Fatos_Mensal.py` no Lakehouse `LH_Bolsa_Familia` e agende via Fabric Pipeline.
