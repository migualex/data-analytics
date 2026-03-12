"""
CASE TÉCNICO DIGITAL FINANCE LATAM UNILEVER
Consolidação Actual vs Plan | Q4 2025
Autor: Miguel Alexandre da Cunha
"""

import pandas as pd
import numpy as np
import os

# 1. Leitura dos arquivos
DATA_PATH = "data_dir/"

try:
    actual_df = pd.read_csv(os.path.join(DATA_PATH, "actual_df.csv"))
    plan_df = pd.read_csv(os.path.join(DATA_PATH, "plan_df.csv"))
    product_df = pd.read_csv(os.path.join(DATA_PATH, "product_df.csv"))
    currency_df = pd.read_csv(os.path.join(DATA_PATH, "currency_df.csv"))
    fsitem_df = pd.read_csv(os.path.join(DATA_PATH, "fsitem_df.csv"))
except FileNotFoundError as e:
    print(f"Erro ao carregar arquivos: {e}")
    exit()

# 2. Filtro temporal – Q4 2025
# Considerando apenas outubro, novembro e dezembro de 2025
meses_q4 = [10, 11, 12]

actual_df = actual_df[
    (actual_df["year"] == 2025) &
    (actual_df["month"].isin(meses_q4))
].copy()

plan_df = plan_df[
    (plan_df["year"] == 2025) &
    (plan_df["month"].isin(meses_q4))
].copy()

print(f"Actual Q4: {len(actual_df)} linhas")
print(f"Plan Q4: {len(plan_df)} linhas")

# 3. Identificação da Company
# Company é definida pelos 4 primeiros dígitos da COCO
actual_df["Company"] = actual_df["COCO"].astype(str).str[:4]
plan_df["Company"]   = plan_df["COCO"].astype(str).str[:4]
currency_df["Company"] = currency_df["Company"].astype(str)

# 4. Harmonização SKU → BFPP
# Actual está no nível SKU (PKEY). 
# Mapeamento para BFPP via tabela mestre de produto.
actual_df = actual_df.merge(
    product_df[["PKEY", "BFPP_Code"]],
    on="PKEY",
    how="left"
)

# Caso algum SKU não esteja cadastrado corretamente
actual_df["BFPP"] = actual_df["BFPP_Code"].fillna("UNMAPPED_BFPP")

unmapped = actual_df["BFPP_Code"].isna().sum()
print(f"SKUs sem mapeamento BFPP: {unmapped}")

# Plan já está estruturado no nível BFPP
plan_df["BFPP"] = plan_df["PKEY"]

# 5. Alinhamento Financeiro
# Padronização do nome da coluna para merge
fsitem_df = fsitem_df.rename(columns={"FS Item": "FSITMS"})

# Mapeamento das contas financeiras para nível L01
actual_df = actual_df.merge(
    fsitem_df[["FSITMS", "L01"]],
    on="FSITMS",
    how="left"
)

plan_df = plan_df.merge(
    fsitem_df[["FSITMS", "L01"]],
    on="FSITMS",
    how="left"
)

# Contas não encontradas permanecem rastreáveis
actual_df["L01"] = actual_df["L01"].fillna("UNMAPPED_L01")
plan_df["L01"]   = plan_df["L01"].fillna("UNMAPPED_L01")

# 6. Conversão Cambial para EUR
# Identifica a granularidade da taxa cambial
# (caso exista year e month na tabela de câmbio)
fx_keys = ["Company"]

if "year" in currency_df.columns:
    fx_keys.append("year")

if "month" in currency_df.columns:
    fx_keys.append("month")

actual_df = actual_df.merge(
    currency_df[fx_keys + ["Euro_Rate"]],
    on=fx_keys,
    how="left"
)

# Verificação simples de integridade cambial
missing_fx = actual_df["Euro_Rate"].isna().sum()
if missing_fx > 0:
    print(f"Atenção: {missing_fx} registros sem taxa de câmbio.")
    # Remove registros sem taxa para evitar distorção financeira
    actual_df = actual_df.dropna(subset=["Euro_Rate"])

# Conversão do valor local para EUR
actual_df["Actual_EUR"] = actual_df["TOTAL"] / actual_df["Euro_Rate"]

# Plan já está em EUR conforme especificação do dataset
plan_df["Plan_EUR"] = plan_df["TOTALEU"]

# 7. Consolidação Final
# Estrutura final no nível:
# Company | Month | BFPP | L01
group_cols = ["Company", "month", "BFPP", "L01"]

actual_agg = (
    actual_df
    .groupby(group_cols, as_index=False)["Actual_EUR"]
    .sum()
)

plan_agg = (
    plan_df
    .groupby(group_cols, as_index=False)["Plan_EUR"]
    .sum()
)

# Outer join para capturar cenários:
# - orçamento sem execução
# - execução sem orçamento
final_df = actual_agg.merge(
    plan_agg,
    on=group_cols,
    how="outer"
)

# Preenche ausências com zero para análise de desvio
final_df["Actual_EUR"] = final_df["Actual_EUR"].fillna(0)
final_df["Plan_EUR"]   = final_df["Plan_EUR"].fillna(0)

# 8. Cálculo de Gaps
final_df["Gap_EUR"] = final_df["Actual_EUR"] - final_df["Plan_EUR"]

# Gap percentual apenas quando há baseline de plan
final_df["Gap_%"] = np.where(
    final_df["Plan_EUR"] != 0,
    final_df["Gap_EUR"] / final_df["Plan_EUR"],
    np.nan
)

# Ordenação para facilitar consumo analítico
final_df = final_df.sort_values(
    by=["Company", "month", "L01", "BFPP"]
)

# 9. Exportação
final_df.to_csv("output_q4_2025.csv", index=False)
print("Pipeline executada com sucesso. Arquivo gerado: output_q4_2025.csv")

"""
ANÁLISE DE DESVIOS – Q4 2025

Principais fatores dos gaps (Gap_EUR e Gap_%):

1. Diferenças operacionais: volumes e preços realizados vs planejados (L01).
2. Impacto cambial: conversão para EUR afeta países com maior volatilidade.
3. Execução x Planejamento: casos de Plan_EUR = 0 ou Actual_EUR = 0 mostram desalinhamento.
4. Lacunas de dados: UNMAPPED_BFPP e UNMAPPED_L01 indicam cadastro incompleto.

A consolidação permite analisar os desvios por Company, mês, BFPP e L01.
"""