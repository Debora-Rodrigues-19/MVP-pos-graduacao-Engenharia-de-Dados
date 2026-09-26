# MVP-pos-graduacao-Engenharia-de-Dados
Repositorio para trazer o trabalho de MVP de Engenharia de Dados feito no Databricks

# Pipeline de Análise de Preços de Combustíveis no Varejo Nacional (Medallion Architecture)

[![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-red?logo=databricks)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange?logo=apachespark)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.x-blue)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.x-yellow?logo=python)](https://www.python.org/)

## 📌 Visão Geral do Projeto
Este projeto consiste na implementação de um pipeline de dados *end-to-end* em ambiente de nuvem (**Databricks**) para ingestão, tratamento e modelagem do histórico de preços de combustíveis (Gasolina Comum e Etanol) no Brasil, utilizando dados abertos fornecidos pela **ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis)**.

O objetivo principal é transformar dados brutos não estruturados/semistruturados em dados analíticos confiáveis, organizados em uma **Arquitetura Medalhão (Bronze, Silver e Gold)** sob o formato **Delta Lake**, respondendo a hipóteses estratégicas de precificação regional e paridade de mercado.

---

## 🎯 Problema & Perguntas de Negócio

O pipeline foi desenhado para processar os volumes históricos da ANP e responder às seguintes perguntas analíticas:

1. **Variação Temporal e Geográfica:** Qual é a variação média do preço da gasolina comum discriminada por Estado (UF) e Mês/Ano?
2. **Paridade de Mercado (Etanol vs. Gasolina):** Qual é a razão percentual ($\frac{\text{Preço Etanol}}{\text{Preço Gasolina}} \times 100$) por UF para identificação de viabilidade econômica ao consumidor?
3. **Dispersão por Bandeiras:** Quais distribuidoras/bandeiras (ex: BR/Vibra, Shell, Ipiranga, Branca) apresentam maior volatilidade e desvio padrão de preços por região geográfica?

---

## 🏗️ Arquitetura da Solução

O pipeline segue o padrão **Data Lakehouse** utilizando **Delta Lake** para garantir transações ACID, controle de esquema (*schema enforcement*) e alta performance de escrita/leitura.

```text
[ Fonte: ANP CSVs ]
       │
       ▼
 ┌───────────┐
 │   BRONZE  │  ---> Ingestão Raw (Arquivos brutos + Metadados de controle)
 └─────┬─────┘
       │
       ▼  (PySpark ETL / Cleansing & Normalization)
 ┌───────────┐
 │   SILVER  │  ---> Dados limpos, tipados, deduplicados e filtrados
 └─────┬─────┘
       │
       ▼  (Modeling & Aggregations / PySpark & SQL)
 ┌───────────┐
 │    GOLD   │  ---> Tabelas fato/dimensão agregadas e prontas para BI/SQL
 └───────────┘
