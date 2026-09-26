# MVP-pos-graduacao-Engenharia-de-Dados
Repositorio para trazer o trabalho de MVP de Engenharia de Dados feito no Databricks

# Pipeline de Análise de Preços de Combustíveis no Varejo Nacional (Medallion Architecture)

[![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-red?logo=databricks)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange?logo=apachespark)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.x-blue)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.x-yellow?logo=python)](https://www.python.org/)

## Visão Geral do Projeto
Este projeto consiste na implementação de um pipeline de dados *end-to-end* em ambiente de nuvem (**Databricks**) para ingestão, tratamento e modelagem do histórico de Preços de Combustíveis (Gasolina Comum e Etanol) no Brasil, utilizando dados abertos fornecidos pela **ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis)**.

O objetivo principal é transformar dados brutos em dados analíticos confiáveis, organizados em uma **Arquitetura Medalhão (Bronze, Silver e Gold)** sob o formato **Delta Lake**, respondendo a hipóteses estratégicas de precificação regional e paridade de mercado.

---

## Problema & Perguntas de Negócio

O pipeline foi desenhado para processar os volumes históricos da ANP e responder às seguintes perguntas analíticas:

1. **Variação Temporal e Geográfica:** Q
2. **Paridade de Mercado (Etanol vs. Gasolina):** 
3. **Dispersão por Bandeiras:** 

---

## Arquitetura da Solução

O pipeline segue o padrão **Data Lakehouse** utilizando **Delta Lake** para garantir transações ACID, controle de esquema (*schema enforcement*) e alta performance de escrita/leitura.
