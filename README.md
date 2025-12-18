# ETL PySpark – TLC Taxi Trip Data

Este projeto implementa uma **pipeline de ETL distribuída em PySpark**, seguindo boas práticas de engenharia de dados e organização em camadas **Bronze / Silver / Gold**.  

---

## 📌 Objetivo do Projeto

- Praticar **ETL em larga escala com PySpark**
- Trabalhar com dados reais (NYC TLC Taxi Trips)
- Aplicar limpeza, enriquecimento e agregações
- Gerar datasets prontos para **análise e machine learning**
- Registrar **metadados de execução** para reprodutibilidade

---

## 📂 Estrutura do Projeto

```text
etl_pyspark/
├── extract/        # Leitura e ingestão de dados (Bronze)
├── transform/      # Limpeza, normalização e enriquecimento (Silver)
├── enrich/         # Criação de features derivadas
├── load/           # Escrita dos dados e metadados (Gold)
├── schema/         # Definição de schemas e nomes de colunas
├── utils/          # Funções auxiliares (logging, paths, etc.)
├── jobs/
├── tests/          # Testes unitários (opcional)
├── README.md
└── run_etl.py      # Job principal da pipeline
