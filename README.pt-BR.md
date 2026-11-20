# ETL Pipeline Recovery

**Pipeline Rescue** é um caso funcional para o serviço **“Fix Broken ETL Pipelines.”** O projeto reproduz falhas reais do Spark em dados sintéticos, documenta as causas e comprova a correção com testes e reconciliação SQL.

**Python · PySpark 4.3 · SQL · SQLite · pytest**

## O que o projeto demonstra

- **Mudança de schema:** a consulta antiga espera `unit_price` e `order_date`; a origem passou a entregar `price` e `ordered_at`.
- **Conversões inválidas:** o cast direto falha em preços malformados e valores com vírgula decimal.
- **Join incorreto:** juntar todas as versões históricas do cliente multiplica as linhas.
- **Duplicação na repetição:** o código defeituoso usa append e acumula a mesma carga.
- **Recuperação validada:** aliases explícitos, tipos, deduplicação, tratamento de versões, quarentena e publicação transacional de uma partição.

As falhas de schema e cast são exceções produzidas pelo **PySpark executando de verdade**. O cenário de join grava somente em um destino defeituoso isolado, sem contaminar a tabela corrigida.

## Resultado comprovado

Dos **17 registros de origem**, a recuperação classificou **7 linhas válidas**, **8 para revisão**, **1 duplicata** e **1 versão antiga**. As sete linhas correspondem a **6 pedidos demonstrativos e R$ 511,20 em valores fictícios**.

O join defeituoso produziu **25 linhas**, e sua repetição acumulou **50** no destino legado. A carga corrigida manteve as mesmas sete linhas ao repetir. A opção estrita, que exige zero registros em quarentena, bloqueou a publicação e preservou a última partição válida.

**39 testes automatizados** verificam transformações, falhas, contratos, totais, repetição, concorrência e rollback de uma gravação interrompida. Os relatórios reais estão em [docs/evidence](docs/evidence).

## Rodar no computador

Requisitos: Python 3.11 ou superior e Java 17 ou superior configurado em `JAVA_HOME`. O pacote PySpark é grande; a primeira instalação pode levar alguns minutos.

```powershell
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.lock
.venv/Scripts/python -m pip install --no-deps -e .
.venv/Scripts/python -m uvicorn recovery.main:app --host 127.0.0.1 --port 8002
```

Abra **http://127.0.0.1:8002/**. Em Linux/macOS, use `.venv/bin/python`. O primeiro processamento inicia a JVM; os seguintes reaproveitam a sessão do Spark.

No painel, execute **Reproduce failure**, **Diagnose input** e **Recover partition**. Repita a recuperação para conferir o resultado inalterado. Ative **Require zero quarantined rows** para demonstrar a preservação da carga anterior quando uma validação bloqueia o processo.

O controle automático do navegador ficou indisponível nesta sessão. As ações e downloads foram exercitados pela API HTTP; a revisão visual e responsiva não foi confirmada.

## Execução reproduzível

```powershell
.venv/Scripts/python scripts/verify_case.py --database tmp/novo-caso.sqlite3 --output artifacts/verified-case
```

O script exige um caminho de banco novo, executa os oito casos e salva relatórios JSON, logs, resultados CSV e um manifesto com as verificações. Os erros esperados fazem parte da prova e o script termina com código zero quando todas as afirmações passam.

Este é um projeto pessoal de portfólio, sem cliente real, incidente de produção ou promessa de ROI. Consulte o [caso de engenharia](docs/INCIDENT_REPORT.md), o [contrato dos dados](docs/DATA_CONTRACT.md), o [guia de operação](docs/RUNBOOK.md) e o [README técnico](README.md).
