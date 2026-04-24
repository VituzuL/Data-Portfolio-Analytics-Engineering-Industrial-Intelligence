🏭 Industrial Intelligence — Data Analytics & Predictive Maintenance Platform

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)
![Machine Learning](https://img.shields.io/badge/ML-RandomForest-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

---

Projeto de **Data Analytics** e **Data Engineering** voltado para monitoramento industrial, manutenção preditiva e redução de falhas operacionais em ambientes de alta criticidade.

---

## 📌 Contexto de Negócio

Em ambientes industriais, especialmente em operações contínuas como indústrias de alimentos, óleos e manufatura pesada, cada parada não planejada representa perdas financeiras significativas, impacto na produção e aumento de risco operacional.

O problema normalmente **não está na falta de dados** — e sim na falta de estrutura para transformar esses dados em decisões.

Sensores geram milhares de eventos por hora, sistemas operacionais trabalham de forma isolada e a manutenção muitas vezes atua de forma **reativa**: o time só age depois que o problema já aconteceu.

**Isso gera:**
- paradas emergenciais de alto custo
- desperdício de matéria-prima
- baixa previsibilidade operacional
- dependência excessiva de conhecimento tácito
- dificuldade na priorização de manutenção

---

## 🎯 Objetivo do Projeto

Construir uma plataforma completa de análise industrial capaz de:

- simular ambientes reais de produção industrial
- estruturar pipelines de dados para ingestão e tratamento
- gerar indicadores operacionais em tempo real
- prever falhas antes que elas aconteçam
- apoiar decisões com inteligência analítica e IA aplicada

O foco não foi apenas treinar um modelo de Machine Learning, mas construir uma **solução de ponta a ponta** com visão real de negócio.

---

## 💡 Solução Desenvolvida

A solução foi estruturada em **quatro grandes frentes**:

### 1. Engenharia de Dados
Construção de pipelines ETL para ingestão, padronização e persistência de dados industriais em PostgreSQL, utilizando metadados, regras de negócio e simulação operacional realista.

### 2. Analytics Industrial
Desenvolvimento de indicadores estratégicos como OEE, downtime, performance operacional, health score e detecção de anomalias para suporte à tomada de decisão.

### 3. Machine Learning Preditivo
Treinamento de modelo supervisionado para antecipação de falhas industriais com foco em manutenção preditiva e redução de paradas não planejadas.

### 4. Assistente Industrial com IA
Interface conversacional com LLM integrada ao banco de dados, permitindo consultas em linguagem natural e recomendações acionáveis para operação e manutenção.

---

## 🏗️ Arquitetura da Solução

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/d0347963-1f13-4006-88c7-d632a05a081c" />



---

## 🛠️ Stack Tecnológica

| Camada | Tecnologias |
|--------|-------------|
| **Banco de Dados** | PostgreSQL, SQLAlchemy |
| **Processamento** | Python, Pandas, NumPy |
| **Engenharia de Dados** | ETL, Data Modeling, Feature Engineering |
| **Machine Learning** | Scikit-learn, Random Forest |
| **Visualização** | Matplotlib, Seaborn |
| **IA Aplicada** | Google Gemini |
| **Governança** | Logging, versionamento com Git |
| **Arquitetura** | Data Pipelines, Simulação Industrial |

---

## 📊 Principais Entregas

### Pipeline ETL Profissional
- carga e normalização de metadados industriais
- validação e padronização de dados
- tratamento de inconsistências
- logging estruturado
- persistência em PostgreSQL

### Simulador Industrial Realista

Simulação de comportamento operacional com:

- ciclos de falha (normal → degradação → falha → recuperação)
- correlação física entre sensores
- drift gradual de sensores
- campanhas de produção por SKU
- setup de linha e mudança de operação

O objetivo foi aproximar o cenário o **máximo possível da realidade industrial**.

### Feature Engineering Aplicado

Construção de features relevantes para manutenção preditiva:

- lags temporais
- rolling averages
- rolling standard deviation
- taxas de variação
- health score agregado
- detecção de anomalias com Z-Score
- remoção de leakage e overfitting estrutural

### Modelo de Predição de Falhas

Modelo Random Forest com foco em:

- classificação de pré-falha
- alta sensibilidade operacional
- threshold tuning baseado em F1-score
- interpretação via feature importance
- avaliação com matriz de confusão e curva ROC

> Mais importante que "acertar muito" foi construir um modelo **utilizável operacionalmente**.

### Assistente Industrial com IA

Permite perguntas como:

- "Qual máquina está mais crítica hoje?"
- "Teve falha essa semana?"
- "Como está o Desodorizador 1?"
- "Quais equipamentos exigem manutenção preventiva?"

Com respostas contextualizadas e **recomendações acionáveis**.

---

## 📈 Resultados Obtidos

### Performance do Modelo

| Métrica | Resultado |
|---------|-----------|
| **Recall** | ~80% |
| **Precisão** | ~70% |
| **F1-Score** | ~75% |
| **AUC-ROC** | ~0.89 |

### Impacto Simulado de Negócio

- antecipação de falhas antes da parada crítica
- redução de manutenção corretiva emergencial
- maior previsibilidade operacional
- melhor priorização das equipes de manutenção
- redução de desperdícios e perdas produtivas
- aumento da confiabilidade analítica

> O projeto foi pensado para responder uma pergunta simples:
> **"Como usar dados para evitar que a operação pare?"**

---

 Sobre Este Projeto
Este projeto foi desenvolvido como portfólio profissional com foco em Data Analytics, Analytics Engineering e Data Engineering aplicados ao contexto industrial.

A proposta não foi criar apenas mais um projeto de Machine Learning, mas demonstrar capacidade de construir soluções completas de dados com visão de negócio, arquitetura consistente e aplicabilidade real.

Ele reúne experiências práticas que vivi em ambientes industriais e transforma isso em uma solução estruturada, escalável e próxima do que existe no mercado.

# Se dados evitam falhas, então dados também geram resultado.
