"""
scripts/atualizar_tabelas_metadata.py

Script para carregar e atualizar as tabelas de metadados do projeto Smart Factory.

Este script lê os arquivos JSON da pasta /data e os envia para o PostgreSQL,
substituindo completamente as tabelas location, sku e tag.

Uso:
    python scripts/atualizar_tabelas_metadata.py

Requer:
    - Arquivo .env na raiz do projeto com as credenciais do banco
    - Arquivos location.json, sku.json, tag.json dentro da pasta /data
"""

import json
import logging
import os
from typing import Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Configuração do sistema de logs
# Isso substitui todos os print() do código original
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# FUNÇÃO 1: CARREGAR ARQUIVO JSON
# ============================================================

def carregar_json(caminho_arquivo: str) -> list[dict]:
    """
    Carrega um arquivo JSON e retorna como lista de dicionários.
    
    Args:
        caminho_arquivo: Caminho absoluto ou relativo do arquivo
        
    Returns:
        Lista de dicionários com os dados do JSON
        
    Raises:
        FileNotFoundError: Se o arquivo não existir
        json.JSONDecodeError: Se o JSON for inválido
    """
    logger.debug(f"Lendo arquivo: {caminho_arquivo}")
    
    with open(caminho_arquivo, "r", encoding="utf-8") as arquivo:
        return json.load(arquivo)


# ============================================================
# FUNÇÃO 2: CRIAR DATAFRAMES A PARTIR DOS JSONS
# ============================================================

def criar_dataframes() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Carrega os três JSONs da pasta /data e os converte em DataFrames.
    
    Returns:
        Tupla com (df_location, df_sku, df_tag) nesta ordem
        
    Raises:
        FileNotFoundError: Se algum dos arquivos obrigatórios estiver faltando
    """
    # Encontra o diretório raiz do projeto
    # Assumindo que este script está dentro da pasta /scripts
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    raiz_projeto = os.path.dirname(diretorio_atual)
    pasta_data = os.path.join(raiz_projeto, "data")
    
    # Define os caminhos completos dos arquivos
    caminho_location = os.path.join(pasta_data, "location.json")
    caminho_sku = os.path.join(pasta_data, "sku.json")
    caminho_tag = os.path.join(pasta_data, "tag.json")
    
    # Verifica se todos os arquivos existem
    for caminho in [caminho_location, caminho_sku, caminho_tag]:
        if not os.path.exists(caminho):
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
    
    logger.info("📂 Carregando arquivos JSON...")
    
    # Carrega os dados
    dados_location = carregar_json(caminho_location)
    dados_sku = carregar_json(caminho_sku)
    dados_tag = carregar_json(caminho_tag)
    
    # Converte para DataFrames
    df_location = pd.DataFrame(dados_location)
    df_sku = pd.DataFrame(dados_sku)
    df_tag = pd.DataFrame(dados_tag)
    
    # Converte colunas booleanas para o tipo correto
    if "is_machine" in df_location.columns:
        df_location["is_machine"] = df_location["is_machine"].astype(bool)
    
    if "is_active" in df_sku.columns:
        df_sku["is_active"] = df_sku["is_active"].astype(bool)
    
    if "is_critical" in df_tag.columns:
        df_tag["is_critical"] = df_tag["is_critical"].astype(bool)
    
    # Log dos resultados
    logger.info(f"   ✅ Location: {len(df_location)} registros")
    logger.info(f"   ✅ SKU: {len(df_sku)} registros")
    logger.info(f"   ✅ Tag: {len(df_tag)} registros")
    
    return df_location, df_sku, df_tag


# ============================================================
# FUNÇÃO 3: VALIDAR DATAFRAMES
# ============================================================

def validar_dataframes(
    df_location: pd.DataFrame,
    df_sku: pd.DataFrame,
    df_tag: pd.DataFrame
) -> bool:
    """
    Valida se os DataFrames possuem as colunas mínimas obrigatórias.
    
    Args:
        df_location: DataFrame com dados de localização
        df_sku: DataFrame com dados de produtos
        df_tag: DataFrame com dados de sensores
        
    Returns:
        True se todos os DataFrames forem válidos
        
    Raises:
        ValueError: Se alguma coluna obrigatória estiver faltando
    """
    # Colunas obrigatórias para cada tabela
    colunas_location_obrigatorias = {"location_id", "equipment_code", "is_machine"}
    colunas_sku_obrigatorias = {"id", "sku_code", "is_active"}
    colunas_tag_obrigatorias = {"tag_name", "location_id", "is_critical"}
    
    # Verifica Location
    faltando_location = colunas_location_obrigatorias - set(df_location.columns)
    if faltando_location:
        raise ValueError(f"Colunas faltando em location.json: {faltando_location}")
    
    # Verifica SKU
    faltando_sku = colunas_sku_obrigatorias - set(df_sku.columns)
    if faltando_sku:
        raise ValueError(f"Colunas faltando em sku.json: {faltando_sku}")
    
    # Verifica Tag
    faltando_tag = colunas_tag_obrigatorias - set(df_tag.columns)
    if faltando_tag:
        raise ValueError(f"Colunas faltando em tag.json: {faltando_tag}")
    
    logger.info("✅ Validação dos dados concluída")
    return True


# ============================================================
# FUNÇÃO 4: CONECTAR AO POSTGRESQL
# ============================================================

def conectar_postgres() -> Engine:
    """
    Cria conexão com PostgreSQL usando variáveis de ambiente.
    
    As variáveis devem estar no arquivo .env:
        DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
        
    Returns:
        Engine do SQLAlchemy pronta para uso
        
    Raises:
        ValueError: Se alguma variável de ambiente estiver faltando
    """
    # Carrega as variáveis do arquivo .env
    load_dotenv()
    
    # Lê cada variável
    usuario = os.getenv("DB_USER")
    senha = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    porta = os.getenv("DB_PORT")
    banco = os.getenv("DB_NAME")
    
    # Verifica se todas foram informadas
    variaveis_faltando = []
    if not usuario:
        variaveis_faltando.append("DB_USER")
    if not senha:
        variaveis_faltando.append("DB_PASSWORD")
    if not host:
        variaveis_faltando.append("DB_HOST")
    if not porta:
        variaveis_faltando.append("DB_PORT")
    if not banco:
        variaveis_faltando.append("DB_NAME")
    
    if variaveis_faltando:
        raise ValueError(f"Variáveis faltando no .env: {variaveis_faltando}")
    
    # Monta a URL de conexão
    url_conexao = f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}"
    
    logger.info(f"🔌 Conectando ao banco: {banco} em {host}:{porta}")
    
    return create_engine(url_conexao)


# ============================================================
# FUNÇÃO 5: ENVIAR PARA O POSTGRESQL
# ============================================================

def enviar_para_postgres(
    df_location: pd.DataFrame,
    df_sku: pd.DataFrame,
    df_tag: pd.DataFrame,
    engine: Engine
) -> None:
    """
    Envia os DataFrames para o PostgreSQL.
    
    ATENÇÃO: Usa if_exists="replace" que DROP e RECRIA as tabelas.
    Isso é adequado para ambiente de desenvolvimento.
    
    Args:
        df_location: DataFrame com localizações
        df_sku: DataFrame com SKUs
        df_tag: DataFrame com tags
        engine: Conexão ativa com o banco
    """
    try:
        # Testa a conexão antes de começar
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Conexão com banco estabelecida")
        
        # Envia cada tabela
        logger.info("📤 Subindo tabela: location...")
        df_location.to_sql("location", engine, if_exists="replace", index=False)
        
        logger.info("📤 Subindo tabela: sku...")
        df_sku.to_sql("sku", engine, if_exists="replace", index=False)
        
        logger.info("📤 Subindo tabela: tag...")
        df_tag.to_sql("tag", engine, if_exists="replace", index=False)
        
        # Resumo final
        logger.info("=" * 50)
        logger.info("✅ UPLOAD CONCLUÍDO COM SUCESSO!")
        logger.info(f"   Location: {len(df_location)} registros")
        logger.info(f"   SKU: {len(df_sku)} registros")
        logger.info(f"   Tag: {len(df_tag)} registros")
        logger.info("=" * 50)
        
    except Exception as erro:
        logger.error(f"❌ Erro durante o upload: {erro}")
        
        # Dica útil se o banco não existir
        if "database" in str(erro).lower():
            logger.info("")
            logger.info("💡 DICA: Se o erro for 'database não existe', execute:")
            logger.info("   CREATE DATABASE smart_factory;")
        
        raise


# ============================================================
# FUNÇÃO PRINCIPAL (MAIN)
# ============================================================

def main():
    """
    Função principal que orquestra todo o processo.
    
    Passos:
        1. Carregar JSONs e criar DataFrames
        2. Validar os dados
        3. Conectar ao PostgreSQL
        4. Enviar para o banco
    """
    logger.info("=" * 50)
    logger.info("🚀 INICIANDO ATUALIZAÇÃO DAS TABELAS")
    logger.info("=" * 50)
    
    try:
        # Passo 1: Carregar os dados
        df_location, df_sku, df_tag = criar_dataframes()
        
        # Passo 2: Validar
        validar_dataframes(df_location, df_sku, df_tag)
        
        # Passo 3: Conectar ao banco
        engine = conectar_postgres()
        
        # Passo 4: Enviar para o PostgreSQL
        enviar_para_postgres(df_location, df_sku, df_tag, engine)
        
    except FileNotFoundError as erro:
        logger.error(f"❌ Arquivo não encontrado: {erro}")
        logger.info("   Verifique se os arquivos JSON estão na pasta /data")
        
    except ValueError as erro:
        logger.error(f"❌ Erro de validação: {erro}")
        
    except Exception as erro:
        logger.error(f"❌ Erro inesperado: {erro}")
        logger.info("   Verifique a mensagem acima para mais detalhes")


# ============================================================
# PONTO DE ENTRADA DO SCRIPT
# ============================================================

if __name__ == "__main__":
    main()