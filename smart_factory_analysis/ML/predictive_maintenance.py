"""
predictive_maintenance.py

Leitor de dados do simulador industrial.

Este módulo carrega os dados do PostgreSQL e gera um dataset
completo para Machine Learning, contendo:
    - Todas as máquinas
    - Todos os sensores (tags)
    - Labels de falha (is_failure, is_pre_failure)
    - Dados agregados por janela de tempo (ex: 10 minutos)

Modos de execução:
    - full_load (padrão): recria o dataset do zero
    - incremental: adiciona apenas dados novos (mais eficiente)
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ============================================================
# CONEXÃO COM O BANCO DE DADOS
# ============================================================

def conectar_banco():
    """
    Cria conexão com PostgreSQL usando variáveis de ambiente.
    
    Returns:
        Engine do SQLAlchemy
    """
    load_dotenv()
    
    usuario = os.getenv("DB_USER")
    senha = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    porta = os.getenv("DB_PORT")
    banco = os.getenv("DB_NAME")
    
    url_conexao = (
        f"postgresql+psycopg2://{usuario}:{senha}"
        f"@{host}:{porta}/{banco}"
    )
    
    return create_engine(url_conexao)


engine = conectar_banco()


# ============================================================
# CARREGAMENTO DE DADOS
# ============================================================

def load_all_machines():
    """
    Carrega todas as máquinas disponíveis no banco.
    
    Returns:
        DataFrame com location_id, equipment_code, location_name, etc.
    """
    query = """
    SELECT
        location_id,
        equipment_code,
        location_name,
        parent_id,
        is_machine,
        description
    FROM location
    WHERE is_machine = true
    ORDER BY location_id;
    """
    
    maquinas_df = pd.read_sql(query, engine)
    
    print(f"\n🏭 TOTAL DE MÁQUINAS: {len(maquinas_df)}")
    print(maquinas_df[['location_id', 'location_name', 'equipment_code']].to_string(index=False))
    
    return maquinas_df


def load_all_tags():
    """
    Carrega todas as tags (sensores) do banco.
    
    Returns:
        DataFrame com tag_name, location_id, label, unit, is_critical
    """
    query = """
    SELECT
        tag_name,
        location_id,
        description,
        label,
        unit,
        is_critical
    FROM tag
    ORDER BY location_id, tag_name;
    """
    
    tags_df = pd.read_sql(query, engine)
    
    print(f"\n📌 TOTAL DE TAGS: {len(tags_df)}")
    
    print(f"\n📊 Distribuição por máquina:")
    tags_por_maquina = tags_df.groupby('location_id').size()
    for loc_id, count in tags_por_maquina.items():
        print(f"   Máquina {loc_id}: {count} tags")
    
    return tags_df


def get_ultimo_timestamp_dataset():
    """
    Busca o timestamp mais recente no dataset existente.
    
    Returns:
        Datetime do último registro, ou None se arquivo não existe
    """
    if not os.path.exists('data_ml_complete.parquet'):
        return None
    
    try:
        df_existente = pd.read_parquet('data_ml_complete.parquet')
        ultimo_timestamp = df_existente['timestamp'].max()
        print(f"\n📅 Dataset existente encontrado. Último registro: {ultimo_timestamp}")
        return ultimo_timestamp
    except Exception as e:
        print(f"\n⚠️ Erro ao ler dataset existente: {e}")
        return None


def load_all_events(start_date=None, end_date=None, limit=None):
    """
    Carrega todos os eventos (leituras dos sensores).
    
    Args:
        start_date: Data/hora inicial (opcional)
        end_date: Data/hora final (opcional)
        limit: Limite de registros (opcional)
        
    Returns:
        DataFrame com eventos enriquecidos (incluindo localização)
    """
    
    conditions = []
    
    if start_date:
        conditions.append(f"e.time >= '{start_date}'")
    
    if end_date:
        conditions.append(f"e.time <= '{end_date}'")
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
    SELECT
        e.time,
        e.tag,
        e.value,
        e.info,
        t.label,
        t.location_id,
        t.unit,
        t.is_critical,
        l.location_name,
        l.equipment_code
    FROM event e
    JOIN tag t ON e.tag = t.tag_name
    JOIN location l ON t.location_id = l.location_id
    WHERE {where_clause}
    ORDER BY e.time
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    eventos_df = pd.read_sql(query, engine)
    
    if not eventos_df.empty:
        eventos_df['time'] = pd.to_datetime(eventos_df['time'])
    
    print(f"\n📊 TOTAL DE EVENTOS CARREGADOS: {len(eventos_df):,}")
    
    if not eventos_df.empty:
        print(f"   Período: {eventos_df['time'].min()} a {eventos_df['time'].max()}")
        print(f"   Máquinas: {eventos_df['location_name'].nunique()}")
    
    return eventos_df


# ============================================================
# CRIAÇÃO DE LABELS (ROTULOS) DE FALHA
# ============================================================

def create_failure_labels(eventos_df):
    """
    Cria labels de falha baseadas no campo 'info' da tabela event.
    
    Mapeamento:
        - critical_failure → is_failure = 1
        - anomaly_detected → is_pre_failure = 1 (pré-falha)
        - warning_detected → is_pre_failure = 1
        - normal → ambos 0
        - scheduled_maintenance → ambos 0
    
    Args:
        eventos_df: DataFrame com coluna 'info'
        
    Returns:
        DataFrame com colunas adicionais: machine_state, is_failure, is_pre_failure
    """
    
    if eventos_df.empty:
        return eventos_df
    
    # Dicionário de mapeamento
    mapeamento_info = {
        'normal':               {'state': 'normal',       'is_failure': 0, 'is_pre_failure': 0},
        'warning_detected':     {'state': 'degrading',    'is_failure': 0, 'is_pre_failure': 1},
        'anomaly_detected':     {'state': 'pre_failure',  'is_failure': 0, 'is_pre_failure': 1},
        'critical_failure':     {'state': 'failure',      'is_failure': 1, 'is_pre_failure': 0},
        'scheduled_maintenance':{'state': 'maintenance',  'is_failure': 0, 'is_pre_failure': 0}
    }
    
    # Aplica o mapeamento
    eventos_df['machine_state'] = eventos_df['info'].map(
        lambda x: mapeamento_info.get(x, {}).get('state', 'unknown')
    )
    eventos_df['is_failure'] = eventos_df['info'].map(
        lambda x: mapeamento_info.get(x, {}).get('is_failure', 0)
    )
    eventos_df['is_pre_failure'] = eventos_df['info'].map(
        lambda x: mapeamento_info.get(x, {}).get('is_pre_failure', 0)
    )
    
    print(f"\n🏷️ LABELS DE FALHA CRIADAS:")
    print(f"   Falhas reais (is_failure): {eventos_df['is_failure'].sum():,}")
    print(f"   Pré-falhas (is_pre_failure): {eventos_df['is_pre_failure'].sum():,}")
    
    return eventos_df


# ============================================================
# TRANSFORMAÇÃO PARA FORMATO ML (PIVOT)
# ============================================================

def pivot_for_ml(eventos_df, aggregate_by='10min'):
    """
    Transforma dados de séries temporais em formato tabular para ML.
    
    O pivô transforma cada timestamp em uma linha, com cada sensor
    em uma coluna diferente. Isso é o formato esperado pelos modelos
    de Machine Learning.
    
    Args:
        eventos_df: DataFrame com eventos (time, tag, value, location_id, etc.)
        aggregate_by: Janela de agregação (ex: '10min', '1h', '1d')
        
    Returns:
        DataFrame no formato ML (uma linha por timestamp, uma coluna por sensor)
    """
    
    if eventos_df.empty:
        return pd.DataFrame()
    
    print(f"\n🔄 Transformando dados para formato ML...")
    print(f"   Agregação por: {aggregate_by}")
    
    # Cria janela de tempo para agregação
    eventos_df['time_window'] = eventos_df['time'].dt.floor(aggregate_by)
    
    # Pivô: cada tag vira uma coluna (usa média dos valores no período)
    valores_pivot = eventos_df.pivot_table(
        index=['time_window', 'location_id', 'location_name'],
        columns='tag',
        values='value',
        aggfunc='mean'
    ).reset_index()
    
    # Agrega as labels (pega o máximo: se houve falha em qualquer minuto)
    falhas_agg = eventos_df.groupby(['time_window', 'location_id'])['is_failure'].max().reset_index()
    pre_falhas_agg = eventos_df.groupby(['time_window', 'location_id'])['is_pre_failure'].max().reset_index()
    
    # Estado da máquina (moda = o estado mais frequente no período)
    estado_agg = eventos_df.groupby(['time_window', 'location_id'])['machine_state'].agg(
        lambda x: x.mode()[0] if len(x) > 0 else 'normal'
    ).reset_index()
    
    # Junta tudo em um único DataFrame
    ml_df = valores_pivot
    ml_df = ml_df.merge(falhas_agg, on=['time_window', 'location_id'], how='left')
    ml_df = ml_df.merge(pre_falhas_agg, on=['time_window', 'location_id'], how='left')
    ml_df = ml_df.merge(estado_agg, on=['time_window', 'location_id'], how='left')
    
    # Renomeia colunas
    ml_df = ml_df.rename(columns={
        'time_window': 'timestamp',
        'is_failure': 'is_failure',
        'is_pre_failure': 'is_pre_failure',
        'machine_state': 'machine_state'
    })
    
    # Preenche valores nulos (sensores que não tiveram leitura no período)
    ml_df = ml_df.fillna(0)
    
    print(f"\n📊 DADOS TRANSFORMADOS PARA ML:")
    print(f"   Shape: {ml_df.shape}")
    print(f"   Total janelas de tempo: {ml_df['timestamp'].nunique()}")
    print(f"   Máquinas no dataset: {ml_df['location_name'].nunique()}")
    
    if len(ml_df) > 0:
        print(f"   Período: {ml_df['timestamp'].min()} a {ml_df['timestamp'].max()}")
    
    return ml_df


# ============================================================
# FUNÇÃO PRINCIPAL COM MODO INCREMENTAL
# ============================================================

def executar_pipeline(modo_incremental=True, dias_histórico=90):
    """
    Executa o pipeline de carregamento e transformação dos dados.
    
    Args:
        modo_incremental: 
            - True: adiciona apenas dados novos (mais eficiente)
            - False: recria o dataset do zero (full load)
        dias_histórico: Quantos dias de histórico carregar (padrão: 90)
    """
    
    print("=" * 70)
    print("🏭 PREDICTIVE MAINTENANCE - CARREGADOR DE DADOS")
    print("=" * 70)
    
    print(f"\n⚙️ CONFIGURAÇÕES:")
    print(f"   Modo incremental: {modo_incremental}")
    print(f"   Dias de histórico: {dias_histórico}")
    
    # ============================================================
    # 1. LISTAR TODAS AS MÁQUINAS
    # ============================================================
    maquinas_df = load_all_machines()
    
    # ============================================================
    # 2. CARREGAR TODAS AS TAGS
    # ============================================================
    tags_df = load_all_tags()
    
    # ============================================================
    # 3. DEFINIR PERÍODO (com suporte a incremental)
    # ============================================================
    data_fim = datetime.now()
    
    if modo_incremental:
        # Busca o último timestamp no dataset existente
        ultimo_timestamp = get_ultimo_timestamp_dataset()
        
        if ultimo_timestamp:
            # Adiciona 1 minuto para não pegar dados duplicados
            data_inicio = ultimo_timestamp + timedelta(minutes=1)
            print(f"\n📅 Modo incremental: buscando dados a partir de {data_inicio}")
        else:
            # Dataset não existe, faz full load
            print(f"\n📅 Dataset não encontrado. Fazendo full load...")
            data_inicio = data_fim - timedelta(days=dias_histórico)
            modo_incremental = False  # De facto, será full load
    else:
        # Modo full load: carrega X dias
        data_inicio = data_fim - timedelta(days=dias_histórico)
        print(f"\n📅 Modo full load: {dias_histórico} dias de histórico")
    
    print(f"   Período de análise: {data_inicio.date()} a {data_fim.date()}")
    
    # ============================================================
    # 4. CARREGAR EVENTOS
    # ============================================================
    eventos_df = load_all_events(
        start_date=data_inicio,
        end_date=data_fim,
        limit=None
    )
    
    if not eventos_df.empty:
        # ============================================================
        # 5. CRIAR LABELS DE FALHA
        # ============================================================
        eventos_df = create_failure_labels(eventos_df)
        
        # ============================================================
        # 6. TRANSFORMAR PARA FORMATO ML
        # ============================================================
        ml_df_novo = pivot_for_ml(eventos_df, aggregate_by='10min')
        
        if not ml_df_novo.empty:
            
            # ============================================================
            # 7. CONCATENAR COM DATASET EXISTENTE (SE FOR INCREMENTAL)
            # ============================================================
            if modo_incremental and os.path.exists('data_ml_complete.parquet'):
                df_existente = pd.read_parquet('data_ml_complete.parquet')
                print(f"\n📦 Dataset existente: {len(df_existente)} registros")
                print(f"   Novos registros: {len(ml_df_novo)}")
                
                # Concatena e remove duplicatas (por timestamp e location_id)
                ml_df_final = pd.concat([df_existente, ml_df_novo], ignore_index=True)
                ml_df_final = ml_df_final.drop_duplicates(
                    subset=['timestamp', 'location_id'],
                    keep='last'
                )
                print(f"   Total após merge: {len(ml_df_final)} registros")
            else:
                ml_df_final = ml_df_novo
            
            # ============================================================
            # 8. SALVAR DATASET
            # ============================================================
            ml_df_final.to_parquet('data_ml_complete.parquet', index=False)
            ml_df_final.to_csv('data_ml_complete.csv', index=False)
            
            print("\n" + "=" * 70)
            print("💾 DATASET COMPLETO SALVO!")
            print("=" * 70)
            print("   - data_ml_complete.parquet (formato otimizado)")
            print("   - data_ml_complete.csv (para debug)")
            
            print(f"\n📋 PREVIEW DOS DADOS:")
            print(ml_df_final.head(10))
            
            print(f"\n📊 ESTATÍSTICAS DO DATASET:")
            print(f"   Total registros: {len(ml_df_final):,}")
            print(f"   Máquinas: {ml_df_final['location_name'].nunique()}")
            print(f"   Colunas: {len(ml_df_final.columns)}")
            
            # Mostra distribuição das classes
            falhas = ml_df_final['is_failure'].sum()
            pre_falhas = ml_df_final['is_pre_failure'].sum()
            print(f"\n🎯 DISTRIBUIÇÃO DAS CLASSES:")
            print(f"   Falhas (is_failure): {falhas} ({falhas/len(ml_df_final)*100:.2f}%)")
            print(f"   Pré-falhas (is_pre_failure): {pre_falhas} ({pre_falhas/len(ml_df_final)*100:.2f}%)")
            
        else:
            print("\n⚠️ Erro ao transformar dados para ML")
    else:
        print("\n⚠️ NENHUM EVENTO ENCONTRADO!")
        print("   Execute o generator.py primeiro para gerar dados.")
    
    print("\n" + "=" * 70)
    print("✅ PROCESSO CONCLUÍDO!")
    print("=" * 70)


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

if __name__ == "__main__":
    
    # Exemplo 1: Modo incremental (só adiciona dados novos)
    executar_pipeline(modo_incremental=True, dias_histórico=90)
    
    # Exemplo 2: Modo full load (recria tudo do zero)
    # executar_pipeline(modo_incremental=False, dias_histórico=90)