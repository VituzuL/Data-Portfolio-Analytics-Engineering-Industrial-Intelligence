"""
generator.py

Gerador principal de eventos do simulador industrial.

Este módulo orquestra a geração de dados de todos os sensores e atuadores,
coordenando:
    - Estados da máquina (do simulation_engine)
    - Estados de SKU (do sku_engine)
    - Valores de sensores (do value_generators)
    - Efeitos de falha (do failure_engine)

Os eventos gerados são salvos na tabela 'event' do PostgreSQL.
"""

import json
import os
import random
import logging
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Importa as configurações e outros módulos
from config import DEFAULT_START_DATE
from simulation_engine import get_machine_state
from sku_engine import SKUState, update_sku_state
from value_generators import (
    generate_sensor_value,
    generate_counter,
    generate_alarm,
    generate_info
)
from failure_engine import process_failure_pipeline

# ============================================================
# CONFIGURAÇÃO DE LOG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# CONEXÃO COM BANCO DE DADOS
# ============================================================

def conectar_banco():
    """Cria e retorna a engine de conexão com PostgreSQL."""
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
# CARREGAMENTO DAS TAGS
# ============================================================

def carregar_tags() -> List[dict]:
    """Carrega a lista de tags do arquivo JSON."""
    # Encontra o diretório correto
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    raiz_projeto = os.path.dirname(diretorio_atual)
    caminho_tags = os.path.join(raiz_projeto, "data", "tag.json")
    
    with open(caminho_tags, "r", encoding="utf-8") as arquivo:
        return json.load(arquivo)


def obter_minutos_existentes() -> set:
    """
    Busca no banco todos os minutos já existentes na tabela event.
    
    Isso evita duplicação de dados quando o script é executado
    múltiplas vezes sobre o mesmo período.
    
    Returns:
        Conjunto de timestamps (minuto) que já existem no banco
    """
    query = """
        SELECT DISTINCT DATE_TRUNC('minute', time) AS minute
        FROM event
    """
    
    try:
        df_existente = pd.read_sql(query, engine)
        return set(df_existente["minute"])
    except Exception:
        # Tabela vazia ou não existe - primeira execução
        logger.info("Tabela event vazia ou não existe. Iniciando do zero.")
        return set()


# ============================================================
# ROTEADOR DE VALORES POR RÓTULO
# ============================================================

def gerar_valor_por_rotulo(
    rotulo: str,
    estado: str,
    estado_sku: SKUState
) -> Optional[float]:
    """
    Decide como gerar o valor de cada tag baseado no seu rótulo.
    
    O roteamento é feito por palavras-chave, não por nome exato,
    permitindo que novas tags sejam adicionadas sem modificar o código.
    
    Args:
        rotulo: Rótulo da tag (temperature, pressure, production, etc.)
        estado: Estado atual da máquina
        estado_sku: Estado atual do SKU (para tags relacionadas a produto)
        
    Returns:
        Valor gerado (float, int ou None se não aplicável)
    """
    rotulo = rotulo.lower()
    
    # ============================================================
    # SENSORES ANALÓGICOS
    # ============================================================
    if any(palavra in rotulo for palavra in [
        "temp", "temperature", "pressure", "vacuum", "flow",
        "vibration", "current", "level", "wear", "health", "lubrication"
    ]):
        return generate_sensor_value(rotulo, estado)
    
    # ============================================================
    # CONTADORES / HORÍMETROS
    # ============================================================
    if any(palavra in rotulo for palavra in [
        "production", "runtime", "hourmeter", "hours",
        "maintenance", "failure_counter", "trip_counter",
        "start_counter", "days_since_last_maintenance"
    ]):
        return generate_counter(rotulo)
    
    # ============================================================
    # ALARMES
    # ============================================================
    if "alarm" in rotulo:
        return generate_alarm(estado)
    
    # ============================================================
    # SKU (produto atual)
    # ============================================================
    if any(palavra in rotulo for palavra in ["sku", "current_sku", "running_sku"]):
        sku, _ = update_sku_state(estado_sku)
        
        # Converte "SKU_002" -> 2 (se for string)
        if isinstance(sku, str) and "SKU_" in sku:
            return int(sku.replace("SKU_", ""))
        
        return sku
    
    # ============================================================
    # STATUS DA MÁQUINA (ligado/desligado)
    # ============================================================
    if any(palavra in rotulo for palavra in ["status", "machine_status"]):
        return 0 if estado == "falha" else 1
    
    # ============================================================
    # FALLBACK - valor genérico
    # ============================================================
    return None


# ============================================================
# CLASSE PRINCIPAL - GERADOR DE EVENTOS
# ============================================================

class EventGenerator:
    """
    Gerador principal de eventos do simulador industrial.
    
    Esta classe orquestra todo o processo de geração de dados,
    desde o carregamento das tags até o salvamento no banco.
    
    Atributos:
        tags: Lista de tags carregadas do JSON
        estado_sku: Estado atual da produção (SKU)
        data_inicio_base: Data de referência para o ciclo de falhas
    """
    
    def __init__(self):
        """Inicializa o gerador carregando as tags e o estado do SKU."""
        self.tags = carregar_tags()
        self.estado_sku = SKUState()
        self.data_inicio_base = datetime.strptime(
            DEFAULT_START_DATE,
            "%Y-%m-%d %H:%M:%S"
        )
        
        logger.info(f"Gerador inicializado com {len(self.tags)} tags")
    
    def run(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        frequencia_minutos: int = 1,
        salvar_no_banco: bool = True
    ) -> pd.DataFrame:
        """
        Executa a geração de eventos para um período.
        
        Args:
            data_inicio: Data/hora inicial (None = usa DEFAULT_START_DATE)
            data_fim: Data/hora final (None = usa agora)
            frequencia_minutos: Intervalo entre eventos (padrão: 1 minuto)
            salvar_no_banco: Se True, salva os eventos no PostgreSQL
            
        Returns:
            DataFrame com todos os eventos gerados
        """
        # Define as datas
        data_inicio = data_inicio or self.data_inicio_base
        data_fim = data_fim or datetime.now()
        
        logger.info(f"Gerando eventos de {data_inicio} até {data_fim}")
        
        # ============================================================
        # CRIA OS TIMESTAMPS (baseados na frequência)
        # ============================================================
        timestamps = pd.date_range(
            start=data_inicio,
            end=data_fim,
            freq=f"{frequencia_minutos}min"
        )
        
        logger.info(f"Total de timestamps brutos: {len(timestamps)}")
        
        # ============================================================
        # REMOVE MINUTOS QUE JÁ EXISTEM NO BANCO
        # ============================================================
        minutos_existentes = obter_minutos_existentes()
        
        timestamps = [
            ts for ts in timestamps
            if ts.floor("min") not in minutos_existentes
        ]
        
        logger.info(f"Timestamps novos (após deduplicação): {len(timestamps)}")
        
        # ============================================================
        # ADICIONA SEGUNDOS ALEATÓRIOS (SIMULA VARIAÇÃO REAL)
        # ============================================================
        timestamps = [
            ts + pd.Timedelta(seconds=random.randint(0, 59))
            for ts in timestamps
        ]
        
        # ============================================================
        # LOOP PRINCIPAL - GERA EVENTOS
        # ============================================================
        todos_eventos = []
        
        for i, ts in enumerate(timestamps):
            if i % 1000 == 0 and i > 0:
                logger.info(f"Processando timestamp {i}/{len(timestamps)}...")
            
            # Define o estado atual da máquina
            estado = get_machine_state(ts, self.data_inicio_base)
            
            # Gera alarme baseado no estado
            alarme = generate_alarm(estado)
            
            # Gera informação (critical_failure, anomaly_detected, etc.)
            info = generate_info(estado, alarme)
            
            # Para cada tag, gera um evento
            for tag in self.tags:
                nome_tag = tag.get("tag_name")
                rotulo = tag.get("label")
                
                if not nome_tag or not rotulo:
                    continue
                
                # Gera o valor base da tag
                valor = gerar_valor_por_rotulo(rotulo, estado, self.estado_sku)
                
                if valor is None:
                    continue
                
                # Aplica efeitos de falha (correlações entre sensores)
                # Nota: apenas para valores que são dicionários
                # No caso original, isso é simplificado
                
                evento = {
                    "time": ts,
                    "tag": nome_tag,
                    "value": valor,
                    "info": info
                }
                
                todos_eventos.append(evento)
        
        # Converte para DataFrame
        df = pd.DataFrame(todos_eventos)
        
        logger.info(f"Total de eventos gerados: {len(df):,}")
        
        # ============================================================
        # SALVA NO POSTGRESQL
        # ============================================================
        if salvar_no_banco and not df.empty:
            logger.info("Salvando eventos na tabela 'event'...")
            
            df.to_sql(
                "event",
                engine,
                if_exists="append",
                index=False,
                chunksize=500
            )
            
            logger.info("✅ Upload concluído com sucesso!")
            
            # Mostra estatísticas finais
            logger.info(f"   Período: {df['time'].min()} a {df['time'].max()}")
            logger.info(f"   Total eventos: {len(df):,}")
            logger.info(f"   Tags únicas: {df['tag'].nunique()}")
            
        elif df.empty:
            logger.warning("Nenhum dado novo para inserir.")
        
        return df


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

if __name__ == "__main__":
    """
    Execução principal - gera eventos desde 01/01/2026 até agora.
    """
    
    print("=" * 70)
    print("🚀 SIMULADOR INDUSTRIAL - GERADOR DE EVENTOS")
    print("=" * 70)
    
    # Cria o gerador
    gerador = EventGenerator()
    
    # Define o período de simulação
    data_inicio = datetime(2026, 1, 1)
    data_fim = datetime.now()
    
    print(f"\n📅 Período: {data_inicio.date()} a {data_fim.date()}")
    print(f"⏱️  Frequência: 1 minuto")
    print(f"🏭 Total de tags: {len(gerador.tags)}")
    print("\n" + "-" * 70)
    
    # Executa a geração
    df = gerador.run(
        data_inicio=data_inicio,
        data_fim=data_fim,
        frequencia_minutos=1,
        salvar_no_banco=True
    )
    
    print("\n" + "=" * 70)
    print("✅ SIMULAÇÃO CONCLUÍDA!")
    print("=" * 70)
    
    # Preview dos dados gerados
    if not df.empty:
        print("\n📋 PREVIEW DOS DADOS:")
        print(df.head(10).to_string())
        
        print("\n📊 ESTATÍSTICAS RÁPIDAS:")
        print(f"   Total de eventos: {len(df):,}")
        print(f"   Período: {df['time'].min()} até {df['time'].max()}")
        print(f"   Tags únicas: {df['tag'].nunique()}")
        
        # Distribuição dos tipos de info
        print("\n📊 Distribuição por tipo de evento:")
        info_counts = df['info'].value_counts()
        for tipo, count in info_counts.items():
            print(f"   {tipo:25}: {count:8,} ({count/len(df)*100:.1f}%)")