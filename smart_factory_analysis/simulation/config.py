"""
config.py

Configurações centrais do simulador industrial.

Este arquivo contém todos os parâmetros que controlam o comportamento da simulação:
- Ciclos de falha das máquinas
- Faixas de valores para sensores
- Probabilidades de alarmes
- Duração de campanhas de produção

NENHUM nome de variável foi alterado - todos mantidos originais.
"""

from datetime import timedelta

# ============================================================
# 1. CICLO DE FALHAS (FAILURE CYCLE)
# ============================================================

# Número de dias que uma máquina leva para completar um ciclo completo
FAILURE_CYCLE_DAYS = 10

# Lista completa de estados possíveis
MACHINE_STATES = [
    "normal",           # Operação saudável
    "degrading",        # Performance caindo, mas ainda operando
    "pre_failure",      # Padrões anormais detectados
    "failure",          # Parada crítica
    "maintenance",      # Manutenção planejada
    "recovery"          # Voltando à operação normal
]


# ============================================================
# 2. CAMPANHA DE PRODUÇÃO (SKU CAMPAIGN)
# ============================================================

# Duração mínima e máxima que um mesmo SKU pode rodar (em horas)
SKU_CAMPAIGN_HOURS = {
    "min": 8,    # Mínimo de 8 horas (1 turno)
    "max": 72    # Máximo de 72 horas (3 dias)
}

# Tempo gasto para trocar de SKU (setup da linha) - em minutos
SETUP_DURATION_MINUTES = {
    "min": 30,   # Setup rápido (30 minutos)
    "max": 180   # Setup demorado (3 horas)
}


# ============================================================
# 3. FREQUÊNCIA DE GERAÇÃO POR TIPO DE SENSOR
# ============================================================

# Define com que frequência cada tipo de dado é gerado
EVENT_FREQUENCY = {
    # Sensores críticos (amostragem alta)
    "temperature": "1min",
    "pressure": "1min",
    "flow": "1min",
    "vibration": "1min",
    "current": "1min",
    "vacuum": "1min",
    "level": "2min",
    
    # Contadores (amostragem média)
    "production": "5min",
    "runtime": "5min",
    
    # Contadores de falha (amostragem baixa)
    "failure_counter": "10min",
    "trip_counter": "10min",
    
    # Eventos discretos (só quando mudam)
    "alarm": "on_change",
    "maintenance": "on_change",
    "status": "on_change",
    "current_sku": "on_change",
    "setup_change": "on_change"
}


# ============================================================
# 4. FAIXAS DE VALORES DOS SENSORES POR ESTADO (LABEL RANGES)
# ============================================================

LABEL_RANGES = {
    
    # Temperatura (°C)
    "temperature": {
        "normal":      (55, 65),
        "degrading":   (66, 75),
        "pre_failure": (76, 88),
        "failure":     (90, 110)
    },
    
    # Pressão (bar)
    "pressure": {
        "normal":      (3.5, 5.0),
        "degrading":   (5.1, 6.2),
        "pre_failure": (6.3, 7.5),
        "failure":     (7.6, 10.0)
    },
    
    # Vácuo (mbar_abs)
    "vacuum": {
        "normal":      (55, 70),
        "degrading":   (71, 85),
        "pre_failure": (86, 100),
        "failure":     (101, 120)
    },
    
    # Vazão (kg/h ou m³/h)
    "flow": {
        "normal":      (80, 120),
        "degrading":   (70, 79),
        "pre_failure": (55, 69),
        "failure":     (0, 54)
    },
    
    # Vibração (mm/s)
    "vibration": {
        "normal":      (1.0, 2.5),
        "degrading":   (2.6, 4.0),
        "pre_failure": (4.1, 6.0),
        "failure":     (6.1, 10.0)
    },
    
    # Corrente elétrica (A)
    "current": {
        "normal":      (18, 28),
        "degrading":   (29, 35),
        "pre_failure": (36, 42),
        "failure":     (43, 55)
    },
    
    # Nível de tanque (%)
    "level": {
        "normal":      (45, 85),
        "degrading":   (35, 44),
        "pre_failure": (20, 34),
        "failure":     (0, 19)
    }
}


# ============================================================
# 5. INCREMENTOS DE CONTADORES (COUNTER INCREMENT)
# ============================================================

COUNTER_INCREMENT = {
    "production":      (10, 120),   # 10 a 120 unidades
    "runtime":         (1, 5),      # 1 a 5 minutos
    "failure_counter": (0, 1),      # 0 ou 1
    "trip_counter":    (0, 1)       # 0 ou 1
}


# ============================================================
# 6. PROBABILIDADES DE ALARME (ALARM PROBABILITY)
# ============================================================

# Chance de um alarme ser gerado (valor entre 0 e 1)
ALARM_PROBABILITY = {
    "normal":      0.01,   # 1% de chance
    "degrading":   0.08,   # 8% de chance
    "pre_failure": 0.30,   # 30% de chance
    "failure":     0.90    # 90% de chance
}

# Estados que disparam manutenção automática
MAINTENANCE_TRIGGER_STATES = ["failure", "pre_failure"]


# ============================================================
# 7. MENSAGENS DE INFORMAÇÃO (INFO MESSAGES)
# ============================================================

INFO_MESSAGES = {
    "normal":      "operacao_normal",
    "degrading":   "degradacao_performance",
    "pre_failure": "padrao_pre_falha",
    "failure":     "parada_critica",
    "maintenance": "manutencao_planejada",
    "recovery":    "recuperacao_pos_manutencao"
}


# ============================================================
# 8. CONFIGURAÇÕES DE SIMULAÇÃO
# ============================================================

# Data a partir da qual a simulação começa (quando não especificada)
DEFAULT_START_DATE = "2026-01-01 00:00:00"

# Quantos dias de dados gerar por vez (lote)
BATCH_WINDOW_DAYS = 30


# ============================================================
# 9. SKUS DISPONÍVEIS PARA PRODUÇÃO
# ============================================================

AVAILABLE_SKUS = [1, 2, 3, 4, 5]