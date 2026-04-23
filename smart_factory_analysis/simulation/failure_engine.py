"""
failure_engine.py

Engine de falhas correlacionadas (industrial simulation)

Este módulo implementa as correlações físicas entre sensores:
    - Pressão alta → Temperatura sobe
    - Vácuo baixo → Temperatura sobe
    - Vibração alta → Corrente elétrica sobe
    - Vazão instável → Produção cai
"""

import random
from config import LABEL_RANGES


# ============================================================
# CORE FAILURE LOGIC
# ============================================================

def apply_failure_effects(state, base_values):
    """
    Aplica efeitos de falha correlacionada entre sensores.
    
    Args:
        state: Estado atual da máquina (normal, degrading, pre_failure, failure)
        base_values: Dicionário com valores base dos sensores
        
    Returns:
        Dicionário com valores atualizados após aplicar efeitos
    """
    
    values = base_values.copy()
    
    # ============================================================
    # DEGRADAÇÃO FÍSICA CORRELACIONADA
    # ============================================================
    # Só aplica efeitos se a máquina não estiver normal
    if state in ["degrading", "pre_failure", "failure"]:
        
        # Correlação 1: pressão afeta temperatura
        if "pressure" in values and "temperature" in values:
            # Cada 1 bar de pressão aumenta temperatura em 0.8°C
            values["temperature"] += values["pressure"] * 0.8
        
        # Correlação 2: vácuo ruim aumenta temperatura
        if "vacuum" in values and "temperature" in values:
            # Quanto pior o vácuo (valor maior), mais a temperatura sobe
            vacuum_factor = max(0, values["vacuum"] - 70)
            values["temperature"] += vacuum_factor * 0.3
        
        # Correlação 3: vibração afeta corrente
        if "vibration" in values and "current" in values:
            # Cada 1 mm/s de vibração aumenta corrente em 0.6A
            values["current"] += values["vibration"] * 0.6
        
        # Correlação 4: fluxo instável reduz produção
        if "flow" in values and "production" in values:
            # Se o fluxo está baixo, produção cai
            if values["flow"] < 70:
                values["production"] *= random.uniform(0.7, 0.95)
    
    # ============================================================
    # FALHA CRÍTICA - TUDO CAI
    # ============================================================
    if state == "failure":
        for key in values:
            if isinstance(values[key], (int, float)):
                # Mantém apenas 10% a 30% do valor original
                values[key] *= random.uniform(0.1, 0.3)
    
    return values


def apply_sensor_drift(values, state):
    """
    Simula drift lento de sensores ao longo do tempo (envelhecimento).
    
    Args:
        values: Dicionário com valores dos sensores
        state: Estado atual da máquina
        
    Returns:
        Dicionário com valores afetados pelo drift
    """
    
    drift_factor = {
        "normal": 0.0,
        "degrading": 0.02,
        "pre_failure": 0.05,
        "failure": 0.1
    }.get(state, 0.0)
    
    if drift_factor == 0.0:
        return values.copy()
    
    drifted = {}
    
    for k, v in values.items():
        if isinstance(v, (int, float)):
            noise = random.uniform(-drift_factor, drift_factor)
            drifted[k] = v * (1 + noise)
        else:
            drifted[k] = v
    
    return drifted


def process_failure_pipeline(state, base_values):
    """
    Pipeline completo de falha:
        1. Aplica correlação física entre sensores
        2. Aplica drift de sensores (envelhecimento)
    
    Args:
        state: Estado atual da máquina
        base_values: Valores base dos sensores
        
    Returns:
        Valores finais após aplicar todos os efeitos
    """
    
    values = apply_failure_effects(state, base_values)
    values = apply_sensor_drift(values, state)
    
    return values


# ============================================================
# TESTE LOCAL
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("TESTE DO MOTOR DE FALHAS CORRELACIONADAS")
    print("=" * 60)
    
    # Teste: máquina em degradação
    print("\n📊 Teste: Máquina em estado 'degrading'")
    valores_base = {
        "pressure": 6.0,
        "temperature": 62.0,
        "vibration": 3.5,
        "current": 25.0
    }
    
    resultado = process_failure_pipeline("degrading", valores_base)
    print(f"   Entrada: {valores_base}")
    print(f"   Saída:   {resultado}")
    
    print("\n" + "=" * 60)
    print("✅ TESTE CONCLUÍDO")
    print("=" * 60)