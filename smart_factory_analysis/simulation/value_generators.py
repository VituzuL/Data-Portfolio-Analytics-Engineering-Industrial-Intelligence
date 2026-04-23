"""
value_generators.py

Geradores de valores para sensores, contadores, alarmes e informações.

Este módulo é responsável por gerar os valores numéricos e categóricos
que alimentam a tabela 'event' do banco de dados.

Cada tipo de dado tem seu próprio gerador:
    - Sensores analógicos (temperatura, pressão, vibração, etc.)
    - Contadores (produção, tempo de operação, falhas)
    - Alarmes (binário: 0 ou 1)
    - Status (máquina ligada/desligada)
"""

import random
from typing import Union

# ============================================================
# CONTADORES GLOBAIS (ACUMULAM AO LONGO DA SIMULAÇÃO)
# ============================================================
# Estes contadores mantêm estado entre chamadas de função.
# Eles simulam hodômetros e contadores de produção reais.

contador_producao = 0      # Total produzido (unidades ou kg)
contador_tempo_operacao = 0  # Horímetro total (horas)
contador_manutencao = 0    # Número de manutenções executadas


# ============================================================
# GERADOR DE SENSORES ANALÓGICOS
# ============================================================

def generate_sensor_value(rotulo: str, estado: str) -> float:
    """
    Gera valor para um sensor analógico baseado no rótulo e estado.
    
    Suporta diversos tipos de sensores via palavras-chave:
        - temperatura / temp
        - pressure / pressao
        - vibration / vibracao
        - flow / vazao
        - vacuum / vacuo
        - current / corrente
        - level / nivel
    
    Args:
        rotulo: Rótulo do sensor (ex: "temperature", "pressure")
        estado: Estado atual da máquina (normal, degradando, etc.)
        
    Returns:
        Valor numérico do sensor (float)
        
    Exemplo:
        >>> generate_sensor_value("temperature", "degradando")
        72.4  # Temperatura elevada devido à degradação
    """
    
    global contador_tempo_operacao, contador_manutencao
    
    rotulo = rotulo.lower()
    
    # ============================================================
    # 1. ALARMES (tag com "alarm" no nome)
    # ============================================================
    if "alarm" in rotulo:
        if estado in ["pre_falha", "falha"]:
            return 1
        return 0
    
    # ============================================================
    # 2. STATUS DA MÁQUINA (tag com "status" no nome)
    # ============================================================
    if "status" in rotulo:
        # 0 = desligado (falha), 1 = ligado (operando normalmente)
        if estado == "falha":
            return 0
        return 1
    
    # ============================================================
    # 3. TEMPERATURA
    # ============================================================
    # Quanto pior o estado, maior a temperatura
    if "temp" in rotulo or "temperature" in rotulo:
        faixas = {
            "normal":      (210, 225),
            "degradando":  (225, 240),
            "pre_falha":   (240, 255),
            "falha":       (255, 280),
            "manutencao":  (0, 0)      # Durante manutenção, desligado
        }
        minimo, maximo = faixas.get(estado, (200, 220))
        
        if estado == "manutencao":
            return 0.0
        return round(random.uniform(minimo, maximo), 2)
    
    # ============================================================
    # 4. PRESSÃO
    # ============================================================
    # Quanto pior o estado, maior a pressão (até explodir)
    if "pressure" in rotulo:
        faixas = {
            "normal":      (4.0, 5.2),
            "degradando":  (5.0, 6.5),
            "pre_falha":   (6.5, 8.0),
            "falha":       (8.0, 10.0),
            "manutencao":  (0, 0)
        }
        minimo, maximo = faixas.get(estado, (4, 5))
        
        if estado == "manutencao":
            return 0.0
        return round(random.uniform(minimo, maximo), 2)
    
    # ============================================================
    # 5. VIBRAÇÃO
    # ============================================================
    # Vibração excessiva indica desgaste mecânico
    if "vibration" in rotulo:
        faixas = {
            "normal":      (1.0, 2.5),
            "degradando":  (2.5, 4.5),
            "pre_falha":   (4.5, 7.0),
            "falha":       (7.0, 12.0),
            "manutencao":  (0, 0)
        }
        minimo, maximo = faixas.get(estado, (1, 3))
        
        if estado == "manutencao":
            return 0.0
        return round(random.uniform(minimo, maximo), 2)
    
    # ============================================================
    # 6. VAZÃO
    # ============================================================
    if "flow" in rotulo:
        return round(random.uniform(70, 120), 2)
    
    # ============================================================
    # 7. VÁCUO
    # ============================================================
    if "vacuum" in rotulo:
        return round(random.uniform(55, 100), 2)
    
    # ============================================================
    # 8. HORÍMETRO / TEMPO DE OPERAÇÃO
    # ============================================================
    # Acumula continuamente, como um hodômetro de carro
    if "runtime" in rotulo or "hours" in rotulo or "horimetro" in rotulo:
        contador_tempo_operacao += random.uniform(0.8, 1.2)
        return round(contador_tempo_operacao, 2)
    
    # ============================================================
    # 9. CONTADOR DE MANUTENÇÕES
    # ============================================================
    if "maintenance" in rotulo or "failure_counter" in rotulo:
        contador_manutencao += random.randint(0, 1)
        return contador_manutencao
    
    # ============================================================
    # 10. DESGASTE (WEAR) - Percentual de vida útil restante
    # ============================================================
    if "wear" in rotulo or "desgaste" in rotulo:
        return round(random.uniform(10, 90), 2)
    
    # ============================================================
    # 11. SAÚDE DO ATIVO - Quanto maior, melhor
    # ============================================================
    if "health" in rotulo or "saude" in rotulo:
        return round(random.uniform(70, 100), 2)
    
    # ============================================================
    # 12. LUBRIFICAÇÃO - Percentual de óleo/graxa
    # ============================================================
    if "lubrication" in rotulo:
        return round(random.uniform(0, 100), 2)
    
    # ============================================================
    # 13. FALLBACK - Qualquer outro sensor não mapeado
    # ============================================================
    return round(random.uniform(1, 100), 2)


# ============================================================
# GERADOR DE CONTADORES (PRODUÇÃO)
# ============================================================

def generate_counter(rotulo: str) -> float:
    """
    Gera valores para contadores de produção.
    
    O contador de produção aumenta gradualmente a cada chamada,
    simulando a produção contínua de uma fábrica.
    
    Args:
        rotulo: Rótulo do contador (ex: "production")
        
    Returns:
        Valor atualizado do contador
        
    Exemplo:
        >>> generate_counter("production")
        0.0  # Primeira chamada
        >>> generate_counter("production")
        35.2  # Segunda chamada, aumentou
    """
    global contador_producao
    
    if rotulo == "production":
        # Incremento aleatório entre 15 e 45 unidades
        incremento = random.uniform(15, 45)
        contador_producao += incremento
        return round(contador_producao, 2)
    
    # Para outros contadores não implementados
    return 0


# ============================================================
# GERADOR DE ALARMES
# ============================================================

def generate_alarm(estado: str) -> int:
    """
    Gera flag de alarme (0 ou 1) baseado no estado da máquina.
    
    Args:
        estado: Estado atual da máquina
        
    Returns:
        1 se alarme ativo, 0 caso contrário
    """
    # Em pré-falha ou falha, alarme é quase certo
    if estado in ["pre_falha", "falha"]:
        return 1
    
    # 3% de chance de alarme falso em operação normal
    if random.random() < 0.03:
        return 1
    
    return 0


# ============================================================
# GERADOR DE INFORMAÇÕES (CAMPO INFO)
# ============================================================

def generate_info(estado: str, alarme: int) -> str:
    """
    Gera a mensagem de informação para o campo 'info' da tabela event.
    
    Esta mensagem é usada como label para classificação de falhas.
    
    Mapeamento:
        - falha → "critical_failure"
        - pre_falha → "anomaly_detected"
        - manutencao → "scheduled_maintenance"
        - alarme ativo → "warning_detected"
        - normal → "normal"
    
    Args:
        estado: Estado atual da máquina
        alarme: Flag de alarme (0 ou 1)
        
    Returns:
        String com a mensagem de informação
    """
    
    # Prioridade: falha é o mais grave
    if estado == "falha":
        return "critical_failure"
    
    # Pré-falha indica anomalia detectada
    if estado == "pre_falha":
        return "anomaly_detected"
    
    # Manutenção planejada
    if estado == "manutencao":
        return "scheduled_maintenance"
    
    # Alarme ativo = aviso (warning)
    if alarme == 1:
        return "warning_detected"
    
    # Operação normal
    return "normal"


# ============================================================
# FUNÇÃO PARA RESETAR CONTADORES (ÚTIL PARA TESTES)
# ============================================================

def resetar_contadores() -> None:
    """
    Reseta todos os contadores globais para zero.
    
    Útil para testes ou para reiniciar a simulação.
    """
    global contador_producao, contador_tempo_operacao, contador_manutencao
    
    contador_producao = 0
    contador_tempo_operacao = 0
    contador_manutencao = 0


# ============================================================
# TESTE LOCAL
# ============================================================

if __name__ == "__main__":
    """
    Testa todos os geradores para verificar comportamento.
    Executar: python value_generators.py
    """
    
    print("=" * 60)
    print("TESTE DOS GERADORES DE VALORES")
    print("=" * 60)
    
    # Teste 1: Sensores em diferentes estados
    print("\n📊 TESTE 1: Sensores por estado")
    estados = ["normal", "degradando", "pre_falha", "falha", "manutencao"]
    
    for estado in estados:
        temp = generate_sensor_value("temperature", estado)
        pressao = generate_sensor_value("pressure", estado)
        vibracao = generate_sensor_value("vibration", estado)
        print(f"   {estado:12} | Temp: {temp:6.1f}°C | Pressão: {pressao:4.1f}bar | Vibração: {vibracao:4.1f}mm/s")
    
    # Teste 2: Contadores
    print("\n📈 TESTE 2: Contadores acumulando")
    for _ in range(5):
        producao = generate_counter("production")
        print(f"   Produção acumulada: {producao:8.0f} unidades")
    
    # Teste 3: Alarmes
    print("\n🚨 TESTE 3: Alarmes por estado")
    for estado in estados:
        alarme = generate_alarm(estado)
        info = generate_info(estado, alarme)
        print(f"   {estado:12} | Alarme: {alarme} | Info: {info}")
    
    print("\n" + "=" * 60)
    print("✅ TESTES CONCLUÍDOS")
    print("=" * 60)