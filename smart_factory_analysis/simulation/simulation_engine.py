"""
simulation_engine.py

Motor de estados da máquina para simulação industrial.

Responsável por definir em qual estágio do ciclo de falha a máquina está
em um determinado momento. O ciclo segue uma progressão natural:

    normal → degradando → pre_falha → falha → manutencao → recuperacao

Cada ciclo completo dura DIAS_CICLO_FALHA (configurado em config.py).
"""

from datetime import datetime
from config import FAILURE_CYCLE_DAYS


# ============================================================
# FUNÇÃO PRINCIPAL: DEFINIR ESTADO DA MÁQUINA
# ============================================================

def get_machine_state(current_timestamp: datetime, base_start: datetime) -> str:
    """
    Determina o estado atual da máquina com base no ciclo de falha.
    
    O ciclo funciona como um relógio: a cada FAILURE_CYCLE_DAYS dias,
    a máquina completa um ciclo completo de degradação e recuperação.
    
    A distribuição do ciclo é:
        0%   → 70%  : normal (operação saudável)
        70%  → 85%  : degradando (performance caindo)
        85%  → 92%  : pre_falha (padrões anormais)
        92%  → 95%  : falha (parada crítica)
        95%  → 98%  : manutencao (equipe trabalhando)
        98%  → 100% : recuperacao (voltando ao normal)
    
    Args:
        current_timestamp: Data/hora atual para verificar
        base_start: Data/hora de início da simulação (marco zero)
        
    Returns:
        String com o estado atual: "normal", "degradando", "pre_falha", 
        "falha", "manutencao" ou "recuperacao"
        
    Exemplo:
        >>> start = datetime(2026, 1, 1)
        >>> agora = datetime(2026, 1, 5)  # 4 dias depois
        >>> get_machine_state(agora, start)
        'normal'  # Ainda nos primeiros 70% do ciclo
    """
    
    # Calcula quantas horas se passaram desde o início da simulação
    horas_passadas = int(
        (current_timestamp - base_start).total_seconds() / 3600
    )
    
    # Duração total de um ciclo em horas
    horas_ciclo = FAILURE_CYCLE_DAYS * 24
    
    # Posição atual dentro do ciclo (0 a horas_ciclo)
    posicao_ciclo = horas_passadas % horas_ciclo
    
    # ============================================================
    # DEFINE O ESTADO BASEADO NA POSIÇÃO DO CICLO
    # ============================================================
    
    # 0% a 70% do ciclo → Operação normal
    if posicao_ciclo < horas_ciclo * 0.70:
        return "normal"
    
    # 70% a 85% do ciclo → Começando a degradar
    elif posicao_ciclo < horas_ciclo * 0.85:
        return "degradando"
    
    # 85% a 92% do ciclo → Pré-falha (anomalias detectadas)
    elif posicao_ciclo < horas_ciclo * 0.92:
        return "pre_falha"
    
    # 92% a 95% do ciclo → Falha crítica (máquina parou)
    elif posicao_ciclo < horas_ciclo * 0.95:
        return "falha"
    
    # 95% a 98% do ciclo → Manutenção (time trabalhando)
    elif posicao_ciclo < horas_ciclo * 0.98:
        return "manutencao"
    
    # 98% a 100% do ciclo → Recuperação (voltando ao normal)
    else:
        return "recuperacao"


# ============================================================
# FUNÇÃO AUXILIAR: VERIFICAR DISPONIBILIDADE
# ============================================================

def is_machine_available(state: str) -> bool:
    """
    Verifica se a máquina está disponível para produzir.
    
    Durante falha ou manutenção, a máquina não consegue produzir.
    Nos outros estados, a produção continua normalmente.
    
    Args:
        state: Estado atual da máquina
        
    Returns:
        True se a máquina pode produzir, False caso contrário
    """
    
    estados_indisponiveis = ["falha", "manutencao"]
    
    return state not in estados_indisponiveis


# ============================================================
# TESTE LOCAL
# ============================================================

if __name__ == "__main__":
    """
    Testa o comportamento do motor de estados ao longo do tempo.
    Executar: python simulation_engine.py
    """
    
    print("=" * 60)
    print("TESTE DO MOTOR DE ESTADOS - SIMULATION ENGINE")
    print("=" * 60)
    
    # Define a data de início da simulação
    base_start = datetime(2026, 1, 1, 0, 0, 0)
    
    print(f"\n📅 Data de início: {base_start}")
    print(f"📊 Duração do ciclo: {FAILURE_CYCLE_DAYS} dias\n")
    
    # Testa diferentes datas para ver a evolução do estado
    datas_teste = [
        datetime(2026, 1, 2),   # 1 dia depois
        datetime(2026, 1, 8),   # 7 dias depois
        datetime(2026, 1, 9),   # 8 dias depois
        datetime(2026, 1, 10),  # 9 dias depois
        datetime(2026, 1, 11),  # 10 dias depois
        datetime(2026, 1, 12),  # 11 dias depois
    ]
    
    print("📈 EVOLUÇÃO DOS ESTADOS:")
    print("-" * 50)
    
    for dt in datas_teste:
        estado = get_machine_state(dt, base_start)
        disponivel = is_machine_available(estado)
        
        simbolo = "✅" if disponivel else "❌"
        print(f"   {dt.strftime('%Y-%m-%d')} | Estado: {estado:12} | Disponível: {simbolo}")
    
    print("\n" + "=" * 60)
    
    # Explicação do ciclo
    print("\n📖 ENTENDENDO O CICLO:")
    print("   Com FAILURE_CYCLE_DAYS = 10 dias:")
    print("   • Dias 0-7    → normal")
    print("   • Dias 7-8.5  → degradando")
    print("   • Dias 8.5-9.2 → pre_falha")
    print("   • Dias 9.2-9.5 → falha")
    print("   • Dias 9.5-9.8 → manutencao")
    print("   • Dias 9.8-10  → recuperacao")
    print("   • Dia 10 em diante → reinicia o ciclo")
    
    print("\n" + "=" * 60)
    print("✅ TESTES CONCLUÍDOS")
    print("=" * 60)