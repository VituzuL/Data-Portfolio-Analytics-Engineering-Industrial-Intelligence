"""
sku_engine.py

Motor de controle de campanhas de produção (SKU).

Este módulo gerencia qual produto (SKU) está sendo produzido em um determinado
momento, simulando campanhas de produção reais onde um mesmo produto é feito
por um período (ex: 8 a 72 horas) antes de trocar para outro.

Características simuladas:
    - Duração variável da campanha (configurável)
    - Setup entre trocas de SKU (tempo de parada)
    - SKUs disponíveis definidos na config
    - Troca aleatória entre SKUs diferentes
"""

import random
from config import AVAILABLE_SKUS, SKU_CAMPAIGN_HOURS, SETUP_DURATION_MINUTES


# ============================================================
# CLASSE DE ESTADO DO SKU
# ============================================================

class SKUState:
    """
    Mantém o estado atual da produção de SKU.
    
    Esta classe armazena:
        - Qual SKU está sendo produzido agora
        - Há quantas horas ele está rodando
        - Se está em setup (troca de produto)
        - Quanto tempo de setup ainda falta
    
    A simulação é "em memória" - uma única instância mantém o estado
    durante toda a execução do gerador.
    
    Atributos:
        current_sku: SKU atual em produção (ex: 1, 2, 3, 4, 5)
        hours_running: Horas acumuladas deste SKU na campanha atual
        in_setup: Flag indicando se está em setup (troca de produto)
        setup_remaining: Minutos restantes de setup
    """
    
    def __init__(self):
        """Inicializa o estado com um SKU aleatório e zero horas rodando."""
        self.current_sku = random.choice(AVAILABLE_SKUS)
        self.hours_running = 0
        self.in_setup = False
        self.setup_remaining = 0


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def pick_next_sku(current_sku: int) -> int:
    """
    Escolhe o próximo SKU a ser produzido.
    
    A escolha é aleatória, mas garante que o novo SKU seja
    diferente do atual (não faz sentido "trocar" para o mesmo).
    
    Args:
        current_sku: SKU atual em produção
        
    Returns:
        Novo SKU (diferente do atual)
        
    Exemplo:
        >>> pick_next_sku(1)
        3  # Pode ser qualquer um exceto 1
    """
    
    # Lista de SKUs disponíveis, excluindo o atual
    opcoes = [sku for sku in AVAILABLE_SKUS if sku != current_sku]
    
    return random.choice(opcoes)


def should_change_sku(state: SKUState) -> bool:
    """
    Decide se deve trocar o SKU baseado no tempo de campanha.
    
    A duração da campanha é aleatória entre os valores configurados
    em SKU_CAMPAIGN_HOURS (ex: mínimo 8h, máximo 72h).
    
    Uma campanha pode acabar mais cedo ou mais tarde - isso simula
    a demanda variável do mercado e planejamento de produção.
    
    Args:
        state: Estado atual do SKU (contém hours_running)
        
    Returns:
        True se a campanha atual deve terminar, False caso contrário
    """
    
    # Define a duração máxima desta campanha (aleatória)
    duracao_maxima = random.randint(
        SKU_CAMPAIGN_HOURS["min"],
        SKU_CAMPAIGN_HOURS["max"]
    )
    
    # Verifica se já rodou o suficiente
    return state.hours_running >= duracao_maxima


def start_setup(state: SKUState) -> None:
    """
    Inicia o processo de setup para troca de SKU.
    
    Durante o setup, a linha de produção fica parada.
    O tempo de setup é aleatório (configurável).
    
    Args:
        state: Estado do SKU (será modificado)
    """
    
    state.in_setup = True
    state.setup_remaining = random.randint(
        SETUP_DURATION_MINUTES["min"],
        SETUP_DURATION_MINUTES["max"]
    )


# ============================================================
# FUNÇÃO PRINCIPAL DE ATUALIZAÇÃO
# ============================================================

def update_sku_state(state: SKUState):
    """
    Atualiza o estado da linha de produção a cada intervalo.
    
    Esta função deve ser chamada a cada passo da simulação.
    Ela gerencia:
        1. Se está em setup → decrementa o contador
        2. Se setup terminou → troca o SKU e reinicia horas
        3. Se não está em setup → incrementa horas rodando
        4. Se atingiu duração máxima → inicia setup
    
    Args:
        state: Estado do SKU (será modificado in-place)
        
    Returns:
        Tupla com (sku_atual, esta_em_setup)
        
    Exemplo:
        >>> sku = SKUState()
        >>> sku.current_sku = 1
        >>> update_sku_state(sku)
        (1, False)  # Ainda produzindo SKU 1
        >>> # Após várias chamadas, pode trocar...
    """
    
    # ============================================================
    # CASO 1: ESTÁ EM SETUP (TROCA DE PRODUTO)
    # ============================================================
    if state.in_setup:
        # Decrementa o tempo restante de setup
        state.setup_remaining -= 1
        
        # Se o setup terminou
        if state.setup_remaining <= 0:
            state.in_setup = False
            # Troca para um novo SKU (diferente do atual)
            state.current_sku = pick_next_sku(state.current_sku)
            # Zera o contador de horas desta nova campanha
            state.hours_running = 0
        
        return state.current_sku, True
    
    # ============================================================
    # CASO 2: PRODUÇÃO NORMAL
    # ============================================================
    
    # Incrementa o tempo de campanha do SKU atual
    state.hours_running += 1
    
    # Verifica se está na hora de trocar de SKU
    if should_change_sku(state):
        start_setup(state)
        return state.current_sku, True
    
    # Continua produzindo o mesmo SKU
    return state.current_sku, False


# ============================================================
# FUNÇÃO DE RESET (ÚTIL PARA TESTES)
# ============================================================

def reset_sku_state(state: SKUState, novo_sku: int = None) -> None:
    """
    Reseta o estado do SKU para valores iniciais.
    
    Útil para testes ou para reiniciar a simulação.
    
    Args:
        state: Estado do SKU a ser resetado
        novo_sku: SKU opcional para iniciar (se None, escolhe aleatório)
    """
    if novo_sku is None:
        state.current_sku = random.choice(AVAILABLE_SKUS)
    else:
        state.current_sku = novo_sku
    
    state.hours_running = 0
    state.in_setup = False
    state.setup_remaining = 0


# ============================================================
# TESTE LOCAL
# ============================================================

if __name__ == "__main__":
    """
    Testa o comportamento do motor de SKU.
    Executar: python sku_engine.py
    """
    
    print("=" * 60)
    print("TESTE DO MOTOR DE SKU - CAMPANHAS DE PRODUÇÃO")
    print("=" * 60)
    
    # Configurações atuais
    print(f"\n📋 CONFIGURAÇÕES:")
    print(f"   SKUs disponíveis: {AVAILABLE_SKUS}")
    print(f"   Duração campanha: {SKU_CAMPAIGN_HOURS['min']} a {SKU_CAMPAIGN_HOURS['max']} horas")
    print(f"   Tempo de setup: {SETUP_DURATION_MINUTES['min']} a {SETUP_DURATION_MINUTES['max']} minutos")
    
    # Cria o estado inicial
    estado = SKUState()
    print(f"\n🎯 ESTADO INICIAL:")
    print(f"   SKU atual: {estado.current_sku}")
    print(f"   Horas rodando: {estado.hours_running}")
    print(f"   Em setup: {estado.in_setup}")
    
    # Simula 100 iterações (cada iteração = 1 hora)
    print("\n📈 SIMULANDO 100 HORAS DE PRODUÇÃO:")
    print("-" * 60)
    
    trocas_realizadas = 0
    ultimo_sku = estado.current_sku
    
    for hora in range(1, 101):
        sku_atual, em_setup = update_sku_state(estado)
        
        # Detecta se houve troca de SKU
        if sku_atual != ultimo_sku and not em_setup:
            print(f"   Hora {hora:3d}: ✅ TROCA DE SKU! Agora produzindo SKU {sku_atual}")
            trocas_realizadas += 1
            ultimo_sku = sku_atual
        elif em_setup:
            print(f"   Hora {hora:3d}: 🔧 EM SETUP... faltam {estado.setup_remaining} min")
        elif hora % 20 == 0:  # Mostra a cada 20 horas
            print(f"   Hora {hora:3d}: Produzindo SKU {sku_atual} (há {estado.hours_running}h)")
    
    print("-" * 60)
    print(f"\n📊 RESUMO DA SIMULAÇÃO:")
    print(f"   Total de trocas realizadas: {trocas_realizadas}")
    print(f"   SKU final: {estado.current_sku}")
    print(f"   Horas na campanha atual: {estado.hours_running}")
    
    print("\n" + "=" * 60)
    print("✅ TESTES CONCLUÍDOS")
    print("=" * 60)