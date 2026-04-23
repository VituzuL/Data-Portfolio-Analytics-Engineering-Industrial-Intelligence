"""
industrial_assistant.py

Assistente industrial inteligente com IA (Gemini).

Este chatbot entende linguagem natural e responde perguntas sobre
o estado da fábrica, máquinas, falhas, temperaturas, etc.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import google.generativeai as genai

# ============================================================
# CONFIGURAÇÃO DE LOG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# CLASSE DE CONSULTA AO BANCO DE DADOS
# ============================================================

class IndustrialDatabase:
    """Gerencia consultas ao banco de dados industrial"""
    
    def __init__(self):
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
        
        self.engine = create_engine(url_conexao)
    
    def listar_maquinas(self) -> pd.DataFrame:
        """Retorna lista de todas as máquinas"""
        query = """
        SELECT location_id, location_name, equipment_code
        FROM location
        WHERE is_machine = true
        ORDER BY location_name
        """
        return pd.read_sql(query, self.engine)
    
    def resumo_falhas(self, dias: int = 30) -> pd.DataFrame:
        """Resumo de falhas dos últimos X dias"""
        data_limite = datetime.now() - timedelta(days=dias)
        
        query = f"""
        SELECT 
            l.location_name,
            COUNT(CASE WHEN e.info = 'critical_failure' THEN 1 END) as falhas_criticas,
            COUNT(CASE WHEN e.info = 'anomaly_detected' THEN 1 END) as anomalias,
            COUNT(CASE WHEN e.info = 'warning_detected' THEN 1 END) as alertas
        FROM event e
        JOIN tag t ON e.tag = t.tag_name
        JOIN location l ON t.location_id = l.location_id
        WHERE e.time >= '{data_limite}'
        GROUP BY l.location_name
        ORDER BY falhas_criticas DESC
        """
        return pd.read_sql(query, self.engine)
    
    def top_temperaturas(self, dias: int = 7) -> pd.DataFrame:
        """Máquinas com temperaturas mais altas"""
        data_limite = datetime.now() - timedelta(days=dias)
        
        query = f"""
        SELECT 
            l.location_name,
            AVG(e.value) as temp_media,
            MAX(e.value) as temp_maxima,
            MIN(e.value) as temp_minima
        FROM event e
        JOIN tag t ON e.tag = t.tag_name
        JOIN location l ON t.location_id = l.location_id
        WHERE t.label = 'temperature'
        AND e.time >= '{data_limite}'
        GROUP BY l.location_name
        ORDER BY temp_maxima DESC
        """
        return pd.read_sql(query, self.engine)
    
    def top_vibracoes(self, dias: int = 7) -> pd.DataFrame:
        """Máquinas com vibrações mais altas"""
        data_limite = datetime.now() - timedelta(days=dias)
        
        query = f"""
        SELECT 
            l.location_name,
            AVG(e.value) as vib_media,
            MAX(e.value) as vib_maxima
        FROM event e
        JOIN tag t ON e.tag = t.tag_name
        JOIN location l ON t.location_id = l.location_id
        WHERE t.label = 'vibration'
        AND e.time >= '{data_limite}'
        GROUP BY l.location_name
        ORDER BY vib_maxima DESC
        """
        return pd.read_sql(query, self.engine)
    
    def falhas_semana(self) -> pd.DataFrame:
        """Falhas específicas desta semana"""
        data_limite = datetime.now() - timedelta(days=7)
        
        query = f"""
        SELECT 
            l.location_name,
            e.time,
            e.info,
            e.tag,
            e.value,
            t.unit
        FROM event e
        JOIN tag t ON e.tag = t.tag_name
        JOIN location l ON t.location_id = l.location_id
        WHERE e.info IN ('critical_failure', 'anomaly_detected')
        AND e.time >= '{data_limite}'
        ORDER BY e.time DESC
        LIMIT 30
        """
        return pd.read_sql(query, self.engine)
    
    def status_maquina(self, nome_maquina: str) -> Optional[Dict]:
        """Retorna o status mais recente de uma máquina"""
        query = f"""
        SELECT 
            e.time,
            e.tag,
            e.value,
            e.info,
            t.label,
            t.unit
        FROM event e
        JOIN tag t ON e.tag = t.tag_name
        JOIN location l ON t.location_id = l.location_id
        WHERE l.location_name = '{nome_maquina}'
        ORDER BY e.time DESC
        LIMIT 30
        """
        df = pd.read_sql(query, self.engine)
        
        if df.empty:
            return None
        
        status = {
            "maquina": nome_maquina,
            "ultima_atualizacao": df['time'].max(),
            "sensores": {}
        }
        
        for _, row in df.iterrows():
            if row['label'] not in status['sensores']:
                status['sensores'][row['label']] = {
                    "valor": row['value'],
                    "unidade": row['unit'],
                    "info": row['info']
                }
        
        return status


# ============================================================
# ASSISTENTE INTELIGENTE COM GEMINI (CORRIGIDO)
# ============================================================

class IndustrialAssistant:
    """
    Assistente industrial que usa Google Gemini para responder perguntas.
    """
    
    def __init__(self):
        load_dotenv()
        api_key = os.getenv("GEMINI_API_KEY")
        
        if not api_key:
            logger.error("GEMINI_API_KEY não encontrada no arquivo .env")
            raise ValueError("API key do Gemini não configurada")
        
        # Configura o Gemini
        genai.configure(api_key=api_key)
        
        # Usando o nome correto do modelo (da lista que você gerou)
        # Opções disponíveis no seu sistema:
        # - "gemini-flash-latest" → mais rápido (recomendado)
        # - "gemini-2.0-flash" → estável
        # - "gemini-pro-latest" → mais poderoso
        self.nome_modelo = "gemini-flash-latest"  # ← CORRIGIDO!
        
        try:
            self.modelo = genai.GenerativeModel(self.nome_modelo)
            logger.info(f"✅ Modelo carregado: {self.nome_modelo}")
        except Exception as e:
            logger.error(f"Erro ao carregar modelo: {e}")
            # Fallback para outro modelo
            self.nome_modelo = "gemini-2.0-flash"
            self.modelo = genai.GenerativeModel(self.nome_modelo)
            logger.info(f"✅ Fallback para: {self.nome_modelo}")
        
        self.db = IndustrialDatabase()
        logger.info("✅ Assistente industrial inicializado com Gemini")
    
    def buscar_dados_relevantes(self, pergunta: str) -> str:
        """Busca dados no banco baseado na intenção da pergunta"""
        pergunta_lower = pergunta.lower()
        contexto = []
        
        # Temperatura (o que você perguntou)
        if any(p in pergunta_lower for p in ["temperatura", "quente", "calor", "temp"]):
            df = self.db.top_temperaturas(dias=7)
            if not df.empty:
                contexto.append("🌡️ MÁQUINAS COM TEMPERATURA MAIS ALTA (ÚLTIMA SEMANA):")
                for _, row in df.iterrows():
                    alerta = "⚠️ CRÍTICO!" if row['temp_maxima'] > 250 else "⚠️ Atenção" if row['temp_maxima'] > 230 else "✅ Normal"
                    contexto.append(
                        f"   • {row['location_name']}: "
                        f"média {row['temp_media']:.1f}°C, "
                        f"máxima {row['temp_maxima']:.1f}°C → {alerta}"
                    )
            else:
                contexto.append("🌡️ Nenhum dado de temperatura encontrado nos últimos 7 dias.")
        
        # Falhas essa semana
        if any(p in pergunta_lower for p in ["falha essa semana", "falha esta semana", "teve alguma falha"]):
            df = self.db.falhas_semana()
            if not df.empty:
                contexto.append("\n🚨 FALHAS DESTA SEMANA:")
                for _, row in df.head(10).iterrows():
                    contexto.append(
                        f"   • {row['time'].strftime('%d/%m %H:%M')} - {row['location_name']}: "
                        f"{row['info']}"
                    )
                total_falhas = len(df[df['info'] == 'critical_failure'])
                contexto.append(f"\n📊 TOTAL: {total_falhas} falhas críticas")
            else:
                contexto.append("\n✅ NENHUMA FALHA registrada nos últimos 7 dias.")
        
        # Lista de máquinas
        if any(p in pergunta_lower for p in ["máquina", "equipamento", "quais máquinas"]):
            df = self.db.listar_maquinas()
            if not df.empty:
                contexto.append("\n📋 MÁQUINAS CADASTRADAS:")
                for _, row in df.iterrows():
                    contexto.append(f"   • {row['location_name']}")
        
        # Falhas em geral
        if "falha" in pergunta_lower and "semana" not in pergunta_lower:
            df = self.db.resumo_falhas(dias=30)
            if not df.empty:
                contexto.append("\n📊 RESUMO DE FALHAS (ÚLTIMOS 30 DIAS):")
                for _, row in df.head(5).iterrows():
                    contexto.append(
                        f"   • {row['location_name']}: "
                        f"{row['falhas_criticas']} falhas críticas"
                    )
        
        # Status de máquina específica
        nomes_maquinas = ["desodorizador", "caldeira", "branqueamento", "compressor"]
        for nome in nomes_maquinas:
            if nome in pergunta_lower:
                for _, row in self.db.listar_maquinas().iterrows():
                    if nome.lower() in row['location_name'].lower():
                        status = self.db.status_maquina(row['location_name'])
                        if status:
                            contexto.append(f"\n🏭 STATUS: {row['location_name']}")
                            for sensor, dados in status['sensores'].items():
                                contexto.append(
                                    f"   • {sensor}: {dados['valor']} {dados['unidade']}"
                                )
                        break
        
        if not contexto:
            contexto.append("📊 DADOS GERAIS DA FÁBRICA disponíveis para consulta.")
        
        return "\n".join(contexto)
    
    def responder(self, pergunta: str) -> str:
        """Responde à pergunta usando Gemini"""
        
        # Busca dados relevantes
        contexto = self.buscar_dados_relevantes(pergunta)
        
        # Monta o prompt
        prompt = f"""
Você é um assistente industrial especializado em manutenção preditiva.

Responda à pergunta do usuário usando APENAS os dados abaixo.

DADOS DA FÁBRICA:
{contexto}

PERGUNTA: {pergunta}

REGRAS:
- Seja direto e objetivo
- Se um valor está anormal, sugira ação
- Destaque a máquina com maior temperatura se perguntarem

RESPOSTA:
"""
        
        try:
            resposta = self.modelo.generate_content(prompt)
            return resposta.text
        except Exception as e:
            logger.error(f"Erro no Gemini: {e}")
            return self._resposta_fallback(pergunta, contexto)
    
    def _resposta_fallback(self, pergunta: str, contexto: str) -> str:
        """Resposta baseada em regras quando a IA falha"""
        
        if "temperatura" in pergunta.lower():
            linhas = contexto.split('\n')
            for linha in linhas:
                if "máxima" in linha and "°C" in linha:
                    return f"{linha}\n\n💡 Dica: Temperaturas acima de 250°C indicam risco de falha."
            return "🌡️ Não encontrei dados de temperatura nos últimos 7 dias."
        
        if "falha" in pergunta.lower() and "semana" in pergunta.lower():
            if "NENHUMA FALHA" in contexto:
                return "✅ Ótimas notícias! Nenhuma falha foi registrada nos últimos 7 dias."
            return contexto
        
        return f"📊 Dados encontrados:\n\n{contexto}"
    
    def chat_interativo(self):
        """Interface de chat interativa"""
        print("\n" + "=" * 70)
        print("🤖 ASSISTENTE INDUSTRIAL INTELIGENTE")
        print("=" * 70)
        print(f"\n✅ Usando Google Gemini - Modelo: {self.nome_modelo}")
        print("\nPergunte em português natural:")
        print("   • 'Qual máquina está com temperatura mais alta?'")
        print("   • 'Teve alguma falha essa semana?'")
        print("   • 'Como está o Desodorizador 1?'")
        print("\n" + "=" * 70)
        
        while True:
            print("\n" + "─" * 50)
            pergunta = input("\n👨‍🏭 Você: ").strip()
            
            if pergunta.lower() in ['sair', 'exit', 'quit']:
                print("\n🤖 Assistente: Até mais!")
                break
            
            if pergunta.lower() in ['ajuda', 'help']:
                continue
            
            print("\n🤖 Assistente: Consultando dados...")
            resposta = self.responder(pergunta)
            print(f"\n🤖 Assistente:\n{resposta}")


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":
    try:
        assistente = IndustrialAssistant()
        assistente.chat_interativo()
    except ValueError as e:
        print(f"\n❌ {e}")