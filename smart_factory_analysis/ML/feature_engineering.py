"""
feature_engineering.py

Feature engineering para dados industriais.

Este módulo transforma dados brutos de sensores em features
para modelos de Machine Learning de manutenção preditiva.

Features criadas:
    - Temporais: hora, dia da semana, turno, fim de semana, noturno
    - Lags: valores defasados (1, 3, 6 horas atrás)
    - Estatísticas móveis: média e desvio móvel (janelas de 3, 6, 12)
    - Taxas de variação: diferença percentual, aceleração
    - Health score: score de saúde da máquina (0-100)
    - Anomalias: detecção por Z-score
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
import warnings
warnings.filterwarnings('ignore')


class IndustrialFeatureEngineer:
    """
    Engenheiro de features especializado em dados industriais.
    
    Esta classe aplica transformações específicas para séries temporais
    de sensores, como lags, médias móveis e detecção de anomalias.
    
    Atributos:
        df: DataFrame original com os dados
        scaler: Padronizador para normalizar as features
        label_encoders: Dicionário com codificadores para variáveis categóricas
        sensor_cols: Lista de colunas que são sensores
    """
    
    def __init__(self, df):
        """
        Inicializa o feature engineer com os dados.
        
        Args:
            df: DataFrame com colunas timestamp + tags + labels
        """
        self.df = df.copy()
        self.scaler = StandardScaler()
        self.label_encoders = {}
        
        # Identifica as colunas que são sensores (excluindo timestamp e labels)
        self.sensor_cols = [col for col in df.columns 
                           if col not in ['timestamp', 'is_failure', 'is_pre_failure']]
        
        print(f"\n🔧 INICIALIZANDO FEATURE ENGINEERING")
        print(f"   Total colunas originais: {len(df.columns)}")
        print(f"   Colunas de sensores: {len(self.sensor_cols)}")
        
    def extract_time_features(self):
        """
        Extrai features temporais a partir do timestamp.
        
        Features criadas:
            - hour: hora do dia (0-23)
            - day_of_week: dia da semana (0=segunda, 6=domingo)
            - day_of_month: dia do mês (1-31)
            - month: mês (1-12)
            - is_weekend: 1 se sábado ou domingo, 0 caso contrário
            - is_night: 1 se entre 22h e 5h, 0 caso contrário
            - shift: turno (madrugada, manha, tarde, noite)
        """
        print("\n⏰ Extraindo features temporais...")
        
        self.df['hour'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.dayofweek
        self.df['day_of_month'] = self.df['timestamp'].dt.day
        self.df['month'] = self.df['timestamp'].dt.month
        self.df['is_weekend'] = (self.df['day_of_week'] >= 5).astype(int)
        self.df['is_night'] = ((self.df['hour'] >= 22) | (self.df['hour'] <= 5)).astype(int)
        
        # Classifica em turnos (sem duplicatas)
        self.df['shift'] = pd.cut(
            self.df['hour'],
            bins=[0, 6, 14, 22, 24],
            labels=['madrugada', 'manha', 'tarde', 'noite'],
            right=False
        )
        
        print(f"   Features criadas: hour, day_of_week, day_of_month, month, is_weekend, is_night, shift")
        return self
    
    def create_lag_features(self, lags=[1, 3, 6]):
        """
        Cria features defasadas (lags) para sensores críticos.
        
        Lags são valores anteriores no tempo. Exemplo: lag_1 é o valor
        da hora anterior. Isso ajuda o modelo a entender tendências.
        
        Args:
            lags: Lista de defasagens em horas (ex: [1, 3, 6])
        """
        print("\n🔄 Criando lag features...")
        
        # Seleciona sensores críticos para aplicar lags
        sensores_criticos = [col for col in self.sensor_cols 
                           if any(x in col.lower() for x in ['temp', 'pressure', 'vibration', 'current'])]
        
        # Limita para performance (até 10 sensores)
        sensores_criticos = sensores_criticos[:10]
        
        for col in sensores_criticos:
            for lag in lags:
                self.df[f'{col}_lag_{lag}'] = self.df[col].shift(lag)
        
        print(f"   Lags criados para {len(sensores_criticos)} sensores críticos")
        return self
    
    def create_rolling_statistics(self, windows=[3, 6, 12]):
        """
        Cria estatísticas móveis (rolling windows).
        
        Médias e desvios móveis ajudam a suavizar ruídos e capturar
        tendências de degradação ao longo do tempo.
        
        Args:
            windows: Lista de tamanhos de janela em horas (ex: [3, 6, 12])
        """
        print("\n📊 Criando estatísticas móveis...")
        
        # Seleciona sensores analógicos
        sensores_analogicos = [col for col in self.sensor_cols 
                             if self.df[col].dtype in ['float64', 'int64']
                             and col not in ['is_failure', 'is_pre_failure']]
        
        # Limita para performance
        sensores_analogicos = sensores_analogicos[:15]
        
        for col in sensores_analogicos:
            for window in windows:
                self.df[f'{col}_rolling_mean_{window}'] = (
                    self.df[col].rolling(window=window, min_periods=1).mean()
                )
                self.df[f'{col}_rolling_std_{window}'] = (
                    self.df[col].rolling(window=window, min_periods=1).std()
                )
        
        print(f"   Estatísticas criadas para {len(sensores_analogicos)} sensores")
        return self
    
    def create_rate_of_change(self):
        """
        Cria taxas de variação (derivadas) dos sensores.
        
        Features criadas:
            - pct_change: variação percentual em relação ao valor anterior
            - diff: diferença simples entre valores consecutivos
            - acceleration: aceleração (diferença da diferença)
        
        Estas features indicam VELOCIDADE de degradação, não apenas o valor absoluto.
        """
        print("\n📈 Criando taxas de variação...")
        
        # Sensores que indicam degradação
        sensores_degradacao = [col for col in self.sensor_cols 
                              if any(x in col.lower() for x in ['temp', 'pressure', 'vibration'])]
        sensores_degradacao = sensores_degradacao[:10]
        
        for col in sensores_degradacao:
            # Valor anterior (defasado em 1)
            valor_anterior = self.df[col].shift(1)
            
            # Variação percentual com proteção contra divisão por zero
            with np.errstate(divide='ignore', invalid='ignore'):
                pct_change = (self.df[col] - valor_anterior) / valor_anterior.abs() * 100
                # Substitui infinitos e NaNs por 0
                pct_change = pct_change.replace([np.inf, -np.inf], 0).fillna(0)
            
            # Diferença simples (valor - anterior)
            diff = self.df[col].diff().fillna(0)
            
            # Aceleração (diferença da diferença)
            aceleracao = diff.diff().fillna(0)
            
            self.df[f'{col}_pct_change'] = pct_change
            self.df[f'{col}_diff'] = diff
            self.df[f'{col}_acceleration'] = aceleracao
        
        print(f"   Taxas criadas para {len(sensores_degradacao)} sensores")
        return self
    
    def create_health_score(self):
        """
        Cria um score de saúde da máquina (0-100).
        
        Quanto maior o score, mais saudável está a máquina.
        Sensores com valores altos (temperatura, pressão, vibração)
        reduzem o score proporcionalmente.
        """
        print("\n🏥 Criando health score...")
        
        # Sensores onde valor alto = problema
        sensores_problema = [col for col in self.sensor_cols 
                           if any(x in col.lower() for x in ['temp', 'pressure', 'vibration', 'current'])]
        
        if sensores_problema:
            # Começa com 100 (máquina perfeita)
            health_score = pd.Series(100, index=self.df.index)
            
            for col in sensores_problema[:8]:  # Limita para performance
                # Normaliza o sensor para 0-1 (quanto maior o valor, pior)
                min_val = self.df[col].min()
                max_val = self.df[col].max()
                
                if max_val > min_val:
                    normalized = (self.df[col] - min_val) / (max_val - min_val + 1e-10)
                    # Penaliza proporcionalmente ao valor normalizado
                    penalidade = normalized * 100 / len(sensores_problema)
                    health_score = health_score - penalidade
            
            # Garante que o score fique entre 0 e 100
            self.df['health_score'] = np.clip(health_score, 0, 100)
        
        print(f"   Health score criado")
        return self
    
    def create_anomaly_detection(self, threshold=3):
        """
        Detecta anomalias por Z-score.
        
        Z-score > threshold indica que o valor é muito diferente
        da média (possível anomalia).
        
        Args:
            threshold: Limiar para considerar anomalia (padrão: 3 desvios)
        """
        print("\n⚠️ Detectando anomalias...")
        
        sensores = [col for col in self.sensor_cols 
                   if self.df[col].dtype in ['float64', 'int64']][:10]
        
        for col in sensores:
            media = self.df[col].mean()
            desvio = self.df[col].std()
            
            if desvio > 0:
                zscore = (self.df[col] - media) / desvio
                self.df[f'{col}_is_anomaly'] = (abs(zscore) > threshold).astype(int)
        
        print(f"   Anomalias detectadas para {len(sensores)} sensores")
        return self
    
    def encode_categorical(self):
        """
        Codifica variáveis categóricas em números.
        
        Exemplo: turno 'manha' → 0, 'tarde' → 1, etc.
        """
        print("\n🔤 Codificando variáveis categóricas...")
        
        colunas_categoricas = ['shift']
        
        for col in colunas_categoricas:
            if col in self.df.columns:
                le = LabelEncoder()
                self.df[f'{col}_encoded'] = le.fit_transform(self.df[col].astype(str))
                self.label_encoders[col] = le
                print(f"   {col} → {col}_encoded")
        
        return self
    
    def handle_missing_values(self):
        """
        Trata valores ausentes e infinitos.
        
        Estratégias:
            1. Substitui infinitos por NaN
            2. Forward fill (propaga último valor válido)
            3. Backward fill (propaga próximo valor válido)
            4. Preenche o que restou com 0
        """
        print("\n🩹 Tratando valores ausentes e infinitos...")
        
        # Substitui infinitos por NaN
        self.df = self.df.replace([np.inf, -np.inf], np.nan)
        
        # Forward fill e backward fill
        self.df = self.df.ffill().bfill()
        
        # O que ainda tiver NaN, preenche com 0
        self.df = self.df.fillna(0)
        
        print(f"   Valores ausentes e infinitos tratados")
        return self
    
    def prepare_for_modeling(self, target_col='is_pre_failure'):
        """
        Prepara os dados finais para modelagem.
        
        Args:
            target_col: Coluna alvo (padrão: 'is_pre_failure')
            
        Returns:
            Tupla com (X_scaled, y, feature_names)
        """
        print("\n🎯 Preparando dados para modelagem...")
        
        # Colunas a serem excluídas (não são features)
        colunas_excluir = ['timestamp', 'is_failure', 'is_pre_failure', 'shift']
        
        # Seleciona apenas colunas numéricas que não estão na lista de exclusão
        feature_cols = [col for col in self.df.columns 
                       if col not in colunas_excluir 
                       and self.df[col].dtype in ['float64', 'int64', 'int32']]
        
        # Remove colunas que são constantes (variância zero ou todos valores iguais)
        for col in feature_cols[:]:  # Itera sobre cópia
            if self.df[col].std() == 0 or self.df[col].nunique() == 1:
                feature_cols.remove(col)
                print(f"   Removida coluna constante: {col}")
        
        print(f"   Features selecionadas: {len(feature_cols)}")
        
        # Prepara X (features) e y (target)
        X = self.df[feature_cols].values
        y = self.df[target_col].values
        
        # Verifica e trata infinitos novamente
        if np.isinf(X).any():
            print(f"   ⚠️ Ainda há infinitos! Substituindo por 0...")
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        # Normaliza as features (média 0, desvio 1)
        X_scaled = self.scaler.fit_transform(X)
        
        print(f"   Shape final: X={X_scaled.shape}, y={y.shape}")
        print(f"   Distribuição target: {np.bincount(y)}")
        
        return X_scaled, y, feature_cols
    
    def run_full_pipeline(self):
        """
        Executa o pipeline completo de feature engineering.
        
        Returns:
            Tupla com (X_scaled, y, feature_names)
        """
        print("\n" + "=" * 70)
        print("🚀 INICIANDO PIPELINE DE FEATURE ENGINEERING")
        print("=" * 70)
        
        self.extract_time_features()
        self.create_lag_features()
        self.create_rolling_statistics()
        self.create_rate_of_change()
        self.create_health_score()
        self.create_anomaly_detection()
        self.encode_categorical()
        self.handle_missing_values()
        
        X, y, features = self.prepare_for_modeling()
        
        print("\n" + "=" * 70)
        print("✅ FEATURE ENGINEERING CONCLUÍDO!")
        print("=" * 70)
        
        return X, y, features


# ============================================================
# EXECUÇÃO (TESTE)
# ============================================================

if __name__ == "__main__":
    print("Este script deve ser executado após o predictive_maintenance.py")
    print("\nUso:")
    print("  from feature_engineering import IndustrialFeatureEngineer")
    print("  engineer = IndustrialFeatureEngineer(df_ml)")
    print("  X, y, features = engineer.run_full_pipeline()")