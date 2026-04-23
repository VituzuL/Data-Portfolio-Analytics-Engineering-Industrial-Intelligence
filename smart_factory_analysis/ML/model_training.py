"""
model_training.py

Modelo de manutenção preditiva com interface gráfica.

Este módulo treina um modelo Random Forest para cada máquina selecionada,
com interface interativa para:
    - Selecionar a máquina via janela gráfica
    - Visualizar resultados (matriz de confusão, curva ROC, features)
    - Salvar o modelo treinado
"""

import pandas as pd
import numpy as np
import joblib
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_curve, auc, 
    precision_recall_curve, f1_score, recall_score, precision_score
)
from sklearn.utils.class_weight import compute_class_weight
import warnings
warnings.filterwarnings('ignore')

# Importa a interface gráfica
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


class PredictiveMaintenanceModel:
    """Modelo preditivo para manutenção industrial"""
    
    def __init__(self, model_type='random_forest', machine_id=None, machine_name=None):
        self.model_type = model_type
        self.machine_id = machine_id
        self.machine_name = machine_name
        self.model = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.feature_importance = None
        self.threshold = 0.5
        self.removed_tags = []
        
    def exclude_problematic_tags(self, df):
        """Remove tags que causam overfitting (consequência, não causa)"""
        
        # Tags problemáticas (são CONSEQUÊNCIA da falha, não CAUSA)
        problematic_keywords = [
            'production', 'counter', 'runtime', 'downtime', 'rejects',
            'efficiency', 'maintenance_flag', 'start_counter', 'trip_counter',
            'failure_counter', 'batch_time', 'setup_change', 'days_since_last_maintenance'
        ]
        
        problematic_cols = []
        for col in df.columns:
            col_lower = col.lower()
            for keyword in problematic_keywords:
                if keyword in col_lower:
                    problematic_cols.append(col)
                    break
        
        self.removed_tags = problematic_cols
        
        # Remove as colunas
        df_clean = df.drop(columns=problematic_cols, errors='ignore')
        
        return df_clean
    
    def filter_by_machine(self, df, machine_id=None, machine_name=None):
        """Filtra dados por máquina específica"""
        
        if machine_id:
            filtered_df = df[df['location_id'] == machine_id].copy()
            machine_name = filtered_df['location_name'].iloc[0] if len(filtered_df) > 0 else f"ID_{machine_id}"
        elif machine_name:
            filtered_df = df[df['location_name'] == machine_name].copy()
        else:
            raise ValueError("Forneça machine_id ou machine_name")
        
        if len(filtered_df) == 0:
            return None
        
        return filtered_df
    
    def load_data(self, df, target_col='is_pre_failure', test_size=0.2, random_state=42):
        """Carrega e divide os dados"""
        
        # 1. REMOVER TAGS PROBLEMÁTICAS
        df_clean = self.exclude_problematic_tags(df)
        
        # 2. Selecionar features (apenas colunas numéricas)
        exclude_cols = ['timestamp', 'location_id', 'location_name', 'machine_state', 
                       'is_failure', 'is_pre_failure']
        
        feature_cols = [col for col in df_clean.columns 
                       if col not in exclude_cols 
                       and df_clean[col].dtype in ['float64', 'int64', 'int32']]
        
        # 3. Preparar X e y
        X = df_clean[feature_cols].values
        y = df_clean[target_col].values
        
        # 4. Tratar infinitos e nulos
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        # 5. Dividir treino/teste
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )
        
        return feature_cols
    
    def train(self):
        """Treina o modelo"""
        
        # Calcular pesos das classes (para dados desbalanceados)
        class_weights = compute_class_weight(
            'balanced',
            classes=np.unique(self.y_train),
            y=self.y_train
        )
        weight_dict = dict(zip(np.unique(self.y_train), class_weights))
        
        # Criar modelo
        if self.model_type == 'random_forest':
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                class_weight='balanced',
                n_jobs=-1
            )
        else:
            self.model = RandomForestClassifier(
                n_estimators=100,
                random_state=42,
                class_weight='balanced'
            )
        
        # Treinar
        self.model.fit(self.X_train, self.y_train)
        
        # Feature importance
        if hasattr(self.model, 'feature_importances_'):
            self.feature_importance = self.model.feature_importances_
        
        return self
    
    def optimize_threshold(self):
        """Otimiza o threshold baseado no F1-score"""
        
        y_pred_proba = self.model.predict_proba(self.X_test)[:, 1]
        
        precisions, recalls, thresholds = precision_recall_curve(self.y_test, y_pred_proba)
        
        # Encontrar threshold que maximiza F1
        f1_scores = 2 * (precisions * recalls) / (precisions + recalls + 1e-10)
        best_idx = np.argmax(f1_scores[:-1])
        self.threshold = thresholds[best_idx] if best_idx < len(thresholds) else 0.5
        
        return self
    
    def evaluate(self, feature_names=None):
        """Avalia o modelo e retorna métricas"""
        
        # Predições com threshold otimizado
        y_pred_proba = self.model.predict_proba(self.X_test)[:, 1]
        y_pred = (y_pred_proba >= self.threshold).astype(int)
        
        # Métricas
        accuracy = (y_pred == self.y_test).mean()
        precision = precision_score(self.y_test, y_pred, zero_division=0)
        recall = recall_score(self.y_test, y_pred, zero_division=0)
        f1 = f1_score(self.y_test, y_pred, zero_division=0)
        
        # Matriz de confusão
        cm = confusion_matrix(self.y_test, y_pred)
        
        # Cross-validation
        try:
            cv_scores = cross_val_score(self.model, self.X_train, self.y_train, cv=5, scoring='f1')
            cv_mean = cv_scores.mean()
            cv_std = cv_scores.std()
        except:
            cv_mean = 0
            cv_std = 0
        
        # Feature importance
        importance_df = None
        if self.feature_importance is not None and feature_names is not None:
            importance_df = pd.DataFrame({
                'feature': feature_names[:len(self.feature_importance)],
                'importance': self.feature_importance
            }).sort_values('importance', ascending=False)
        
        # Curva ROC
        fpr, tpr, _ = roc_curve(self.y_test, y_pred_proba)
        roc_auc = auc(fpr, tpr)
        
        return {
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'threshold': self.threshold,
            'cv_mean': cv_mean,
            'cv_std': cv_std,
            'confusion_matrix': cm,
            'feature_importance': importance_df,
            'roc_curve': (fpr, tpr, roc_auc),
            'y_pred_proba': y_pred_proba,
            'y_test': self.y_test,
            'removed_tags_count': len(self.removed_tags)
        }
    
    def save_model(self, filepath='models/'):
        """Salva o modelo treinado"""
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        machine_suffix = self.machine_name.replace(' ', '_').lower()
        filename = f"{filepath}model_{machine_suffix}_{self.model_type}_{timestamp}.pkl"
        
        model_data = {
            'model': self.model,
            'model_type': self.model_type,
            'machine_id': self.machine_id,
            'machine_name': self.machine_name,
            'threshold': self.threshold,
            'feature_importance': self.feature_importance,
            'removed_tags': self.removed_tags,
            'timestamp': timestamp
        }
        
        joblib.dump(model_data, filename)
        
        return filename


# ============================================================
# INTERFACE GRÁFICA
# ============================================================

class ModelTrainingApp:
    """Aplicação gráfica para treinamento de modelos"""
    
    def __init__(self, df):
        self.df = df
        self.modelo = None
        self.resultados = None
        self.feature_names = None
        
        # Cria a janela principal
        self.root = tk.Tk()
        self.root.title("🤖 Manutenção Preditiva - Treinamento de Modelo")
        self.root.geometry("1400x900")
        self.root.configure(bg='#f0f0f0')
        
        # Configura o estilo
        style = ttk.Style()
        style.theme_use('clam')
        
        self.setup_ui()
        
    def setup_ui(self):
        """Configura todos os elementos da interface"""
        
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # ============================================================
        # SEÇÃO DE SELEÇÃO (ESQUERDA)
        # ============================================================
        left_frame = ttk.LabelFrame(main_frame, text="📋 Seleção da Máquina", padding="10")
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        
        # Lista de máquinas
        ttk.Label(left_frame, text="Máquinas Disponíveis:", font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        
        # Frame para a lista com scroll
        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(5, 10))
        
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.machine_listbox = tk.Listbox(
            list_frame, 
            yscrollcommand=scrollbar.set,
            height=20,
            width=40,
            font=('Courier', 10)
        )
        self.machine_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.machine_listbox.yview)
        
        # Lista as máquinas disponíveis
        machines = self.df[['location_id', 'location_name']].drop_duplicates().sort_values('location_id')
        for _, row in machines.iterrows():
            self.machine_listbox.insert(tk.END, f"{row['location_id']:3d} - {row['location_name']}")
        
        # Bind de seleção
        self.machine_listbox.bind('<<ListboxSelect>>', self.on_machine_select)
        
        # Botão de treinar
        self.train_button = ttk.Button(
            left_frame, 
            text="🚀 TREINAR MODELO", 
            command=self.train_model,
            state=tk.DISABLED
        )
        self.train_button.pack(fill=tk.X, pady=(10, 0))
        
        # Status
        self.status_label = ttk.Label(left_frame, text="Selecione uma máquina", font=('Arial', 9))
        self.status_label.pack(fill=tk.X, pady=(10, 0))
        
        # ============================================================
        # SEÇÃO DE RESULTADOS (DIREITA)
        # ============================================================
        right_frame = ttk.Frame(main_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # Notebook para abas
        self.notebook = ttk.Notebook(right_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Aba 1: Métricas
        self.metrics_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.metrics_frame, text="📊 Métricas")
        
        # Área de texto para métricas
        self.metrics_text = scrolledtext.ScrolledText(
            self.metrics_frame, 
            wrap=tk.WORD, 
            font=('Courier', 10),
            height=20
        )
        self.metrics_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Aba 2: Gráficos
        self.plots_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.plots_frame, text="📈 Gráficos")
        
        # Área para os gráficos do matplotlib
        self.figure = Figure(figsize=(8, 6), dpi=100)
        self.canvas = FigureCanvasTkAgg(self.figure, self.plots_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Aba 3: Features
        self.features_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.features_frame, text="🔍 Features Importantes")
        
        # Área para gráfico de features
        self.features_figure = Figure(figsize=(8, 6), dpi=100)
        self.features_canvas = FigureCanvasTkAgg(self.features_figure, self.features_frame)
        self.features_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Aba 4: Tags removidas
        self.removed_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.removed_frame, text="🗑️ Tags Removidas")
        
        self.removed_text = scrolledtext.ScrolledText(
            self.removed_frame,
            wrap=tk.WORD,
            font=('Courier', 9),
            height=20
        )
        self.removed_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
    def on_machine_select(self, event):
        """Quando uma máquina é selecionada na lista"""
        selection = self.machine_listbox.curselection()
        if selection:
            self.train_button.config(state=tk.NORMAL)
            self.status_label.config(text="✅ Máquina selecionada. Clique em TREINAR.")
    
    def train_model(self):
        """Treina o modelo para a máquina selecionada"""
        
        # Obtém a máquina selecionada
        selection = self.machine_listbox.curselection()
        if not selection:
            messagebox.showwarning("Seleção", "Selecione uma máquina primeiro!")
            return
        
        selected_text = self.machine_listbox.get(selection[0])
        machine_id = int(selected_text.split('-')[0].strip())
        
        # Busca o nome da máquina
        machine_name = self.df[self.df['location_id'] == machine_id]['location_name'].iloc[0]
        
        self.status_label.config(text=f"🔄 Treinando modelo para {machine_name}...")
        self.root.update()
        
        try:
            # Filtra dados da máquina
            df_filtered = self.df[self.df['location_id'] == machine_id].copy()
            
            if len(df_filtered) == 0:
                messagebox.showerror("Erro", f"Nenhum dado encontrado para {machine_name}")
                return
            
            # Cria e treina o modelo
            self.modelo = PredictiveMaintenanceModel(
                model_type='random_forest',
                machine_id=machine_id,
                machine_name=machine_name
            )
            
            # Prepara os dados
            self.feature_names = self.modelo.load_data(df_filtered)
            
            # Treina
            self.modelo.train()
            
            # Otimiza threshold
            self.modelo.optimize_threshold()
            
            # Avalia
            self.resultados = self.modelo.evaluate(self.feature_names)
            
            # Salva o modelo
            filename = self.modelo.save_model()
            
            # Atualiza a interface
            self.update_results()
            
            self.status_label.config(
                text=f"✅ Modelo treinado! Recall: {self.resultados['recall']:.2%} | F1: {self.resultados['f1']:.3f}"
            )
            
            messagebox.showinfo(
                "Sucesso", 
                f"Modelo treinado com sucesso!\n\n"
                f"Máquina: {machine_name}\n"
                f"Recall: {self.resultados['recall']:.2%}\n"
                f"F1-Score: {self.resultados['f1']:.3f}\n\n"
                f"Modelo salvo em:\n{filename}"
            )
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro durante o treinamento:\n{str(e)}")
            self.status_label.config(text="❌ Erro no treinamento. Veja o log.")
    
    def update_results(self):
        """Atualiza todas as abas com os resultados"""
        
        # ============================================================
        # ABA 1: MÉTRICAS
        # ============================================================
        self.metrics_text.delete(1.0, tk.END)
        
        metrics_text = f"""
{'='*60}
📊 RESULTADOS DO MODELO - {self.modelo.machine_name}
{'='*60}

🎯 MÉTRICAS PRINCIPAIS (threshold = {self.resultados['threshold']:.3f}):
   ┌─────────────────┬──────────┐
   │ Acurácia        │ {self.resultados['accuracy']:.4f}   │
   │ Precisão        │ {self.resultados['precision']:.4f}   │
   │ Recall          │ {self.resultados['recall']:.4f}   │ ⭐
   │ F1-Score        │ {self.resultados['f1']:.4f}   │
   └─────────────────┴──────────┘

📊 VALIDAÇÃO CRUZADA (5 folds):
   F1 médio: {self.resultados['cv_mean']:.4f} (+/- {self.resultados['cv_std'] * 2:.4f})

📋 MATRIZ DE CONFUSÃO:
   ┌─────────────────────┬────────────┬────────────┐
   │                     │ Predito    │ Predito    │
   │                     │ Normal     │ Pré-Falha  │
   ├─────────────────────┼────────────┼────────────┤
   │ Real Normal         │ {self.resultados['confusion_matrix'][0,0]:>10} │ {self.resultados['confusion_matrix'][0,1]:>10} │
   │ Real Pré-Falha      │ {self.resultados['confusion_matrix'][1,0]:>10} │ {self.resultados['confusion_matrix'][1,1]:>10} │
   └─────────────────────┴────────────┴────────────┘

   • VP (acertou falha):     {self.resultados['confusion_matrix'][1,1]}
   • FP (alarme falso):      {self.resultados['confusion_matrix'][0,1]}
   • VN (acertou normal):    {self.resultados['confusion_matrix'][0,0]}
   • FN (perdeu falha):      {self.resultados['confusion_matrix'][1,0]} ← MAIS CRÍTICO!

📈 INTERPRETAÇÃO:
"""
        
        if self.resultados['recall'] >= 0.8:
            metrics_text += "   ✅ Recall alto - O modelo detecta a maioria das falhas\n"
        elif self.resultados['recall'] >= 0.6:
            metrics_text += "   ⚠️ Recall médio - Algumas falhas podem passar despercebidas\n"
        else:
            metrics_text += "   ❌ Recall baixo - Muitas falhas não são detectadas\n"
        
        if self.resultados['precision'] >= 0.7:
            metrics_text += "   ✅ Precisão alta - Poucos alarmes falsos\n"
        elif self.resultados['precision'] >= 0.4:
            metrics_text += "   ⚠️ Precisão média - Alguns alarmes falsos\n"
        else:
            metrics_text += "   ❌ Precisão baixa - Muitos alarmes falsos\n"
        
        self.metrics_text.insert(1.0, metrics_text)
        
        # ============================================================
        # ABA 2: GRÁFICOS
        # ============================================================
        self.figure.clear()
        
        # Subplot 1: Matriz de Confusão
        ax1 = self.figure.add_subplot(1, 2, 1)
        cm = self.resultados['confusion_matrix']
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax1,
                   xticklabels=['Normal', 'Pré-Falha'],
                   yticklabels=['Normal', 'Pré-Falha'])
        ax1.set_title(f'Matriz de Confusão - {self.modelo.machine_name}')
        ax1.set_ylabel('Real')
        ax1.set_xlabel('Predito')
        
        # Subplot 2: Curva ROC
        ax2 = self.figure.add_subplot(1, 2, 2)
        fpr, tpr, roc_auc = self.resultados['roc_curve']
        ax2.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC (AUC = {roc_auc:.3f})')
        ax2.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--', label='Aleatório (AUC = 0.5)')
        ax2.set_xlim([0.0, 1.0])
        ax2.set_ylim([0.0, 1.05])
        ax2.set_xlabel('Falso Positivo Rate')
        ax2.set_ylabel('Verdadeiro Positivo Rate (Recall)')
        ax2.set_title(f'Curva ROC - {self.modelo.machine_name}')
        ax2.legend(loc="lower right")
        ax2.grid(True, alpha=0.3)
        
        self.figure.tight_layout()
        self.canvas.draw()
        
        # ============================================================
        # ABA 3: FEATURES IMPORTANTES
        # ============================================================
        self.features_figure.clear()
        
        if self.resultados['feature_importance'] is not None:
            ax = self.features_figure.add_subplot(111)
            imp_df = self.resultados['feature_importance'].head(20)
            
            colors = sns.color_palette('viridis', n_colors=len(imp_df))
            bars = ax.barh(range(len(imp_df)), imp_df['importance'].values, color=colors)
            ax.set_yticks(range(len(imp_df)))
            ax.set_yticklabels(imp_df['feature'].values, fontsize=8)
            ax.set_xlabel('Importância', fontsize=10)
            ax.set_title(f'Top 20 Features Mais Importantes - {self.modelo.machine_name}', fontsize=12)
            ax.invert_yaxis()
            ax.grid(True, alpha=0.3)
            
            self.features_figure.tight_layout()
            self.features_canvas.draw()
        
        # ============================================================
        # ABA 4: TAGS REMOVIDAS
        # ============================================================
        self.removed_text.delete(1.0, tk.END)
        
        if self.modelo.removed_tags:
            removed_text = f"""
{'='*60}
🗑️ TAGS REMOVIDAS DO TREINAMENTO
{'='*60}

Foram removidas {len(self.modelo.removed_tags)} tags que representam
CONSEQUÊNCIAS da falha (e não causas). Isso evita overfitting.

Tags removidas:
"""
            for i, tag in enumerate(self.modelo.removed_tags[:30], 1):
                removed_text += f"   {i:3d}. {tag}\n"
            
            if len(self.modelo.removed_tags) > 30:
                removed_text += f"\n   ... e mais {len(self.modelo.removed_tags) - 30} tags"
            
            removed_text += """

📌 Por que remover estas tags?
   Tags como 'production_counter', 'runtime_hours' e 'failure_counter'
   são consequências da falha (a produção para, o contador para de
   aumentar). Incluí-las no treinamento causaria overfitting e daria
   uma falsa sensação de acurácia.
"""
        else:
            removed_text = "Nenhuma tag foi removida."
        
        self.removed_text.insert(1.0, removed_text)
    
    def run(self):
        """Executa a aplicação"""
        self.root.mainloop()


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

if __name__ == "__main__":
    
    print("=" * 70)
    print("🤖 SISTEMA DE MANUTENÇÃO PREDITIVA - TREINAMENTO")
    print("=" * 70)
    
    # Carrega o dataset completo
    if os.path.exists('data_ml_complete.parquet'):
        print("\n📂 Carregando dataset...")
        df = pd.read_parquet('data_ml_complete.parquet')
        print(f"   Dataset carregado: {df.shape}")
        print(f"   Máquinas no dataset: {df['location_name'].nunique()}")
        print(f"   Período: {df['timestamp'].min()} a {df['timestamp'].max()}")
        
        # Inicia a interface gráfica
        print("\n🚀 Iniciando interface gráfica...")
        app = ModelTrainingApp(df)
        app.run()
        
    else:
        print("\n❌ Arquivo data_ml_complete.parquet não encontrado!")
        print("   Execute o predictive_maintenance.py primeiro para gerar o dataset.")