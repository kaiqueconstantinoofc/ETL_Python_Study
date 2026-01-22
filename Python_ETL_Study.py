import pandas as pd
import os
from datetime import datetime

# ==============================================================================
# 1. SETUP INICIAL (Massa de dados)
# ==============================================================================
def criar_dados_teste():
    """
    Gera um CSV fake para simular uma extração do sistema (ERP).
    Coloquei de propósito alguns dados sujos (datas erradas, qtd negativa) 
    para testar a robustez do tratamento.
    """
    dados = {
        'id_venda': [101, 102, 103, 104, 105, 106],
        'data': ['2025-01-20', '2025-01-21', '2025-01-21', 'data_quebrada', '2025-01-22', '2025-01-22'],
        'produto': ['Notebook', 'Mouse', 'Teclado', 'Monitor', 'Mouse', 'Notebook'],
        'quantidade': [1, 10, 5, 2, -3, 2],  # Erro clássico: quantidade negativa
        'preco_unitario': [3500.00, 50.00, 100.00, 800.00, 50.00, 3400.00]
    }
    df = pd.DataFrame(dados)
    df.to_csv('vendas_bruto.csv', index=False)
    print("LOG: Base de testes 'vendas_bruto.csv' gerada.")

# ==============================================================================
# 2. PIPELINE DE DADOS (ETL)
# ==============================================================================

def extract(arquivo_origem):
    """Etapa de Extração: Carrega o arquivo bruto para a memória."""
    print(f"\n[1/3 EXTRACT] Lendo o arquivo: {arquivo_origem}")
    try:
        df = pd.read_csv(arquivo_origem)
        print(f"   -> Sucesso! {len(df)} registros carregados.")
        return df
    except FileNotFoundError:
        print("   -> Erro Crítico: O arquivo não existe. Verifique o caminho.")
        return None

def transform(df):
    """
    Etapa de Transformação:
    Aqui acontece a 'mágica': limpeza de sujeira, tipagem de dados 
    e criação de novas colunas (feature engineering simples).
    """
    print("[2/3 TRANSFORM] Iniciando higienização e cálculos...")
    
    # 1. Sanity Check: Remover quantidades negativas ou zeradas (erro de sistema ou devolução)
    # Isso garante que não vamos sujar o cálculo de faturamento depois
    df_clean = df[df['quantidade'] > 0].copy()
    
    # 2. Tratamento de Datas: O que não for data vira NaT e a gente dropa
    # O 'coerce' é vital aqui pra não quebrar o pipeline se vier lixo no campo data
    df_clean['data'] = pd.to_datetime(df_clean['data'], errors='coerce')
    df_clean.dropna(subset=['data'], inplace=True)
    
    # 3. Regra de Negócio: Calcular o GMV (Gross Merchandise Value) por linha
    df_clean['faturamento_total'] = df_clean['quantidade'] * df_clean['preco_unitario']
    
    # 4. Agregação: Criando uma visão sumarizada para facilitar a análise executiva depois
    print("   -> Gerando visão agrupada por Produto...")
    df_agregado = df_clean.groupby('produto')[['faturamento_total', 'quantidade']].sum().reset_index()
    
    # Ordenando pra ficar mais bonito no relatório final (maior faturamento primeiro)
    df_agregado = df_agregado.sort_values(by='faturamento_total', ascending=False)
    
    linhas_removidas = len(df) - len(df_clean)
    print(f"   -> Limpeza concluída. {linhas_removidas} linhas de lixo removidas.")
    
    return df_clean, df_agregado

def load(df_detalhado, df_agregado, pasta_destino='output'):
    """
    Etapa de Carga: Salva os arquivos processados.
    Uso timestamp no nome para manter histórico e não sobrescrever dados antigos.
    """
    print("[3/3 LOAD] Exportando os dados tratados...")
    
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
    
    # Gerando timestamp pro nome do arquivo (YYYYMMDD_HHMMSS)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    path_detalhado = f"{pasta_destino}/vendas_full_{timestamp}.csv"
    path_agregado = f"{pasta_destino}/kpi_produtos_{timestamp}.csv"
    
    df_detalhado.to_csv(path_detalhado, index=False)
    df_agregado.to_csv(path_agregado, index=False)
    
    print(f"   -> Base Analítica salva em: {path_detalhado}")
    print(f"   -> Relatório Executivo salvo em: {path_agregado}")

# ==============================================================================
# ORQUESTRAÇÃO
# ==============================================================================
if __name__ == "__main__":
    # Gera a massa de dados só pra gente ter o que rodar
    criar_dados_teste()
    
    # Executa o pipeline sequencialmente
    df_raw = extract('vendas_bruto.csv')
    
    if df_raw is not None:
        df_tratado, df_kpis = transform(df_raw)
        load(df_tratado, df_kpis)
        print("\nPipeline finalizado! Dados prontos para o Power BI/Dashboard. 🚀")
