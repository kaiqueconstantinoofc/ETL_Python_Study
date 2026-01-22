# Projeto: Pipeline ETL de Vendas (Sales Data Pipeline)
Este projeto consiste em uma pipeline de dados (ETL) desenvolvida em Python, simulando um cenário real de Supply Chain e Vendas. O objetivo é automatizar a ingestão de dados brutos, aplicar regras de negócio para saneamento (Data Quality) e gerar bases confiáveis para análise de faturamento.
1. Visão Geral
Este projeto consiste em uma pipeline de dados (ETL) desenvolvida em Python, simulando um cenário real de Supply Chain e Vendas. O objetivo é automatizar a ingestão de dados brutos, aplicar regras de negócio para saneamento (Data Quality) e gerar bases confiáveis para análise de faturamento.

O script simula a extração de um sistema ERP (via CSV), trata inconsistências comuns em bases operacionais e entrega dados prontos para visualização em ferramentas de BI (como Power BI).

2. Tecnologias Utilizadas
Linguagem: Python 3.x

Manipulação de Dados: Pandas

Automação de Sistema: Bibliotecas nativas (os, datetime)

3. Arquitetura da Pipeline (ETL)
O processo foi dividido em três estágios modulares para garantir escalabilidade e fácil manutenção:

📥 1. Extract (Extração)
Simula a leitura de dados transacionais brutos (vendas_bruto.csv).

Implementa tratamento de exceção (try/except) para evitar falhas críticas caso a origem dos dados esteja indisponível.

🛠️ 2. Transform (Transformação)
Fase onde ocorre a "mágica" da engenharia de features e limpeza:

Sanity Check: Remoção de registros com quantidades negativas ou nulas (filtros de devoluções ou erros de input).

Tratamento de Datas: Conversão de strings para objetos datetime e remoção de registros com datas inválidas (coerce).

Enriquecimento: Cálculo do GMV (Gross Merchandise Value) através da multiplicação quantidade * preco_unitario.

Agregação: Criação de um dataset sumarizado por Categoria/Produto para análises executivas rápidas.

📤 3. Load (Carga)
Exportação dos dados em formato .csv para uma pasta de saída (/output).

Versionamento: Aplicação de timestamps nos nomes dos arquivos (ex: vendas_full_20260122.csv) para manter histórico e evitar sobrescrita de dados.

4. Regras de Negócio Aplicadas
Durante o desenvolvimento, as seguintes premissas foram adotadas para garantir a qualidade do dado:

Regra	Ação no Código	Justificativa
Erros de Digitação	errors='coerce' nas datas	Evitar quebra da pipeline por erros humanos no input.
Vendas Inválidas	Filtro quantidade > 0	Quantidades negativas distorcem o faturamento total.
Visão Executiva	groupby por Produto	Facilitar a importação direta para dashboards de performance.
5. Como Executar
Certifique-se de ter o Python e o Pandas instalados:

bash
pip install pandas
Execute o script principal:

bash
python etl_vendas.py
Verifique a pasta output gerada automaticamente.
