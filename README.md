# ETL de Infrações de Trânsito de Recife

Este projeto implementa um pipeline de **ETL (Extract, Transform, Load)** que extrai (requests, pandas) dados sobre infrações de trânsito do [Portal de Dados Abertos do Recife](http://dados.recife.pe.gov.br/), realiza um processo de limpeza e transformação (pandas) dos dados, e os carrega em um banco de dados PostgreSQL (Supabase) para futuras análises.

## Funcionalidades

- **Extração**: Coleta de dados via API pública da Prefeitura do Recife.
- **Transformação**:
    - Limpeza e normalização de colunas.
    - Conversão e extração de componentes de data e hora (`ano`, `mes`, `hora`).
    - Criação de novas features, como a identificação de feriados (`is_feriado`).
    - Remoção de dados duplicados e nulos para garantir a qualidade.
- **Carregamento**: Inserção dos dados tratados de forma segura e eficiente em uma tabela PostgreSQL.
- **Modularidade**: O código é organizado em classes (`Pipeline` e `Manager`) que separam as responsabilidades do processo de ETL.
- **Configuração Segura**: Utiliza variáveis de ambiente (`.env`) para gerenciar credenciais do banco de dados, evitando exposição de informações sensíveis no código.

## Tecnologias Utilizadas

- **Linguagem**: Python 3
- **Bibliotecas Principais**:
    - `requests`: para realizar as chamadas à API.
    - `pandas`: para manipulação e transformação dos dados.
    - `psycopg2-binary`: para a conexão com o banco de dados PostgreSQL.
    - `python-dotenv`: para o gerenciamento de variáveis de ambiente.
    - supabase
- **Banco de Dados**: PostgreSQL

## Pré-requisitos

Antes de começar, garanta que você tenha os seguintes softwares instalados:
- [Python 3.8+](https://www.python.org/downloads/)
- [Pip](https://pip.pypa.io/en/stable/installation/) (geralmente já vem com o Python)
- Um servidor [PostgreSQL](https://www.postgresql.org/download/) ativo e acessível.

## Instalação e Configuração

Siga os passos abaixo para executar o projeto localmente.

### 1. Clone o Repositório
```bash
git clone git@github.com:ruantos/Infracoes-em-Recife.git
cd Infracoes-em-Recife
```

### 2. Crie e Ative um Ambiente Virtual

É uma boa prática usar um ambiente virtual para isolar as dependências do projeto.
```bash
python3 -m venv venv
```
- Ativar no Linux/macOS
```bash
source venv/bin/activate
```
- Ativar no Windows
```bash
.\venv\Scripts\activate
```
### 3. Instale as Dependências

Copie o seguinte comando para baixar as dependências:
```bash
pip install -r requirements.tx
```
### 4. Configure as Variáveis de Ambiente

Crie um arquivo chamado .env na raiz do projeto. Ele guardará as credenciais de acesso ao seu banco de dados. Copie o conteúdo abaixo e preencha com suas informações.
```bash
# .env
DB_HOST=localhost
DATABASE=etl
DB_USER=postgres
DB_PASSWORD=sua_senha
PORT=5432
```

## Como usar?

Basta navegar até o diretório do projeto e digitar:
```bash
python3 src/script.py
```
