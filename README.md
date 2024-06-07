# Teste SEPLAN Engenheiro de Dados

Acessar a API de empreendimentos geração distribuída e subir em um banco de dados.

## Pré-requisitos

- Python 3.x
- Bibliotecas necessárias: `requests`

## Instalação

1. Clone o repositório para o seu ambiente local:

```
git clone https://github.com/camposvictor/teste-seplan.git
```

2. Instale as dependências usando pip:

```
pip install -r requirements.txt
```

## Uso

Crie o banco de dados SQLite:

```
sqlite3 ./db/emprendimentos.db < ./db/generate_tables.sql
```

Execute o script `main.py` para iniciar o processo de extração e inserção de dados:

```
python main.py
```

## Estrutura do Projeto

- `main.py`: Ponto de entrada do programa.
- `db/emprendimentos.db`: Banco de dados SQLite.
- `db/generate_tables`: Script para geração das tabelas.
