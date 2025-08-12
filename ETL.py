import pandas as pd
from functions import *
import sys
sys.path.append('/home/opc/scripts')
from connections import *

def funcao_clientes(test=False):
    
    if test:
        cl = query_pg(f"SELECT * FROM staging.stg_clientes ORDER BY RANDOM() LIMIT 2000")
    else:
        cl = query_pg(f"SELECT * FROM staging.stg_clientes")
    
    cl = cl.reset_index(drop=True)

    # Enriquece cpf's através de nomeemail
    cl = enriquece_cpf(cl)

    # Enriquece demais dados através do cpf
    cl = enriquece_dados(cl)

    # Remove usuários sem email e cpf
    cl = cl.loc[~(cl['email'].isna() & cl['cpf'].isna())]

    # Cria userid pros novos usuários
    userids = create_new_userids(cl)

    # Atribui os ids
    cl['userid'] = cl['id'].map(userids)

    # Remove duplicadas mantendo as informações mais recentes
    cl = cl.sort_values('atualizado', ascending=False)
    cl = cl.drop_duplicates(subset=['userid'], keep='first')

    # Seleciona Colunas
    cl = cl[['userid', 'cpf', 'nome', 'nascimento', 'genero', 'email', 'telefone', 'cep', 'atualizado']]

    # Ajustes email e identificação de Gênero
    cl = fill_missing_gender(cl)
    cl = fix_email_domain(cl)
    cl = fix_phonenumbers(cl)

    # Ajustes horários 
    cl = cl[(cl['atualizado'] > '2016-01-01 00:00:00')]

    # Ordena e reseta o index
    cl = cl.sort_values('atualizado', ascending=False).reset_index(drop=True)

    cl = cl.drop_duplicates()

    cl = calcular_faixa_etaria(cl, coluna_nascimento='nascimento')

    cl.loc[~cl['genero'].isin(['MASCULINO', 'FEMININO']), 'genero'] = 'NÃO INFORMADO'

    # Seleciona Colunas
    cl = cl[['userid', 'cpf', 'nome', 'nascimento', 'genero', 'email', 'telefone', 'cep', 'faixa_etaria', 'atualizado']]

    return cl, userids

def funcao_transacoes(userids, test=False):

    if test:
        df_transacoes = query_pg(f"""SELECT * FROM staging.stg_transacoes WHERE data >= '2017-01-01' ORDER BY RANDOM() LIMIT 2000""")
    else:
        df_transacoes = query_pg(f"""SELECT * FROM staging.stg_transacoes WHERE data >= '2017-01-01'""")

    df_transacoes['userid'] = df_transacoes['id'].map(userids)
    df_transacoes = df_transacoes.dropna(subset=['userid'])
    df_transacoes = df_transacoes.drop('id', axis=1)

    # identificação das lojas
    contratos = query_pg("SELECT UE as empreendimento, contrato, rede, categoria, ramo FROM mart.d_contratos")
    df_transacoes = df_transacoes.merge(contratos, on=['empreendimento', 'contrato'], how='left')
    df_transacoes = df_transacoes.drop_duplicates(subset=['transacaoid'], keep='first')
    df_transacoes = df_transacoes.reset_index(drop=True)

    df_transacoes['faixa_valor'] = df_transacoes['valor'].apply(lambda x: faixa_valor(x))
    df_transacoes['ordem_faixa_valor'] = df_transacoes['faixa_valor'].apply(ordem_faixa_valor)

    df_transacoes = df_transacoes[['transacaoid', 'userid', 'loja', 'data',  'origem', 'valor', 'faixa_valor', 'ordem_faixa_valor', 'empreendimento', 'contrato', 'campanhaid', 'rede', 'categoria', 'ramo']].copy()
    
    return df_transacoes

def funcao_interesses(userids, test=False):
    
    if test:
        df_interesses = query_pg(f"""SELECT * FROM staging.stg_interesses ORDER BY RANDOM() LIMIT 2000""")
    
    else:
        df_interesses = query_pg(f"""SELECT * FROM staging.stg_interesses""")
        
    df_interesses['userid'] = df_interesses['id'].map(userids)
    df_interesses = df_interesses.dropna(subset=['userid'])

    df_interesses = df_interesses.drop('id', axis=1)

    df_interesses = df_interesses[['userid', 'marca', 'origem']].copy()

    return df_interesses

def funcao_cadastros(userids, test=False):
    
    if test:
        df_cadastros = query_pg(f"""SELECT * FROM staging.stg_cadastros ORDER BY RANDOM() LIMIT 2000""")
    else:
        df_cadastros = query_pg(f"""SELECT * FROM staging.stg_cadastros""")

    df_cadastros['userid'] = df_cadastros['id'].map(userids)
    df_cadastros = df_cadastros.dropna(subset=['userid'])

    df_cadastros = df_cadastros.drop('id', axis=1)

    return df_cadastros

def funcao_historico_acessos_individuais_wifi(userids):
    df = query_pg(f"SELECT * FROM raw.historico_acessos_individuais_wifi")

    df['id'] = "WIFI-" + df['id'].astype(str)
    df['userid'] = df['id'].map(userids)
    df = df[~df['userid'].isna()]
    df = df.reset_index(drop=True)

    df = df[['userid', 'empreendimento', 'started', 'closed']].copy()

    return df

def funcao_comportamento_wifi(userids):
    df = query_pg(f"SELECT * FROM staging.stg_agregacoes_acessos_individuais_wifi")

    df['id'] = "WIFI-" + df['id'].astype(str)
    df['userid'] = df['id'].map(userids)
    df = df[~df['userid'].isna()]
    df = df.reset_index(drop=True)

    df = df[['userid', 'empreendimento', 'dia_favorito', 'ultima_visita', 'visitas', 'visitas_l30', 'visitas_l365', 'tempo_medio_conexao_minutos', 'cliente_lojista']].copy()

    return df

def funcao_userid(userids):
    df = pd.DataFrame(list(userids.items()), columns=["id", "userid"])
    return df
