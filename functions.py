import pandas as pd
import json
import hashlib
import gender_guesser.detector as gender
from functions import *
from unidecode import unidecode
import difflib
import re
from datetime import datetime
import phonenumbers
from phonenumbers import PhoneNumberFormat, NumberParseException

def is_test(teste):
    if teste == True:
        return f' ORDER BY RAND() LIMIT 2000'
    else:
        return ''

def hash_string(cpf):
    hash_object = hashlib.sha256(cpf.encode())
    return hash_object.hexdigest()[:20]

def create_new_userids(df):
    try:
        with open('dicionario_id_userid.json', 'r') as arquivo_json:
            userids = json.load(arquivo_json)
    except:
        userids = {}
    df_sem_userid = df[~df['id'].isin(list(userids.keys()))]
    df_sem_userid['Coalesceid'] = df_sem_userid['cpf'].fillna(df_sem_userid['nomeemail']).fillna(df_sem_userid['id'])
    df_sem_userid = df_sem_userid.drop('nomeemail', axis=1)
    df_sem_userid['userid'] = df_sem_userid['Coalesceid'].apply(hash_string)
    novo_userids = df_sem_userid[['userid', 'id']].set_index('id').to_dict()['userid']
    userids.update(novo_userids)
    with open('dicionario_id_userid.json', 'w') as arquivo_json:
        json.dump(userids, arquivo_json)
    return userids

def enriquece_cpf(df):
    df = df.sort_values('atualizado')
    df_com_nomeemail = df.loc[(~df['nome'].isna()) & (~df['email'].isna())]
    df_sem_nomeemail = df.loc[~((~df['nome'].isna()) & (~df['email'].isna()))]
    df_com_nomeemail['nomeemail'] = df_com_nomeemail['email'].astype(str) + df_com_nomeemail['nome'].astype(str)
    df_com_nomeemail['cpf'] = df_com_nomeemail.groupby('nomeemail')['cpf'].bfill()
    df_com_nomeemail['cpf'] = df_com_nomeemail.groupby('nomeemail')['cpf'].ffill()
    df_final = pd.concat([df_com_nomeemail, df_sem_nomeemail])
    return df_final

def enriquece_dados(df):
    df = df.sort_values('atualizado')
    df_com_cpf = df.loc[~df['cpf'].isna()]
    df_sem_cpf = df.loc[df['cpf'].isna()]
    for coluna in ['nome', 'nascimento', 'genero', 'email', 'telefone', 'cep']:
        df_com_cpf[coluna] = df_com_cpf.groupby('cpf')[coluna].bfill()
        df_com_cpf[coluna] = df_com_cpf.groupby('cpf')[coluna].ffill()
    df_final = pd.concat([df_com_cpf, df_sem_cpf])
    return df_final

def primeira_data_cadastro(df):    
    df['Cadastro'] = pd.to_datetime(df['Cadastro'])
    df['Cadastro'] = df.groupby('userid')['Cadastro'].transform('min')
    df['Cadastro'] = df['Cadastro'].astype(str)
    return df

def fill_missing_gender(df):
    d = gender.Detector()
    dicionario_generos = {'female': 'FEMININO', 'male': 'MASCULINO'}    
    def get_gender(nome):
        if pd.notna(nome):
            primeiro_nome = nome.split(' ')[0].capitalize()
            genero = d.get_gender(primeiro_nome)
            return dicionario_generos.get(genero)
        return None
    df['genero'] = df.apply(lambda row: row['genero'] if pd.notna(row['genero']) else get_gender(row['nome']), axis=1)
    male_names = r'\b(Luiz|Thiago|Davi|Robson|Wellington|Willian|Fábio|Vinícius|Everton|Jeferson|Junior|Antônio|Murilo|Breno|Ryan|Kaique|Kauan|Jhonatan|Adilson|Kaio|Gilson|Caique|Kauã|Alexsandro|Jonatas|Washington|Higor|Maicon|Sidney|Eder|Fabrício|Weslley|Yago|Maycon|Sidnei|Edilson|Ailton|Cauã|Valdir|Wanderson)\b'
    female_names = r'\b(Thais|Andressa|Kelly|Gisele|Daiane|Talita|Vitoria|Janaina|Giovana|Tamires|Elisangela|Dayane|Thaís|Nayara|Lais|Sueli|Regiane|Andreza|Carol|Gabrielly|Yasmim|Isadora|Roseli|Thamires|Suelen|Geovana|Aparecida|Luciene|Rayssa|Thayna|Rayane|Tais|Geovanna|Lidiane|Laís|Heloisa|Isabelly|Deise|Mônica|Maiara|Rafaela|Glaucia|Stefany|Keila|Cleide|Franciele|Nathália|Tainá|Iara|Nicolly|Cibele|Laryssa|Janaína|Thays|Rosemeire|Andréia|Nicoly)\b'
    df.loc[df['nome'].str.contains(male_names, case=False, na=False), 'genero'] = 'MASCULINO'
    df.loc[df['nome'].str.contains(female_names, case=False, na=False), 'genero'] = 'FEMININO'    
    return df

def fix_email_domain(df):
    corrections = {
        '@gmai.com': '@gmail.com', '@gamil.com': '@gmail.com', '@gmal.com': '@gmail.com', 
        '@gmil.com': '@gmail.com', '@gsmsil.com': '@gmail.com', '@gmail.coml': '@gmail.com', 
        '@gmail.vom': '@gmail.com', '@gmqil.com': '@gmail.com', '@gemail.com': '@gmail.com', 
        '@gmail.coma': '@gmail.com', '@hormail.com': '@hotmail.com', '@homail.com': '@hotmail.com', 
        '@hotmail.con': '@hotmail.com', '@hotmai.com': '@hotmail.com', '@hotnail.com': '@hotmail.com', 
        '@hotmial.com': '@hotmail.com', '@hotmil.com': '@hotmail.com', '@hotamil.com': '@hotmail.com', 
        '@htmail.com': '@hotmail.com', '@jotmail.com': '@hotmail.com', '@hotmail.vom': '@hotmail.com', 
        '@yahool.com': '@yahoo.com', '@yahoo.com.be': '@yahoo.com', '@yhaoo.com.br': '@yahoo.com', 
        '@yahoo.com.bar': '@yahoo.com'
    }
    df['email'] = df['email'].str.replace(r'\.com\.com$', '.com', regex=True)
    for wrong, correct in corrections.items():
        df['email'] = df['email'].str.replace(wrong, correct, regex=True)    
    return df

def fix_phonenumbers(df):
    def normalize(telefone_raw, default_region="BR"):
        if telefone_raw is None or str(telefone_raw).strip() == "":
            return None
        try:
            num = phonenumbers.parse(str(telefone_raw), default_region)
            if not phonenumbers.is_valid_number(num):
                return None
            return phonenumbers.format_number(num, PhoneNumberFormat.E164).replace("+", "")
        except NumberParseException:
            return None
    df['telefone'] = df['telefone'].apply(normalize)
    return df

def ajuste_nome_marcas(df, nome):
    # Remove Bug '/&#39;' dos nomes 
    df[nome] = df[nome].apply(unidecode).replace('/&#39;', '')

    # Remove caracteres que não sejam letras
    df[nome] = df[nome].apply(remove_non_letters)

    # Remove espaços extras
    df[nome] = df[nome].apply(remove_extra_spaces)

    # Unifica lojas com nomes parecidos
    dicionario = find_similar_strings(df[nome].unique())
    df[nome] = df[nome].map(dicionario).fillna(df[nome])

    return df

def calcular_faixa_etaria(df, coluna_nascimento='nascimento'):

    df[coluna_nascimento] = pd.to_datetime(df[coluna_nascimento], errors='coerce')
    hoje = pd.to_datetime(datetime.today())
    df['idade'] = df[coluna_nascimento].apply(lambda x: (hoje - x).days // 365 if pd.notnull(x) else None)

    def faixa(idade):
        if pd.isnull(idade):
            return 'desconhecido'
        elif idade < 18:
            return '< 18'
        elif idade < 30:
            return '18 a 29'
        elif idade < 40:
            return '30 a 39'
        elif idade < 50:
            return '40 a 49'
        elif idade < 60:
            return '50 a 59'
        elif idade < 70:
            return '60 a 69'
        else:
            return '70+'

    df['faixa_etaria'] = df['idade'].apply(faixa)

    df = df.drop('idade', axis=1)

    return df

def faixa_valor(valor):
        if pd.isnull(valor):
            return 'desconhecido'
        elif valor < 100:
            return '< R$100'
        elif valor <= 300:
            return 'R$101 a R$300'
        elif valor <= 500:
            return 'R$301 a R$500'
        elif valor <= 750:
            return 'R$501 a R$750'
        elif valor <= 1000:
            return 'R$751 a R$1000'
        else:
            return 'R$1000+'
        
def ordem_faixa_valor(faixa):
    if faixa == '< R$100':
        return 1
    elif faixa == 'R$101 a R$300':
        return 2
    elif faixa == 'R$301 a R$500':
        return 3
    elif faixa == 'R$501 a R$750':
        return 4
    elif faixa == 'R$751 a R$1000':
        return 5
    elif faixa == 'R$1000+':
        return 6
    else:
        return 0  # Para 'desconhecido' ou valores fora do padrão

#* Funções de Ajuste para nomes de Lojas
def remove_non_letters(s):
    return re.sub(r'[^a-zA-Z\s]', '', s)

def remove_extra_spaces(s):
    return re.sub(r'\s+', ' ', s).strip()

def find_similar_strings(word_list, cutoff=0.88):
    result = {}
    for word in word_list:
        close_match = difflib.get_close_matches(word, word_list, n=2, cutoff=cutoff)
        close_match = [match for match in close_match if match != word]        
        if close_match and close_match[0] in result:
            continue        
        result[word] = close_match[0] if close_match else None
    result = {k: v for k, v in result.items() if v is not None}
    return result
