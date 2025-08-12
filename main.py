from ETL import *
from functions import is_test

test = False

if __name__ == "__main__":
    
    clientes, userids = funcao_clientes(test)
    cadastros = funcao_cadastros(userids, test)
    interesses = funcao_interesses(userids, test)
    transacoes = funcao_transacoes(userids, test)
    historico_acessos_individuais_wifi = funcao_historico_acessos_individuais_wifi(userids)
    comportamento_wifi = funcao_comportamento_wifi(userids)
    df_userids = funcao_userid(userids)

    load_to_postgres(clientes, "d_clientes", dbname="prod", schema="mart", chunksize=100000, how="truncate_append")
    load_to_postgres(interesses, "d_interesses", dbname="prod", schema="mart", chunksize=100000, how="truncate_append")
    load_to_postgres(cadastros, "f_cadastros", dbname="prod", schema="mart", chunksize=100000, how="truncate_append")
    load_to_postgres(transacoes, "f_transacoes", dbname="prod", schema="mart", chunksize=100000, how="truncate_append")

    load_to_postgres(historico_acessos_individuais_wifi, "f_historico_acessos_individuais_wifi", dbname="prod", schema="mart", chunksize=100000, how="truncate_append")
    load_to_postgres(comportamento_wifi, "d_comportamento_wifi", dbname="prod", schema="mart", chunksize=100000, how="truncate_append")

    upsert_to_postgres(df_userids, dbname='prod', table='d_userid', schema='mart', conflict_cols=['id'])