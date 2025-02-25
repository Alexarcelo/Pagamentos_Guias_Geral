import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import gspread
from google.oauth2 import service_account
from datetime import timedelta, time
from babel.numbers import format_currency
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from st_aggrid import AgGrid, GridOptionsBuilder

def gerar_df_phoenix(vw_name, base_luck):

    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    if vw_name=='vw_sales':
        request_name = f'SELECT `Cod_Reserva`, `Data Execucao`, `Nome_Servico`, `Valor_Servico`, `Desconto_Global`, `Data_Servico` FROM {vw_name}'
    else:
        request_name = f'SELECT * FROM {vw_name}'
        
    cursor.execute(request_name)
    resultado = cursor.fetchall()
    cabecalho = [desc[0] for desc in cursor.description]
    cursor.close()
    conexao.close()
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def puxar_dados_phoenix():

    st.session_state.view_phoenix = 'vw_pagamento_fornecedores'

    st.session_state.df_escalas_bruto = gerar_df_phoenix('vw_pagamento_fornecedores', st.session_state.base_luck)

    st.session_state.df_escalas = st.session_state.df_escalas_bruto[~(st.session_state.df_escalas_bruto['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO', 'RASCUNHO'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Status da Reserva'])) & ~(pd.isna(st.session_state.df_escalas_bruto['Escala']))]\
                                                                        .reset_index(drop=True)
    
    st.session_state.df_veiculos = gerar_df_phoenix('vw_veiculos', st.session_state.base_luck)

    st.session_state.df_veiculos = st.session_state.df_veiculos.rename(columns={'name': 'Veiculo', 'Fornecedor Veiculo': 'Fornecedor Motorista'})
    
    if st.session_state.base_luck=='test_phoenix_recife':

        st.session_state.df_veiculos['Fornecedor Motorista'] = st.session_state.df_veiculos['Fornecedor Motorista'].replace({'SV VAN NOITE': 'SALVATORE'})

    elif st.session_state.base_luck=='test_phoenix_joao_pessoa':
    
        st.session_state.df_sales = gerar_df_phoenix('vw_sales', st.session_state.base_luck)

        st.session_state.df_sales = st.session_state.df_sales[~st.session_state.df_sales['Nome_Servico'].isin(st.session_state.excluir_servicos_df_sales)].reset_index(drop=True)

        st.session_state.df_sales['Data_Servico'] = pd.to_datetime(st.session_state.df_sales['Data_Servico'], unit='s').dt.date

        st.session_state.df_sales = st.session_state.df_sales.rename(columns={'Cod_Reserva': 'Reserva', 'Nome_Servico': 'Servico', 'Valor_Servico': 'Valor Venda', 'Desconto_Global': 'Desconto Reserva'})

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def tratar_colunas_numero_df(df, lista_colunas):

    for coluna in df.columns:

        if not coluna in lista_colunas:

            df[coluna] = (df[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

            df[coluna] = pd.to_numeric(df[coluna])

def puxar_configuracoes():

    puxar_aba_simples(st.session_state.id_gsheet, 'Configurações Fornecedores', 'df_config')

    tratar_colunas_numero_df(st.session_state.df_config, st.session_state.lista_colunas_nao_numericas)

def puxar_tarifario_fornecedores():

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Fornecedores', 'df_tarifario')

    tratar_colunas_numero_df(st.session_state.df_tarifario, st.session_state.lista_colunas_nao_numericas)

def puxar_tarifario_lanchas():

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Lanchas', 'df_tarifario_lanchas')

    tratar_colunas_numero_df(st.session_state.df_tarifario_lanchas, st.session_state.lista_colunas_nao_numericas)

def puxar_tarifario_esp_lanchas():

    puxar_aba_simples(st.session_state.id_gsheet, 'Valores Específicos Lanchas', 'df_tarifario_esp_lanchas')

    tratar_colunas_numero_df(st.session_state.df_tarifario_esp_lanchas, st.session_state.lista_colunas_nao_numericas)

def puxar_tarifario_bg_4x4():

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Buggy e 4x4', 'df_tarifario')

    tratar_colunas_numero_df(st.session_state.df_tarifario, st.session_state.lista_colunas_nao_numericas)

def puxar_pedagios():

    puxar_aba_simples(st.session_state.id_gsheet, 'Controle de Pedágios', 'df_pedagios')

    tratar_colunas_numero_df(st.session_state.df_pedagios, st.session_state.lista_colunas_nao_numericas)

def puxar_controle_no_show():

    puxar_aba_simples(st.session_state.id_gsheet, 'Controle No Show', 'df_no_show')

    tratar_colunas_numero_df(st.session_state.df_no_show, st.session_state.lista_colunas_nao_numericas)

def transformar_em_string(serie_dados):

    return ', '.join(list(set(serie_dados.dropna())))

def criar_colunas_escala_veiculo_mot_guia(df):

    df['Apoio'] = df['Apoio'].str.replace(r'Escala Auxiliar: | Veículo: | Motorista: | Guia: ', '', regex=True)

    df[['Escala', 'Veiculo', 'Motorista', 'Guia']] = ''

    df[['Escala', 'Veiculo', 'Motorista', 'Guia']] = df['Apoio'].str.split(',', expand=True)
    
    return df

def adicionar_apoios_em_dataframe(df, df_group):

    df_apoios = df[pd.notna(df['Apoio'])].reset_index(drop=True)

    df_apoios = df_apoios.groupby(['Data da Escala', 'Servico', 'Tipo de Servico', 'Apoio']).agg({'Data | Horario Apresentacao': 'min', 'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    if len(df_apoios)>0:

        df_apoios = criar_colunas_escala_veiculo_mot_guia(df_apoios)

        df_apoios = df_apoios[['Data da Escala', 'Escala', 'Tipo de Servico', 'Servico', 'Veiculo', 'Motorista', 'Guia', 'Data | Horario Apresentacao', 'Total ADT', 'Total CHD']]

        if st.session_state.base_luck=='test_phoenix_joao_pessoa':

            df_apoios['Servico'] = 'APOIO'

        elif st.session_state.base_luck=='test_phoenix_recife':

            df_apoios['Servico'] = df_apoios['Servico'].apply(lambda x: 'APOIO - PORTO DE GALINHAS' if '(PORTO DE GALINHAS)' in x or 'SAINDO DE PORTO DE GALINHAS' in x else 
                                                              'APOIO - BOA VIAGEM' if '(BOA VIAGEM' in x else x)
            
        elif st.session_state.base_luck=='test_phoenix_natal':

            df_apoios['Servico Principal'] = df_apoios['Servico']

            df_apoios['Servico'] = 'APOIO'

        df_apoios = pd.merge(df_apoios, st.session_state.df_veiculos[['Veiculo', 'Tipo Veiculo', 'Fornecedor Motorista']], on='Veiculo', how='left')

        df_final = pd.concat([df_group, df_apoios], ignore_index=True)

        return df_final
        
    else:

        return df_group

def map_regiao(servico):

    for key, value in st.session_state.dict_conjugados.items():

        if key in servico: 

            return value
        
    return None 

def filtro_tipo_servico_out_in(lista):
    encontrou_out = False
    for item in lista:
        if item == 'OUT':
            encontrou_out = True
        if item == 'IN' and encontrou_out:
            return True
    return False

def filtro_tipo_servico_in_out(lista):
    encontrou_in = False
    for item in lista:
        if item == 'IN':
            encontrou_in = True
        if item == 'OUT' and encontrou_in:
            return True
    return False

def identificar_alterar_nome_servico_conjugado(row, index, df):

    lista_index_principal = row['index'][index-1:index+1]

    novo_nome_trf = f"{row['Servico'][index-1]} + {row['Servico'][index]}"

    df.loc[lista_index_principal, ['Serviço Conjugado', 'Servico']] = ['X', novo_nome_trf]

    return df

def coletar_dados_row(row, coluna_dados, index):

    if coluna_dados=='Data | Horario Apresentacao':

        info_primeiro_trf = pd.to_datetime(row[coluna_dados][index-1], unit='s')

        info_segundo_trf = pd.to_datetime(row[coluna_dados][index], unit='s')

    else:

        info_primeiro_trf = row[coluna_dados][index-1]

        info_segundo_trf = row[coluna_dados][index]

    return info_primeiro_trf, info_segundo_trf

def coletar_dados_data_out_in_row(row, coluna_dados_out, index, tipo_primeiro_trf):

    if tipo_primeiro_trf=='OUT':

        data_hora_out = pd.to_datetime(row[coluna_dados_out][index-1], unit='s')

        data_hora_in = pd.to_datetime(row[coluna_dados_out][index-1], unit='s')

    else:

        data_hora_out = pd.to_datetime(row[coluna_dados_out][index], unit='s')

        data_hora_in = pd.to_datetime(row[coluna_dados_out][index-1], unit='s')

    return data_hora_out, data_hora_in

def identificar_trf_conjugados(df):

    df['Regiao'] = df['Servico'].apply(map_regiao)

    df['Serviço Conjugado'] = ''

    df_in_out = df[df['Servico'].isin(st.session_state.dict_conjugados)].sort_values(by=['Regiao', 'Data | Horario Apresentacao']).reset_index()

    df_in_out[df_in_out['Veiculo']=='CICERO']

    df_in_out_group = df_in_out.groupby(['Veiculo']).agg({'index': lambda x: list(x), 'Tipo de Servico': lambda x: list(x), 'Servico': lambda x: list(x), 
                                                                            'Escala': 'count', 'Data | Horario Apresentacao': lambda x: list(x), 'Horario Voo': lambda x: list(x), 
                                                                            'Regiao': lambda x: list(x), 'Tipo Veiculo': 'first', 'Fornecedor Motorista': 'first'}).reset_index()
    
    if st.session_state.base_luck=='test_phoenix_joao_pessoa':
    
        df_in_out_group = df_in_out_group[df_in_out_group['Tipo de Servico'].apply(filtro_tipo_servico_out_in)]

        for _, row in df_in_out_group.iterrows():

            lista_tipos_servicos = row['Tipo de Servico']

            for index in range(1, len(lista_tipos_servicos)):

                tipo_primeiro_trf, tipo_segundo_trf = coletar_dados_row(row, 'Tipo de Servico', index)

                if tipo_primeiro_trf=='OUT' and tipo_segundo_trf=='IN':

                    data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', index, tipo_primeiro_trf)

                    if data_hora_in - data_hora_out < timedelta(hours=4, minutes=15):

                        df = identificar_alterar_nome_servico_conjugado(row, index, df)

    elif st.session_state.base_luck=='test_phoenix_recife':

        df_in_out_group = df_in_out_group[df_in_out_group['Tipo de Servico'].apply(filtro_tipo_servico_in_out)]

        for _, row in df_in_out_group.iterrows():

            lista_tipos_servicos = row['Tipo de Servico']

            for index in range(1, len(lista_tipos_servicos)):

                tipo_primeiro_trf, tipo_segundo_trf = coletar_dados_row(row, 'Tipo de Servico', index)

                if tipo_primeiro_trf=='IN' and tipo_segundo_trf=='OUT':

                    regiao_in, regiao_out = coletar_dados_row(row, 'Regiao', index)

                    if regiao_out==regiao_in:

                        data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', index, tipo_primeiro_trf)

                        if (regiao_out=='Cabo' or regiao_out=='Porto' or regiao_out=='Serrambi') and data_hora_out - data_hora_in < timedelta(hours=3):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                        elif regiao_out=='Maragogi' and data_hora_out - data_hora_in < timedelta(hours=5):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                        elif regiao_out=='Olinda' and data_hora_out - data_hora_in < timedelta(hours=2):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

    elif st.session_state.base_luck=='test_phoenix_natal':

        df_in_out_group = df_in_out_group[df_in_out_group['Escala']>1]

        for _, row in df_in_out_group.iterrows():

            lista_tipos_servicos = row['Tipo de Servico']

            for index in range(1, len(lista_tipos_servicos)):

                regiao_in, regiao_out = coletar_dados_row(row, 'Regiao', index)

                if regiao_out==regiao_in:

                    tipo_primeiro_trf, tipo_segundo_trf = coletar_dados_row(row, 'Tipo de Servico', index)

                    if tipo_primeiro_trf=='IN' and tipo_segundo_trf=='OUT':

                        data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', index, tipo_primeiro_trf)

                        if regiao_out!='Natal' and data_hora_out - data_hora_in < timedelta(hours=4):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                    elif tipo_primeiro_trf=='OUT' and tipo_segundo_trf=='IN':

                        data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', index, tipo_primeiro_trf)

                        tipo_veiculo = row['Tipo Veiculo']

                        if regiao_out=='Natal' and (tipo_veiculo=='Bus' or tipo_veiculo=='Micro') and data_hora_in - data_hora_out < timedelta(hours=4, minutes=30):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                        elif regiao_out=='Natal' and data_hora_in - data_hora_out < timedelta(hours=2, minutes=30):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                        elif regiao_out=='Pipa' and data_hora_in - data_hora_out < timedelta(hours=3, minutes=40) and row['Fornecedor Motorista'] in ['DAMIAO PIPA', 'KLEBER LUIZ', 'LUIZ ANTONIO']:

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                        elif regiao_out=='Touros' and data_hora_in - data_hora_out < timedelta(hours=3, minutes=40) and row['Fornecedor Motorista'] in ['JUDSON', 'JULIO CESAR', 'ADRIANO TOUROS']:

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

    elif st.session_state.base_luck=='test_phoenix_salvador':

        df_in_out_group = df_in_out_group[df_in_out_group['Tipo de Servico'].apply(filtro_tipo_servico_in_out)]

        for _, row in df_in_out_group.iterrows():

            lista_tipos_servicos = row['Tipo de Servico']

            for index in range(1, len(lista_tipos_servicos)):

                tipo_primeiro_trf, tipo_segundo_trf = coletar_dados_row(row, 'Tipo de Servico', index)

                if tipo_primeiro_trf=='IN' and tipo_segundo_trf=='OUT':

                    regiao_in, regiao_out = coletar_dados_row(row, 'Regiao', index)

                    if regiao_out==regiao_in:

                        data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', index, tipo_primeiro_trf)

                        if regiao_out=='Litoral Norte' and data_hora_out - data_hora_in < timedelta(hours=4):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                        elif regiao_out=='Baixio' and data_hora_out - data_hora_in < timedelta(hours=4, minutes=30):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

    elif st.session_state.base_luck=='test_phoenix_aracaju':

        df_in_out_group = df_in_out_group[df_in_out_group['Escala']>1]

        for _, row in df_in_out_group.iterrows():

            lista_tipos_servicos = row['Tipo de Servico']

            for index in range(1, len(lista_tipos_servicos)):

                regiao_in, regiao_out = coletar_dados_row(row, 'Regiao', index)

                tipo_primeiro_trf, tipo_segundo_trf = coletar_dados_row(row, 'Tipo de Servico', index)

                if regiao_out==regiao_in:

                    data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', index, tipo_primeiro_trf)

                    if tipo_primeiro_trf=='IN' and tipo_segundo_trf=='OUT':

                        if regiao_out=='Aracaju' and data_hora_out - data_hora_in < timedelta(hours=1, minutes=30):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                        if regiao_out=='Makai' and data_hora_out - data_hora_in < timedelta(hours=1, minutes=20):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                    elif tipo_primeiro_trf=='OUT' and tipo_segundo_trf=='IN':

                        if regiao_out=='Aracaju' and data_hora_in - data_hora_out < timedelta(hours=1, minutes=30):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                        if regiao_out=='Makai' and data_hora_in - data_hora_out < timedelta(hours=2):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

                elif tipo_primeiro_trf=='OUT' and tipo_segundo_trf=='IN': 
                        
                        regiao_out, regiao_in = coletar_dados_row(row, 'Regiao', index)
                        
                        data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', index, tipo_primeiro_trf)
                        
                        if regiao_out=='Makai' and regiao_in=='Aracaju' and data_hora_in - data_hora_out < timedelta(hours=2):

                            df = identificar_alterar_nome_servico_conjugado(row, index, df)

    elif st.session_state.base_luck=='test_phoenix_maceio':

        df_in_out_group = df_in_out_group[df_in_out_group['Tipo de Servico'].apply(filtro_tipo_servico_out_in)]

        for _, row in df_in_out_group.iterrows():

            lista_tipos_servicos = row['Tipo de Servico']

            for index in range(1, len(lista_tipos_servicos)):

                tipo_primeiro_trf, tipo_segundo_trf = coletar_dados_row(row, 'Tipo de Servico', index)

                if tipo_primeiro_trf=='OUT' and tipo_segundo_trf=='IN':

                    regiao_out, regiao_in = coletar_dados_row(row, 'Regiao', index)

                    data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', index, tipo_primeiro_trf)

                    if (regiao_out=='Barra de Santo Antonio' or regiao_out=='Paripueira' or regiao_out=='Jequia') and data_hora_in - data_hora_out < timedelta(hours=2, minutes=30):

                        df = identificar_alterar_nome_servico_conjugado(row, index, df)

                    elif regiao_out=='Barra de Sao Miguel' and data_hora_in - data_hora_out < timedelta(hours=2, minutes=50):

                        df = identificar_alterar_nome_servico_conjugado(row, index, df)

                    elif (regiao_out=='Grande Maceio' or regiao_out=='Orla Maceio' or regiao_out=='Frances') and data_hora_in - data_hora_out < timedelta(hours=2, minutes=10):

                        df = identificar_alterar_nome_servico_conjugado(row, index, df)

                    elif regiao_out=='Maragogi' and data_hora_in - data_hora_out < timedelta(hours=5):

                        df = identificar_alterar_nome_servico_conjugado(row, index, df)

                    elif regiao_out=='Milagres' and data_hora_in - data_hora_out < timedelta(hours=5, minutes=30):

                        df = identificar_alterar_nome_servico_conjugado(row, index, df)

    return df

def identificar_trf_htl_conjugados(df):

    df_in_out = df[df['Servico'].isin(st.session_state.dict_trf_hotel_conjugado)].reset_index()

    df_in_out['Ajuste'] = df_in_out['Servico'].map(st.session_state.dict_trf_hotel_conjugado)

    df_in_out = df_in_out.sort_values(by=['Data da Escala', 'Fornecedor Motorista', 'Ajuste']).reset_index(drop=True)

    df_in_out_group = df_in_out.groupby(['Fornecedor Motorista']).agg({'index': lambda x: list(x), 'Servico': lambda x: list(x), 'Data | Horario Apresentacao': lambda x: list(x), 
                                                                                         'Escala': 'count'}).reset_index()

    df_in_out_group = df_in_out_group[df_in_out_group['Escala']>1]

    if st.session_state.base_luck=='test_phoenix_recife':

        for _, row in df_in_out_group.iterrows():

            lista_servicos = row['Servico']

            for index in range(1, len(lista_servicos)):

                servico_1, servico_2 = coletar_dados_row(row, 'Servico', index)

                hora_1, hora_2 = coletar_dados_row(row, 'Data | Horario Apresentacao', index)

                if hora_2>hora_1:

                    intervalo_ref = hora_2 - hora_1

                else:

                    intervalo_ref = hora_1 - hora_2

                if ((servico_1=='TRF BOA VIAGEM OU PIEDADE / CABO DE STO AGOSTINHO OU PAIVA' or servico_1=='TRF PIEDADE / CABO DE STO AGOSTINHO OU PAIVA') and 
                    (servico_2=='TRF CABO DE STO AGOSTINHO/BOA VIAGEM OU PIEDADE' or servico_2=='TRF CABO DE STO AGOSTINHO/PIEDADE')) or \
                    ((servico_1=='TRF BOA VIAGEM OU PIEDADE / PORTO DE GALINHAS' or servico_1=='TRF PIEDADE / PORTO DE GALINHAS') and 
                    (servico_2=='TRF PORTO DE GALINHAS / BOA VIAGEM OU PIEDADE' or servico_2=='TRF PORTO DE GALINHAS / PIEDADE')) and (intervalo_ref <= timedelta(hours=3)):
                    
                    df = identificar_alterar_nome_servico_conjugado(row, index, df)

                elif (servico_1=='TRF BOA VIAGEM OU PIEDADE / CARNEIROS OU TAMANDARÉ' or servico_1=='TRF PIEDADE / CARNEIROS OU TAMANDARÉ') and \
                    (servico_2=='TRF CARNEIROS OU TAMANDARÉ / BOA VIAGEM OU PIEDADE' or servico_2=='TRF CARNEIROS OU TAMANDARÉ / PIEDADE') and (intervalo_ref <= timedelta(hours=4)):

                    df = identificar_alterar_nome_servico_conjugado(row, index, df)

                elif ((servico_1=='TRF BOA VIAGEM OU PIEDADE / MARAGOGI OU JAPARATINGA' or servico_1=='TRF PIEDADE / MARAGOGI OU JAPARATINGA') and \
                    (servico_2=='TRF MARAGOGI OU JAPARATINGA / BOA VIAGEM OU PIEDADE' or servico_2=='TRF MARAGOGI OU JAPARATINGA / PIEDADE')) or \
                        (servico_1=='TRF PORTO DE GALINHAS / MARAGOGI OU JAPARATINGA' and servico_2=='TRF MARAGOGI OU JAPARATINGA / PORTO DE GALINHAS') and (intervalo_ref <= timedelta(hours=5)):

                    df = identificar_alterar_nome_servico_conjugado(row, index, df)

    elif st.session_state.base_luck=='test_phoenix_natal':

        for _, row in df_in_out_group.iterrows():

            lista_servicos = row['Servico']

            for index in range(1, len(lista_servicos)):

                servico_1, servico_2 = coletar_dados_row(row, 'Servico', index)

                hora_1, hora_2 = coletar_dados_row(row, 'Data | Horario Apresentacao', index)

                if hora_2>hora_1:

                    intervalo_ref = hora_2 - hora_1

                else:

                    intervalo_ref = hora_1 - hora_2

                if (servico_1=='TRF  Pipa/Natal' and servico_2=='TRF Natal/Pipa ') or (servico_1=='TRF Natal/Touros' and servico_2=='TRF Touros/Natal') and \
                    (intervalo_ref <= timedelta(hours=3, minutes=30)):
                    
                    df = identificar_alterar_nome_servico_conjugado(row, index, df)

                elif servico_1=='TRF Natal/São Miguel' and servico_2=='TRF São Miguel/Natal' and intervalo_ref <= timedelta(hours=4):

                    df = identificar_alterar_nome_servico_conjugado(row, index, df)

    return df

def identificar_trf_in_htl_conjugados(df):
    
    df_in_out = df[(df['Servico'].isin(st.session_state.dict_trf_in_hotel_conjugado)) & (df['Serviço Conjugado']=='')].reset_index()

    df_in_out['Ajuste'] = df_in_out['Servico'].map(st.session_state.dict_trf_in_hotel_conjugado)

    df_in_out = df_in_out.sort_values(by=['Data da Escala', 'Fornecedor Motorista', 'Ajuste']).reset_index(drop=True)

    df_in_out_group = df_in_out.groupby(['Fornecedor Motorista']).agg({'index': lambda x: list(x), 'Servico': lambda x: list(x), 'Data | Horario Apresentacao': lambda x: list(x), 
                                                                                         'Escala': 'count'}).reset_index()
    
    df_in_out_group = df_in_out_group[df_in_out_group['Escala']>1]

    for _, row in df_in_out_group.iterrows():

        lista_servicos = row['Servico']

        for index in range(1, len(lista_servicos)):

            servico_1, servico_2 = coletar_dados_row(row, 'Servico', index)

            hora_1, hora_2 = coletar_dados_row(row, 'Data | Horario Apresentacao', index)

            if hora_2>hora_1:

                intervalo_ref = hora_2 - hora_1

            else:

                intervalo_ref = hora_1 - hora_2

            if (servico_1=='IN (CABO DE STO AGOSTINHO)' and (servico_2=='TRF CABO DE STO AGOSTINHO/BOA VIAGEM OU PIEDADE' or servico_2=='TRF CABO DE STO AGOSTINHO/PIEDADE' or 
                                                             servico_2=='TRF CABO STO AGOSTINHO OU PAIVA / RECIFE (CENTRO)')) \
                or (servico_1=='IN (SERRAMBI)' and (servico_2=='TRF SERRAMBI / BOA VIAGEM OU PIEDADE' or servico_2=='TRF SERRAMBI / PIEDADE' or servico_2=='TRF SERRAMBI / RECIFE (CENTRO)')) \
                    or (servico_1=='IN (PORTO DE GALINHAS)' and (servico_2=='TRF PORTO DE GALINHAS / BOA VIAGEM OU PIEDADE' or servico_2=='TRF PORTO DE GALINHAS / PIEDADE' or 
                                                                 servico_2=='TRF PORTO DE GALINHAS / RECIFE (CENTRO)')) and (intervalo_ref <= timedelta(hours=3)):
                
                df = identificar_alterar_nome_servico_conjugado(row, index, df)

            elif servico_1=='IN (MARAGOGI | JAPARATINGA)' and (servico_2=='TRF MARAGOGI OU JAPARATINGA / BOA VIAGEM OU PIEDADE' or servico_2=='TRF MARAGOGI OU JAPARATINGA / PIEDADE' or 
                                                               servico_2=='TRF MARAGOGI OU JAPARATINGA / RECIFE ') and (intervalo_ref <= timedelta(hours=5)):

                df = identificar_alterar_nome_servico_conjugado(row, index, df)

            elif servico_1=='IN (CARNEIROS I TAMANDARÉ)' and (servico_2=='TRF CARNEIROS OU TAMANDARÉ / BOA VIAGEM OU PIEDADE' or servico_2=='TRF CARNEIROS OU TAMANDARÉ / PIEDADE' or 
                                                              servico_2=='TRF CARNEIROS OU TAMANDARÉ / RECIFE (CENTRO)') and (intervalo_ref <= timedelta(hours=4)):

                df = identificar_alterar_nome_servico_conjugado(row, index, df)

            elif servico_1=='IN (OLINDA)' and servico_2=='TRF OLINDA/RECIFE' and intervalo_ref <= timedelta(hours=2):

                df = identificar_alterar_nome_servico_conjugado(row, index, df)

    return df

def identificar_trf_htl_out_conjugados(df):

    df_in_out = df[(df['Servico'].isin(st.session_state.dict_trf_hotel_out_conjugado)) & (df['Serviço Conjugado']=='')].reset_index()

    df_in_out['Ajuste'] = df_in_out['Servico'].map(st.session_state.dict_trf_hotel_out_conjugado)

    df_in_out = df_in_out.sort_values(by=['Data da Escala', 'Fornecedor Motorista', 'Ajuste']).reset_index(drop=True)

    df_in_out_group = df_in_out.groupby(['Fornecedor Motorista']).agg({'index': lambda x: list(x), 'Servico': lambda x: list(x), 'Data | Horario Apresentacao': lambda x: list(x), 
                                                                                         'Escala': 'count'}).reset_index()
    
    df_in_out_group = df_in_out_group[df_in_out_group['Escala']>1]

    for _, row in df_in_out_group.iterrows():

        lista_servicos = row['Servico']

        for index in range(1, len(lista_servicos)):

            servico_1, servico_2 = coletar_dados_row(row, 'Servico', index)

            hora_1, hora_2 = coletar_dados_row(row, 'Data | Horario Apresentacao', index)

            if hora_2>hora_1:

                intervalo_ref = hora_2 - hora_1

            else:

                intervalo_ref = hora_1 - hora_2

            if ((servico_1=='TRF BOA VIAGEM OU PIEDADE / CABO DE STO AGOSTINHO OU PAIVA' or servico_1=='TRF PIEDADE / CABO DE STO AGOSTINHO OU PAIVA') and servico_2=='OUT (CABO DE STO AGOSTINHO)') \
                    or (servico_1=='TRF PORTO DE GALINHAS / CABO DE STO AGOSTINHO OU PAIVA' and servico_2=='OUT (CABO DE STO AGOSTINHO)') \
                        or ((servico_1=='TRF BOA VIAGEM OU PIEDADE / PORTO DE GALINHAS' or servico_1=='TRF PIEDADE / PORTO DE GALINHAS') and servico_2=='OUT (PORTO DE GALINHAS)') \
                            or (servico_1=='TRF CABO DE STO AGOSTINHO OU PAIVA / PORTO DE GALINHAS' and servico_2=='OUT (PORTO DE GALINHAS)') and (intervalo_ref <= timedelta(hours=3)):
                
                df = identificar_alterar_nome_servico_conjugado(row, index, df)

            elif ((servico_1=='TRF BOA VIAGEM OU PIEDADE / MARAGOGI OU JAPARATINGA' or servico_1=='PIEDADE / MARAGOGI OU JAPARATINGA') and servico_2=='OUT (MARAGOGI | JAPARATINGA)') \
                    or (servico_1=='TRF PORTO DE GALINHAS / MARAGOGI OU JAPARATINGA' and servico_2=='OUT (MARAGOGI | JAPARATINGA)') \
                        or (servico_1=='TRF MARAGOGI OU JAPARATINGA / PORTO DE GALINHAS' and servico_2=='OUT (PORTO DE GALINHAS)') and (intervalo_ref <= timedelta(hours=5)):

                df = identificar_alterar_nome_servico_conjugado(row, index, df)

            elif ((servico_1=='TRF BOA VIAGEM OU PIEDADE / CARNEIROS OU TAMANDARÉ' or servico_1=='TRF PIEDADE / CARNEIROS OU TAMANDARÉ') and servico_2=='OUT (CARNEIROS I TAMANDARÉ)') \
                and (intervalo_ref <= timedelta(hours=4)):

                df = identificar_alterar_nome_servico_conjugado(row, index, df)

    return df

def gerar_coluna_valor_final_jpa(row):

    if row['Fornecedor Motorista']!='LUCENA CANOPUS' and f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}" in st.session_state.df_tarifario.columns:

        return row[f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}"]
    
    elif row['Fornecedor Motorista']!='LUCENA CANOPUS' and f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}" not in st.session_state.df_tarifario.columns:

        return None
    
    elif row['Fornecedor Motorista']=='LUCENA CANOPUS':

        return 50*(row['Total ADT'] + row['Total CHD'])
    
def gerar_coluna_valor_final_rec(row):

    if f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}" in st.session_state.df_tarifario.columns:

        return row[f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}"]
    
    else:

        return row[row['Tipo Veiculo']]

def verificar_tarifarios(df_escalas_group, id_gsheet, aba_gsheet, coluna_valores_none):

    lista_passeios_sem_tarifario = list(set(df_escalas_group['Servico'].unique().tolist()) - set(st.session_state.df_tarifario['Servico'].unique().tolist()))

    lista_passeios_tarifa_nula = df_escalas_group[pd.isna(df_escalas_group[coluna_valores_none])]['Servico'].unique().tolist()

    if len(lista_passeios_sem_tarifario)>0:

        df_itens_faltantes = pd.DataFrame(lista_passeios_sem_tarifario, columns=['Servico'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet(aba_gsheet)
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error('Os serviços acima não estão tarifados. Eles foram inseridos no final da planilha de tarifários. Por favor, tarife os serviços e tente novamente')

        st.stop()

    elif len(lista_passeios_tarifa_nula)>0:

        st.error(f"Os serviços {', '.join(lista_passeios_tarifa_nula)} estão na planilha de tarifários, mas não possuem valor correspondente. Por favor, tarife os serviços e tente novamente")

        df_escalas_group[coluna_valores_none] = df_escalas_group[coluna_valores_none].fillna('Sem Tarifa!')

        st.dataframe(df_escalas_group[df_escalas_group[coluna_valores_none]=='Sem Tarifa!'][['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Fornecedor Motorista', 'Servico', coluna_valores_none]], 
                     hide_index=True)

        st.stop()

def precificar_flor_da_trilha(df_escalas_group_bg_4x4):

    valor_flor_da_trilha = st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor FLOR DA TRILHA']['Valor Parâmetro'].iloc[0]

    df_escalas_group_bg_4x4.loc[df_escalas_group_bg_4x4['Veiculo']=='FLOR DA TRILHA', ['Valor Venda', 'Desconto Reserva', 'Venda Líquida de Desconto']] = \
        [valor_flor_da_trilha, 0, valor_flor_da_trilha]
    
    return df_escalas_group_bg_4x4

def escolher_net_venda_liquida(row):

    if row['Venda Líquida de Desconto']>row['Valor Net']:

        return row['Valor Net']
    
    else:

        return row['Venda Líquida de Desconto']
    
def gerar_df_pag_final_forn_bg_4x4(df_escalas_group_bg_4x4):
    
    st.session_state.df_pag_final_forn_bg_4x4 = df_escalas_group_bg_4x4[['Data da Escala', 'Reserva', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Valor Venda', 
                                                                         'Desconto Reserva', 'Venda Líquida de Desconto', 'Valor Net', 'Valor Final']]
    
    st.session_state.df_pag_final_forn_bg_4x4['Data da Escala'] = pd.to_datetime(st.session_state.df_pag_final_forn_bg_4x4['Data da Escala']).dt.strftime('%d/%m/%Y')

    st.session_state.df_pag_final_forn_bg_4x4 = st.session_state.df_pag_final_forn_bg_4x4.drop_duplicates().reset_index(drop=True)

def gerar_df_insercao_mapa_pagamento(data_inicial, data_final, df_pag_final, aba_gsheet):

    puxar_aba_simples(st.session_state.id_gsheet, aba_gsheet, 'df_historico_pagamentos')

    st.session_state.df_historico_pagamentos['Data da Escala'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data da Escala'], format='%d/%m/%Y').dt.date

    df_historico_fora_do_periodo = st.session_state.df_historico_pagamentos[~((st.session_state.df_historico_pagamentos['Data da Escala'] >= data_inicial) & 
                                                                                (st.session_state.df_historico_pagamentos['Data da Escala'] <= data_final))].reset_index(drop=True)
    
    df_insercao = pd.concat([df_historico_fora_do_periodo, df_pag_final], ignore_index=True)

    df_insercao['Data da Escala'] = df_insercao['Data da Escala'].astype(str)

    return df_insercao

def inserir_dataframe_gsheet(df_itens_faltantes, id_gsheet, nome_aba):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z100000"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

def selecionar_fornecedor_do_mapa(row2, df_pag_final):

    with row2[0]:

        if st.session_state.base_luck=='test_phoenix_joao_pessoa' and 'POLO' in df_pag_final['Veiculo'].unique():

            lista_fornecedores = ['POLO']

            lista_fornecedores.extend(sorted(df_pag_final['Fornecedor Motorista'].dropna().unique().tolist()))

            fornecedor = st.selectbox('Fornecedor', lista_fornecedores, index=None)

        elif st.session_state.base_luck=='test_phoenix_noronha':

            lista_fornecedores = df_pag_final['Servico'].dropna().unique().tolist()

            fornecedor = st.selectbox('Serviço', sorted(lista_fornecedores), index=None)

        else:

            lista_fornecedores = df_pag_final['Fornecedor Motorista'].dropna().unique().tolist()

            fornecedor = st.selectbox('Fornecedor', sorted(lista_fornecedores), index=None)

    return fornecedor, lista_fornecedores

def identificar_cnpj_razao_social(fornecedor):

    if st.session_state.base_luck=='test_phoenix_noronha':

        st.session_state.cnpj = ''

        st.session_state.razao_social = ''
        
    else:

        st.session_state.cnpj = st.session_state.df_veiculos[st.session_state.df_veiculos['Fornecedor Motorista']==fornecedor]['cnpj_cpf'].iloc[0]

        st.session_state.razao_social = st.session_state.df_veiculos[st.session_state.df_veiculos['Fornecedor Motorista']==fornecedor]['razao_social_nome'].iloc[0]

def plotar_mapa_pagamento(fornecedor, row2_1, df_pag_final):

    if st.session_state.base_luck=='test_phoenix_noronha':

        df_pag_fornecedor = df_pag_final[df_pag_final['Servico'].isin(fornecedor)].sort_values(by=['Data da Escala']).reset_index(drop=True)

    else:

        df_pag_fornecedor = df_pag_final[df_pag_final['Fornecedor Motorista']==fornecedor].sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

    if 'df_pag_final_forn_bg_4x4' in st.session_state and len(st.session_state.tipo_de_mapa)>0:

        df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala'], format='%d/%m/%Y').dt.strftime('%d/%m/%Y')

    else:

        df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

    container_dataframe = st.container()

    container_dataframe.dataframe(df_pag_fornecedor, hide_index=True, use_container_width = True)

    with row2_1[0]:

        total_a_pagar = df_pag_fornecedor['Valor Final'].sum()

        st.subheader(f'Valor Total: R${int(total_a_pagar)}')

    return total_a_pagar, df_pag_fornecedor

def definir_html(df_ref):

    html=df_ref.to_html(index=False, escape=False)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                text-align: center;  /* Centraliza o texto */
            }}
            table {{
                margin: 0 auto;  /* Centraliza a tabela */
                border-collapse: collapse;  /* Remove espaço entre as bordas da tabela */
            }}
            th, td {{
                padding: 8px;  /* Adiciona espaço ao redor do texto nas células */
                border: 1px solid black;  /* Adiciona bordas às células */
                text-align: center;
            }}
        </style>
    </head>
    <body>
        {html}
    </body>
    </html>
    """

    return html

def criar_output_html(nome_html, html, fornecedor, soma_servicos):

    if st.session_state.base_luck=='test_phoenix_noronha':

        with open(nome_html, "w", encoding="utf-8") as file:

            file.write(f'<p style="font-size:40px;">{fornecedor}</p>')

            file.write(f'<p style="font-size:30px;">Serviços prestados entre {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}</p>')

            file.write(html)

            file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

            file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

    else:

        with open(nome_html, "w", encoding="utf-8") as file:

            file.write(f'<p style="font-size:40px;">{fornecedor}</p>')

            file.write(f'<p style="font-size:30px;">Serviços prestados entre {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}</p>')

            file.write(f'<p style="font-size:30px;">CPF / CNPJ: {st.session_state.cnpj}</p>')

            file.write(f'<p style="font-size:30px;">Razão Social / Nome Completo: {st.session_state.razao_social}</p><br><br>')

            file.write(html)

            file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

            file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

def botao_download_html_individual(total_a_pagar, df_pag_fornecedor, fornecedor, colunas_valores_df_pag):

    soma_servicos = format_currency(total_a_pagar, 'BRL', locale='pt_BR')

    for item in colunas_valores_df_pag:

        df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

    if st.session_state.base_luck=='test_phoenix_noronha':

        for item in st.session_state.colunas_numeros_inteiros_df_pag_forn:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].astype(int)

    html = definir_html(df_pag_fornecedor)

    nome_html = f'{fornecedor}.html'

    criar_output_html(nome_html, html, fornecedor, soma_servicos)

    with open(nome_html, "r", encoding="utf-8") as file:

        html_content = file.read()

    with row2_1[1]:

        st.download_button(
            label="Baixar Arquivo HTML",
            data=html_content,
            file_name=nome_html,
            mime="text/html"
        )

    st.session_state.html_content = html_content

def gerar_payload_envio_geral(lista_fornecedores, df_pag_final, colunas_valores_df_pag):

    lista_htmls = []

    lista_htmls_email = []

    lista_fornecedores_sem_contato = []

    lista_fornecedores_contato_nulo = []

    for fornecedor_ref in lista_fornecedores:

        if 'CARRO' in fornecedor_ref and st.session_state.base_luck=='test_phoenix_noronha':

            fornecedor_ref = fornecedor_ref.split(' - ')[0]

        if fornecedor_ref in st.session_state.df_contatos['Fornecedores'].unique().tolist():

            contato_fornecedor = st.session_state.df_contatos.loc[st.session_state.df_contatos['Fornecedores']==fornecedor_ref, 'Contato'].values[0]

            if contato_fornecedor=='':

                lista_fornecedores_contato_nulo.append(fornecedor_ref)

        else:

            lista_fornecedores_sem_contato.append(fornecedor_ref)

        identificar_cnpj_razao_social(fornecedor_ref)

        if st.session_state.base_luck=='test_phoenix_noronha':

            df_pag_fornecedor = df_pag_final[df_pag_final['Servico']==fornecedor_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        else:

            df_pag_fornecedor = df_pag_final[df_pag_final['Fornecedor Motorista']==fornecedor_ref].sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

        if 'df_pag_final_forn_bg_4x4' in st.session_state and len(st.session_state.tipo_de_mapa)>0:

            df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala'], format='%d/%m/%Y').dt.strftime('%d/%m/%Y')

        else:

            df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_fornecedor['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in colunas_valores_df_pag:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        if st.session_state.base_luck=='test_phoenix_noronha':

            for item in st.session_state.colunas_numeros_inteiros_df_pag_forn:

                df_pag_fornecedor[item] = df_pag_fornecedor[item].astype(int)

        html = definir_html(df_pag_fornecedor)

        nome_html = f'{fornecedor_ref}.html'

        criar_output_html(nome_html, html, fornecedor_ref, soma_servicos)

        with open(nome_html, "r", encoding="utf-8") as file:

            html_content_fornecedor_ref = file.read()

        if '@' in contato_fornecedor:

            lista_htmls_email.append([html_content_fornecedor_ref, contato_fornecedor])

        else:

            lista_htmls.append([html_content_fornecedor_ref, contato_fornecedor])

    return lista_htmls, lista_htmls_email, lista_fornecedores_sem_contato, lista_fornecedores_contato_nulo

def verificar_fornecedor_sem_contato(lista_fornecedores_sem_contato, id_gsheet, aba_gsheet):

    if len(lista_fornecedores_sem_contato)>0:

        df_itens_faltantes = pd.DataFrame(lista_fornecedores_sem_contato, columns=['Fornecedores'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet(aba_gsheet)
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error('Os fornecedores acima não estão na lista dos contatos. Por favor, cadastre o contato deles e tente novamente.')

        st.stop()

def verificar_fornecedor_contato_nulo(lista_fornecedores_contato_nulo):

    if len(lista_fornecedores_contato_nulo)>0:

        st.error(f"Os fornecedores {', '.join(lista_fornecedores_contato_nulo)} estão na planilha de contatos, mas estão com o contato vazio. Preencha e tente novamente")

        st.stop()

def enviar_informes_gerais(lista_htmls):

    payload = {"informe_html": lista_htmls}
    
    response = requests.post(st.session_state.id_webhook, json=payload)
        
    if response.status_code == 200:
        
        st.success(f"Mapas de Pagamentos enviados pelo Whatsapp com sucesso!")
        
    else:
        
        st.error(f"Erro. Favor contactar o suporte")

        st.error(f"{response}")

def enviar_email_gmail(destinatarios, assunto, conteudo_html, remetente, senha):
    try:
        # Configurações do servidor SMTP
        servidor = smtplib.SMTP('smtp.gmail.com', 587)
        servidor.starttls()
        servidor.login(remetente, senha)

        # Criação do e-mail
        email = MIMEMultipart()
        email['From'] = remetente
        email['To'] = ', '.join(destinatarios)
        email['Subject'] = assunto

        email.attach(MIMEText(conteudo_html, 'html'))

        # Envio do e-mail
        servidor.send_message(email)
        servidor.quit()
        st.success("E-mail enviado com sucesso!")
    except Exception as e:
        st.error(f"Erro ao enviar e-mail: {e}")

def enviar_emails_gerais(lista_htmls_email):

    assunto = f'Mapa de Pagamento {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}'

    for lista_html_contato in lista_htmls_email:

        enviar_email_gmail([lista_html_contato[1]], assunto, lista_html_contato[0], st.session_state.remetente_email, st.session_state.senha_email)

def inserir_html(nome_html, html, fornecedor, soma_servicos):

    with open(nome_html, "a", encoding="utf-8") as file:

        file.write('<div style="page-break-before: always;"></div>\n')

        file.write(f'<p style="font-size:40px;">{fornecedor}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:40px;">O valor total dos serviços é {soma_servicos}</p>')

def gerar_html_mapa_fornecedores_geral(lista_fornecedores, df_pag_final, colunas_valores_df_pag):

    for fornecedor_ref in lista_fornecedores:

        if st.session_state.base_luck=='test_phoenix_noronha':

            df_pag_fornecedor = df_pag_final[df_pag_final['Servico']==fornecedor_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        else:

            df_pag_fornecedor = df_pag_final[df_pag_final['Fornecedor Motorista']==fornecedor_ref].sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

        if 'df_pag_final_forn_bg_4x4' in st.session_state and len(st.session_state.tipo_de_mapa)>0:

            df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala'], format='%d/%m/%Y').dt.strftime('%d/%m/%Y')

        else:

            df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_fornecedor['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in colunas_valores_df_pag:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR') if pd.notna(x) else x)

        if st.session_state.base_luck=='test_phoenix_noronha':

            for item in st.session_state.colunas_numeros_inteiros_df_pag_forn:

                df_pag_fornecedor[item] = df_pag_fornecedor[item].astype(int)

        html = definir_html(df_pag_fornecedor)

        inserir_html(nome_html, html, fornecedor_ref, soma_servicos)

def botao_download_html_geral(nome_html, row2_1):

    with open(nome_html, "r", encoding="utf-8") as file:

        html_content = file.read()

    with row2_1[1]:

        st.download_button(
            label="Baixar Arquivo HTML - Geral",
            data=html_content,
            file_name=nome_html,
            mime="text/html"
        )

def gerar_payload_envio_geral_para_financeiro(lista_fornecedores, df_pag_final, colunas_valores_df_pag):

    lista_htmls = []

    lista_htmls_email = []

    contato_financeiro = st.session_state.df_config[st.session_state.df_config['Configuração']=='Contato Financeiro']['Parâmetro'].iloc[0]

    for fornecedor_ref in lista_fornecedores:

        identificar_cnpj_razao_social(fornecedor_ref)

        if st.session_state.base_luck=='test_phoenix_noronha':

            df_pag_fornecedor = df_pag_final[df_pag_final['Servico']==fornecedor_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        else:

            df_pag_fornecedor = df_pag_final[df_pag_final['Fornecedor Motorista']==fornecedor_ref].sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

        if 'df_pag_final_forn_bg_4x4' in st.session_state and len(st.session_state.tipo_de_mapa)>0:

            df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala'], format='%d/%m/%Y').dt.strftime('%d/%m/%Y')

        else:

            df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_fornecedor['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in colunas_valores_df_pag:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        if st.session_state.base_luck=='test_phoenix_noronha':

            for item in st.session_state.colunas_numeros_inteiros_df_pag_forn:

                df_pag_fornecedor[item] = df_pag_fornecedor[item].astype(int)

        html = definir_html(df_pag_fornecedor)

        nome_html = f'{fornecedor_ref}.html'

        criar_output_html(nome_html, html, fornecedor_ref, soma_servicos)

        with open(nome_html, "r", encoding="utf-8") as file:

            html_content_fornecedor_ref = file.read()

        if '@' in contato_financeiro:

            lista_htmls_email.append([html_content_fornecedor_ref, contato_financeiro])

        else:

            lista_htmls.append([html_content_fornecedor_ref, contato_financeiro])

    return lista_htmls, lista_htmls_email

def calcular_e_ajustar_venda_liquida_valor_final():

    st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Venda Líquida de Desconto'] = \
        (st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Venda']-\
        st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Desconto Reserva'])*0.7
    
    if st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Venda Líquida de Desconto']>\
        st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Net']:

        st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Final'] = \
            st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Net']
        
    else:

        st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Final'] = \
            st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Venda Líquida de Desconto']

def gerar_listas_fornecedores_sem_contato(fornecedor):

    lista_fornecedores_sem_contato = []

    lista_fornecedores_contato_nulo = []

    if fornecedor in st.session_state.df_contatos['Fornecedores'].unique().tolist():

        contato_fornecedor = st.session_state.df_contatos.loc[st.session_state.df_contatos['Fornecedores']==fornecedor, 'Contato'].values[0]

        if contato_fornecedor=='':

            lista_fornecedores_contato_nulo.append(fornecedor)

    else:

        lista_fornecedores_sem_contato.append(fornecedor)

    return lista_fornecedores_contato_nulo, lista_fornecedores_sem_contato, contato_fornecedor

def enviar_informes_individuais(contato_fornecedor):
        
    payload = {"informe_html": st.session_state.html_content, 
                "telefone": contato_fornecedor}
    
    response = requests.post(st.session_state.id_webhook, json=payload)
        
    if response.status_code == 200:
        
        st.success(f"Mapas de Pagamento enviados pelo Whatsapp com sucesso!")
        
    else:
        
        st.error(f"Erro. Favor contactar o suporte")

        st.error(f"{response}")   

def enviar_email_individual(contato_fornecedor):

    assunto = f'Mapa de Pagamento {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}'

    enviar_email_gmail([contato_fornecedor], assunto, st.session_state.html_content, st.session_state.remetente_email, st.session_state.senha_email)

def identificando_in_out_transfer_piedade(df):

    df_escalas_bv_pd = df[(df['Tipo de Servico'].isin(['IN', 'OUT', 'TRANSFER'])) & (df['Servico'].str.contains('BOA VIAGEM|PIEDADE'))].reset_index()

    lista_hoteis_piedade = st.session_state.df_hoteis_piedade['Est Origem'].unique().tolist()

    for _, row in df_escalas_bv_pd.iterrows():

        lista_hoteis_escala = row['Estabelecimento Destino'].split(', ')

        lista_hoteis_escala.extend(row['Estabelecimento Origem'].split(', '))

        lista_hoteis_escala = [item for item in lista_hoteis_escala if not 'AEROPORTO' in item]

        if any(item in lista_hoteis_piedade for item in lista_hoteis_escala):

            df.loc[df['Escala'] == row['Escala'], 'Servico'] = (df.loc[df['Escala'] == row['Escala'], 'Servico'].str.replace(r'\(BOA VIAGEM \| PIEDADE\)', '(PIEDADE)', regex=True)
                                                                .str.replace(r'BOA VIAGEM OU PIEDADE', 'PIEDADE', regex=True))
            
    return df

def verificar_usuarios_cadastrados_errado(df):

    lista_usuarios_cadastro_errado = sorted(df[(df['Fornecedor Motorista']=='LUCK RECEPTIVO - REC')]['Motorista'].unique())

    if len(lista_usuarios_cadastro_errado)>0:

        st.error(f"Os usuários {', '.join(lista_usuarios_cadastro_errado)} estão vinculados ao fornecedor LUCK RECEPTIVO - REC, precisa ajustar no phoenix e puxar os dados novamente aqui no painel")

def ajustar_nome_fornecedor(nome_fornecedor):

    for chave, nome in st.session_state.dict_nomes_fornecedores_ajuste.items():

        if chave in nome_fornecedor:

            return nome
    return nome_fornecedor

def precificar_apoios_2_em_1(df):
    
    df_apoios = df[df['Servico']=='APOIO'].reset_index()

    df_apoios_group = df_apoios.groupby(['Data da Escala', 'Veiculo']).agg({'Escala': lambda x: list(x), 'Servico': 'count'}).reset_index()

    df_apoios_group = df_apoios_group[df_apoios_group['Servico']>1]

    for _, row in df_apoios_group.iterrows():

        lista_escalas_zerar_valor = row['Escala'][1:]

        df.loc[df['Escala'].isin(lista_escalas_zerar_valor), 'Valor Final']=0

    return df

def ajustar_valor_litoral_sul_4x4(df):

    mask_litoral_sul_regular = (df['Servico']=='Passeio Litoral Sul de 4x4') & (df['Modo']=='REGULAR')

    df.loc[mask_litoral_sul_regular, 'Valor Final'] = (df.loc[mask_litoral_sul_regular, 'Total ADT'] + df.loc[mask_litoral_sul_regular, 'Total CHD']) * \
        st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Litoral Sul 4x4 por Pax']['Valor Parâmetro'].iloc[0]

    return df

def ajustar_valor_luiz_damiao_pipa(df):

    mask_apoio_luiz_damiao = (df['Fornecedor Motorista'].isin(['DAMIAO PIPA', 'LUIZ ANTONIO'])) & (df['Servico']=='APOIO')

    df.loc[mask_apoio_luiz_damiao, 'Valor Final'] = ((df.loc[mask_apoio_luiz_damiao, 'Total ADT'] + df.loc[mask_apoio_luiz_damiao, 'Total CHD'] + 3) // 4) * \
        st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Apoio Damião e Luiz Pipa']['Valor Parâmetro'].iloc[0]

    return df

def ajustar_apoios_bolero_pipa(df): 

    mask_apoio_bolero_cunhau = (df['Servico']=='APOIO') & (df['Servico Principal'].isin(st.session_state.lista_passeios_apoio_bolero_cunhau))

    df.loc[mask_apoio_bolero_cunhau, 'Valor Final'] = st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Apoio Bolero e Cunhaú']['Valor Parâmetro'].iloc[0]

    return df

def excluir_escalas_duplicadas(df, lista_servicos):

    df_data_fornecedor = df[df['Servico'].isin(lista_servicos)][['Data da Escala', 'Fornecedor Motorista', 'Veiculo', 'Servico', 'Escala']].drop_duplicates().reset_index(drop=True)

    df_data_fornecedor = df_data_fornecedor.groupby(['Data da Escala', 'Fornecedor Motorista', 'Veiculo']).agg({'Escala': lambda x: list(x), 'Servico': 'count'}).reset_index()

    df_data_fornecedor = df_data_fornecedor[df_data_fornecedor['Servico']>1]

    excluir_escalas = []

    for lista_escalas in df_data_fornecedor['Escala']:

        excluir_escalas.extend(lista_escalas[1:])

    df = df[~df['Escala'].isin(excluir_escalas)].reset_index(drop=True)

    return df

def identificar_no_show_in(df_escalas):

    df_escalas['Observacao'] = df_escalas['Observacao'].fillna('')

    df_escalas['Observacao'] = df_escalas['Observacao'].apply(lambda x: 'NO SHOW' if 'NO SHOW' in x or 'NO-SHOW' in x else '')

    df_escalas.loc[df_escalas['Tipo de Servico']!='IN', 'Observacao'] = ''

    return df_escalas

def gerar_df_pag_tpp():

    # Filtrando período solicitado pelo usuário

    df_escalas_tarif_por_pax = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= st.session_state.data_inicial) & 
                                                           (st.session_state.df_escalas['Data da Escala'] <= st.session_state.data_final) & 
                                                           (st.session_state.df_escalas['Servico'].isin(st.session_state.lista_servicos_tarifarios_por_pax))].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group = df_escalas_tarif_por_pax.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    # Colocando valores tarifarios
        
    df_escalas_pag_tpp = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

    # Calculando Valor Final

    df_escalas_pag_tpp['Valor Final'] = (df_escalas_pag_tpp['Total ADT'] * df_escalas_pag_tpp['Valor ADT']) + (df_escalas_pag_tpp['Total CHD'] * df_escalas_pag_tpp['Valor CHD'])

    return df_escalas_pag_tpp

def gerar_df_pag_entardecer():

    df_escalas_entardecer = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= st.session_state.data_inicial) & 
                                                        (st.session_state.df_escalas['Data da Escala'] <= st.session_state.data_final) & 
                                                        (st.session_state.df_escalas['Servico'].isin(['ENTARDECER']))].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group = df_escalas_entardecer.groupby(['Data da Escala', 'Escala', 'Servico', 'adicional']).agg({'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    # Colocando valores tarifarios
        
    df_escalas_pag_ent = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

    # Identificando MARINA

    df_escalas_pag_ent['Servico'] = df_escalas_pag_ent.apply(lambda row: f"{row['Servico']} - MARINA" if row['adicional']=='ENTARDECER (MARINA SERVICOS NAUTICOS LTDA)' else row['Servico'], axis=1)

    df_escalas_pag_ent = df_escalas_pag_ent.drop(columns=['adicional'])

    # Agrupando de novo pra tirar repetidos e somar ADT e CHD

    df_escalas_pag_ent = df_escalas_pag_ent.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'Total ADT': 'sum', 'Total CHD': 'sum', 'Valor ADT': 'first', 'Valor CHD': 'first'}).reset_index()

    # Calculando Valor Final    

    df_escalas_pag_ent['Valor Final'] = (df_escalas_pag_ent['Total ADT'] * df_escalas_pag_ent['Valor ADT']) + (df_escalas_pag_ent['Total CHD'] * df_escalas_pag_ent['Valor CHD'])

    df_escalas_pag_ent.loc[df_escalas_pag_ent['Servico']=='ENTARDECER - MARINA', 'Valor Final'] = \
        st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Entardecer - Marina']['Valor Parâmetro'].iloc[0]

    return df_escalas_pag_ent

def gerar_df_pag_barco():

    df_escalas_barco = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= st.session_state.data_inicial) & 
                                                   (st.session_state.df_escalas['Data da Escala'] <= st.session_state.data_final) & 
                                                   (st.session_state.df_escalas['Servico'].isin(st.session_state.lista_servicos_barcos))].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group = df_escalas_barco.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'Data | Horario Apresentacao': 'max'}).reset_index()

    df_escalas_group['Horário'] = df_escalas_group['Data | Horario Apresentacao'].dt.time

    valor_diurno = st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Barco Diurno']['Valor Parâmetro'].iloc[0]

    valor_vespertino = st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Barco Vespertino']['Valor Parâmetro'].iloc[0]

    df_escalas_group['Valor Final'] = df_escalas_group['Data | Horario Apresentacao'].apply(lambda x: valor_diurno if x.time()<=time(12) else valor_vespertino)

    df_escalas_group = df_escalas_group.drop(columns=['Data | Horario Apresentacao'])

    return df_escalas_group

def renomear_servicos_lanchas(row):

    lista_palavras_chave_lanchas = st.session_state.df_config[st.session_state.df_config['Configuração']=='Palavra Chave Lanchas']['Parâmetro'].tolist()

    for item in lista_palavras_chave_lanchas:

        if item in row['adicional'] and 'LANCHA' in item:

            return f"{row['Servico']} - {item.upper()}"
        
        elif item in row['adicional']:

            return f"{row['Servico']} - LANCHA {item.upper()}"
        
        else:

            return f"{row['Servico']} - LANCHA FORNECEDOR NÃO IDENTIFICADO"

def gerar_df_pag_lancha():

    df_escalas_lancha = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= st.session_state.data_inicial) & 
                                                    (st.session_state.df_escalas['Data da Escala'] <= st.session_state.data_final) & 
                                                    (st.session_state.df_escalas['Servico'].isin(['LANCHA PRIVATIVA']))].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group = df_escalas_lancha.groupby(['Data da Escala', 'Escala', 'Servico', 'adicional']).agg({'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    df_escalas_group['Servico'] = df_escalas_group.apply(renomear_servicos_lanchas, axis=1)

    df_escalas_group['Qtd. Pax'] = df_escalas_group[['Total ADT', 'Total CHD']].sum(axis=1)

    # Colocando valores tarifarios

    df_escalas_pag_lancha = pd.merge(df_escalas_group, st.session_state.df_tarifario_lanchas, on=['Servico', 'Qtd. Pax'], how='left')

    df_escalas_pag_lancha = df_escalas_pag_lancha.drop(columns=['adicional', 'Qtd. Pax'])

    lista_escalas = st.session_state.df_tarifario_esp_lanchas['Escala'].unique().tolist()

    for escala in lista_escalas:

        df_escalas_pag_lancha.loc[df_escalas_pag_lancha['Escala']==escala, 'Valor Final'] = \
            st.session_state.df_tarifario_esp_lanchas.loc[st.session_state.df_tarifario_esp_lanchas['Escala']==escala, 'Valor Final'].values[0]

    return df_escalas_pag_lancha

def gerar_df_pag_escalas_geral():

    df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= st.session_state.data_inicial) & 
                                             (st.session_state.df_escalas['Data da Escala'] <= st.session_state.data_final) & 
                                             (~st.session_state.df_escalas['Veiculo'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Veículos']['Parâmetro']))]\
                                                .reset_index(drop=True)


    df_escalas['Valor Final'] = pd.to_numeric(st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Diária']['Valor Parâmetro'].iloc[0])

    df_escalas = df_escalas.groupby(['Data da Escala', 'Veiculo']).agg({'Escala': transformar_em_string, 'Servico': transformar_em_string, 'Valor Final': 'first'}).reset_index()

    df_escalas['Servico'] = df_escalas['Veiculo'] + ' - ' + df_escalas['Servico']

    df_escalas = df_escalas[['Data da Escala', 'Escala', 'Servico', 'Valor Final']]

    return df_escalas

def tratar_tipos_veiculos_spin_cobalt_passeio(df):

    for item in ['Spin', 'Cobalt', 'Passeio']:

        df.loc[df['Veiculo'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']==f'Tipo Veículo {item}']['Parâmetro']), 'Tipo Veiculo'] = item

    return df

def excluir_escalas_out_in_frances_sao_miguel(df, lista_servicos):

    df_data_escala = df[df['Servico'].isin(lista_servicos)].sort_values(by=['Data | Horario Apresentacao']).reset_index()

    df_data_escala_group = df_data_escala.groupby(['Data da Escala', 'Veiculo', 'Motorista', 'Guia'])\
        .agg({'index': lambda x: list(x), 'Servico': lambda x: list(x), 'Data | Horario Apresentacao': lambda x: list(x), 'Escala': 'count'}).reset_index()
    
    df_data_escala_group = df_data_escala_group[df_data_escala_group['Escala']>1]

    excluir_index = []

    for _, row in df_data_escala_group.iterrows():

        lista_index = row['index']

        for index in range(1, len(lista_index)):

            servico_1, servico_2 = coletar_dados_row(row, 'Servico', index)

            hora_1, hora_2 = coletar_dados_row(row, 'Data | Horario Apresentacao', index)

            if servico_1!=servico_2 and hora_2-hora_1<=timedelta(hours=1, minutes=30):

                excluir_index.append(int(lista_index[index]))

    df = df.drop(index=excluir_index).reset_index(drop=True)
    
    return df

def identificar_reaproveitamentos(df):

    df_data_veiculo = df[(df['Tipo de Servico'].isin(['OUT', 'IN'])) & (~df['Fornecedor Motorista'].isin(['VALTUR', 'MACIEL']))].reset_index()

    df_data_veiculo['n_servicos'] = df_data_veiculo['Serviço Conjugado'].apply(lambda x: 0.5 if x =='X' else 1)

    df_data_veiculo = df_data_veiculo.sort_values(by='n_servicos').reset_index(drop=True)

    df_data_veiculo = df_data_veiculo.groupby(['Data da Escala', 'Veiculo']).agg({'n_servicos': ['sum', lambda x: list(x)], 'index': lambda x: list(x)}).reset_index()

    df_data_veiculo.columns = ['Data da Escala', 'Veiculo', 'n_servicos', 'n_servicos_lista', 'index']

    df_data_veiculo = df_data_veiculo[df_data_veiculo['n_servicos']>1]

    for _, row in df_data_veiculo.iterrows():

        lista_n_servicos = row['n_servicos_lista']

        if 1 in lista_n_servicos:

            lista_index_reaproveitamento = row['index'][:len(row['index'])-1]

        else:

            lista_index_reaproveitamento = row['index'][:len(row['index'])-2]

        df.loc[lista_index_reaproveitamento, 'Reaproveitamento'] = 'X'

    return df

def excluir_apoios_duplicados(df):

    df_data_veiculo_apoio = df[df['Servico'].str.contains('APOIO')].reset_index()

    df_data_veiculo_apoio = df_data_veiculo_apoio.groupby(['Data da Escala', 'Veiculo', 'Motorista', 'Guia']).agg({'index': lambda x: list(x), 'Escala': 'count'}).reset_index()

    df_data_veiculo_apoio = df_data_veiculo_apoio[df_data_veiculo_apoio['Escala']>1]

    excluir_index = df_data_veiculo_apoio['index'].apply(lambda x: x[1:]).explode().tolist()

    df = df.drop(index=excluir_index).reset_index(drop=True)

    return df

def gerar_coluna_valor_final_mcz(row):

    if f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}" in st.session_state.df_tarifario.columns:

        if pd.notna(row['Valor No Show']):

            return row['Valor No Show']
        
        elif row['Reaproveitamento']=='X':

            return row[f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}"]*0.5
        
        else:

            return row[f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}"]
    
    else:

        return None

def soma_se_apoio_nao_none(x):

    return x[df_escalas['Apoio'].notna()].sum()

st.set_page_config(layout='wide')

if not 'df_escalas' in st.session_state or st.session_state.view_phoenix!='vw_pagamento_fornecedores':

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

st.title('Mapa de Pagamento - Fornecedores')

st.divider()

row1 = st.columns(2)

# Container de datas e botão de gerar mapa

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

    if st.session_state.base_luck=='test_phoenix_joao_pessoa':

        tipo_de_mapa = container_datas.multiselect('Gerar Mapas de Buggy, 4x4 e Polo', ['Sim'], default=None, key='tipo_de_mapa')

    else:

        st.session_state.tipo_de_mapa = []

    row1_2 = container_datas.columns(2)

    with row1_2[0]:

        gerar_mapa = st.button('Gerar Mapa de Pagamentos')

# Botão de atualizar dados do phoenix e data de pagamento

with row1[1]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    container_data_pgto = st.container(border=True)

    container_data_pgto.subheader('Data de Pagamento')

    data_pagamento = container_data_pgto.date_input('Data de Pagamento', value=None ,format='DD/MM/YYYY', key='data_pagamento')

    if not data_pagamento:

        st.warning('Preencha a data de pagamento para visualizar os mapas de pagamentos.')

    if atualizar_phoenix:

        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix()

st.divider()

# Geração de dataframe com os mapas de pagamentos

if gerar_mapa and data_inicial and data_final:

    # Base JPA

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        with st.spinner('Puxando configurações, tarifários...'):

            if len(st.session_state.tipo_de_mapa)==0:

                puxar_configuracoes()

                puxar_tarifario_fornecedores()

            else:

                puxar_configuracoes()

                puxar_tarifario_bg_4x4()

        with st.spinner('Gerando mapas de pagamentos...'):

            # Filtrando período solicitado pelo usuário e excluindo serviços e veículos especificados nas configurações

            df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                     (~st.session_state.df_escalas['Servico'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Serviços']\
                                                                                                   ['Parâmetro']))].reset_index(drop=True)
            
            # Transformando Data | Horario Apresentacao dos INs como Data | Horario Voo

            df_escalas['Data | Horario Apresentacao'] = df_escalas.apply(lambda row: pd.to_datetime(str(row['Data da Escala']) + ' ' + str(row['Horario Voo'])) 
                                                                         if row['Tipo de Servico']=='IN' and not pd.isna(row['Horario Voo']) else row['Data | Horario Apresentacao'], axis=1)

            # Agrupando escalas

            df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista'])\
                .agg({'Horario Voo': 'first', 'Data | Horario Apresentacao': 'max', 'Total ADT': 'sum', 'Total CHD': 'sum', 'Reserva': transformar_em_string}).reset_index()
            
            # Adicionando apoios no dataframe

            df_escalas_group = adicionar_apoios_em_dataframe(df_escalas, df_escalas_group)

            # Excluindo veículos da frota da análise

            df_escalas_group = df_escalas_group[~df_escalas_group['Veiculo'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Veículos']['Parâmetro'])]\
                .reset_index(drop=True)

            # Tratando nomes de tipos de veículos

            df_escalas_group['Tipo Veiculo'] = df_escalas_group['Tipo Veiculo'].replace(st.session_state.dict_tp_veic)

            # Identificando transfers conjugados

            df_escalas_group = identificar_trf_conjugados(df_escalas_group)

            if len(st.session_state.tipo_de_mapa)==0:

                # Colocando valores em escalas que não sejam com buggy, 4x4 ou Polo

                df_escalas_sem_buggy_4x4 = df_escalas_group[(~df_escalas_group['Tipo Veiculo'].isin(['Buggy', '4X4'])) & (df_escalas_group['Veiculo']!='POLO')].reset_index(drop=True)

                df_escalas_sem_buggy_4x4 = pd.merge(df_escalas_sem_buggy_4x4, st.session_state.df_tarifario, on='Servico', how='left')

                # Gerando coluna valor levando em conta o tipo de veículo usado e se é conjugado e se é CANOPUS

                df_escalas_sem_buggy_4x4['Valor Final'] = df_escalas_sem_buggy_4x4.apply(gerar_coluna_valor_final_jpa, axis=1)

                # Verificando se todos os serviços estão na lista de serviços do tarifário
                        
                verificar_tarifarios(df_escalas_sem_buggy_4x4, st.session_state.id_gsheet, 'Tarifário Fornecedores', 'Valor Final')

                st.session_state.df_pag_final_forn = df_escalas_sem_buggy_4x4[['Data da Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Serviço Conjugado', 
                                                                               'Valor Final']]
                
            else:

                # Colocando valores em escalas que sejam com buggy, 4x4 ou Polo

                df_escalas_buggy_4x4 = df_escalas_group[(df_escalas_group['Tipo Veiculo'].isin(['Buggy', '4X4'])) | (df_escalas_group['Veiculo']=='POLO')].reset_index(drop=True)

                # Inserindo valor do serviço e desconto

                df_escalas_group_bg_4x4 = pd.merge(df_escalas_buggy_4x4, st.session_state.df_sales, on=['Reserva', 'Servico'], how='left')

                # Inserindo valores net

                df_escalas_group_bg_4x4 = pd.merge(df_escalas_group_bg_4x4, st.session_state.df_tarifario, on='Servico', how='left')

                # Eliminando valores de desconto maiores que o valor da venda

                df_escalas_group_bg_4x4.loc[df_escalas_group_bg_4x4['Desconto Reserva']>df_escalas_group_bg_4x4['Valor Venda'], 'Desconto Reserva'] = 0

                # Calculando venda líquida de desconto * 70%

                df_escalas_group_bg_4x4['Venda Líquida de Desconto'] = (df_escalas_group_bg_4x4['Valor Venda']-df_escalas_group_bg_4x4['Desconto Reserva'])*0.7

                # Ajustando valor de flor das trilhas

                df_escalas_group_bg_4x4 = precificar_flor_da_trilha(df_escalas_group_bg_4x4)

                # Ajustando valor de polo

                df_escalas_group_bg_4x4.loc[df_escalas_group_bg_4x4['Veiculo']=='POLO', ['Valor Venda', 'Desconto Reserva', 'Venda Líquida de Desconto', 'Valor Net']] = [350, 0, 350, 350]

                # Escolhendo entre valor net e venda líquida de desconto p/ gerar valor de pagamento

                df_escalas_group_bg_4x4['Valor Final'] = df_escalas_group_bg_4x4.apply(escolher_net_venda_liquida, axis=1)

                # Verificando se todos os serviços estão na lista de serviços do tarifário
                        
                verificar_tarifarios(df_escalas_group_bg_4x4[df_escalas_group_bg_4x4['Veiculo']!='POLO'], st.session_state.id_gsheet, 'Tarifário Buggy e 4x4', 'Valor Net')

                gerar_df_pag_final_forn_bg_4x4(df_escalas_group_bg_4x4)

    # Base REC

    elif st.session_state.base_luck == 'test_phoenix_recife':

        # Puxando tarifários e tratando colunas de números
    
        with st.spinner('Puxando configurações, tarifários, hoteis piedade, pedágios, carroças...'):

            puxar_configuracoes()
        
            puxar_tarifario_fornecedores()
        
            puxar_aba_simples(st.session_state.id_gsheet_hoteis_piedade, 'Hoteis Piedade', 'df_hoteis_piedade')

            puxar_pedagios()

            puxar_aba_simples(st.session_state.id_gsheet, 'Controle Carroças', 'df_carrocas')

        with st.spinner('Gerando mapas de pagamentos...'):

            # Filtrando período solicitado pelo usuário
        
            df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)
            
            # Transformando Data | Horario Apresentacao dos INs como Data | Horario Voo
        
            df_escalas['Data | Horario Apresentacao'] = df_escalas.apply(lambda row: pd.to_datetime(str(row['Data da Escala']) + ' ' + str(row['Horario Voo'])) 
                                                                         if row['Tipo de Servico']=='IN' else row['Data | Horario Apresentacao'], axis=1)
            
            # Agrupando escalas
        
            df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista'])\
                .agg({'Horario Voo': 'first', 'Data | Horario Apresentacao': 'min', 'Estabelecimento Destino': transformar_em_string, 'Estabelecimento Origem': transformar_em_string}).reset_index()

            # Identificando IN, OUT e TRANSFER feito p/ piedade
    
            df_escalas_group = identificando_in_out_transfer_piedade(df_escalas_group)

            # Adicionando apoios no dataframe
    
            df_escalas_group = adicionar_apoios_em_dataframe(df_escalas, df_escalas_group)

            # Excluindo veículos da frota da análise

            df_escalas_group = df_escalas_group[~df_escalas_group['Veiculo'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Veículos']['Parâmetro'])]\
                .reset_index(drop=True)

            # Tratando nomes de tipos de veículos

            df_escalas_group['Tipo Veiculo'] = df_escalas_group['Tipo Veiculo'].replace(st.session_state.dict_tp_veic)

            # Aviso de usuários vinculados a LUCK RECEPTIVO - REC
    
            verificar_usuarios_cadastrados_errado(df_escalas_group)

            # Identificando transfers conjugados
    
            df_escalas_group = identificar_trf_conjugados(df_escalas_group)

            # Identificando transfers hotel > hotel conjugados
    
            df_escalas_group = identificar_trf_htl_conjugados(df_escalas_group)

            # Identificando transfers IN > hotel/hotel conjugados
    
            df_escalas_group = identificar_trf_in_htl_conjugados(df_escalas_group)

            # Identificando transfers hotel/hotel > OUT conjugados
    
            df_escalas_group = identificar_trf_htl_out_conjugados(df_escalas_group)

            # Retirando apoios de porto feito pela Luck REC
    
            df_escalas_group = df_escalas_group[df_escalas_group['Veiculo']!='VEICULO APOIO PORTO'].reset_index(drop=True)

            # Ajustando nomes Fornecedores Específicos

            df_escalas_group['Fornecedor Motorista'] = df_escalas_group['Fornecedor Motorista'].apply(ajustar_nome_fornecedor)

            # Colocando valores tarifarios
        
            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

            # Gerando valor final, verificando se existem colunas com tarifários específicos pra cada fornecedor

            df_escalas_group['Valor Final'] = df_escalas_group.apply(gerar_coluna_valor_final_rec, axis=1)

            # Inserir valor de pedágio

            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_pedagios, on='Escala', how='left')

            df_escalas_group['Valor Pedágio'] = df_escalas_group['Valor Pedágio'].fillna(0)

            # Inserir valor de carroças

            df_escalas_group['Valor Carroça'] = 0

            df_escalas_group.loc[df_escalas_group['Escala'].isin(st.session_state.df_carrocas['Escala'].unique()), 'Valor Carroça'] = \
                st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Carroça']['Valor Parâmetro'].iloc[0]
            
            # Somando Valor Final com Pedágio e Carroça
            
            df_escalas_group['Valor Final'] = df_escalas_group['Valor Final'] + df_escalas_group['Valor Pedágio'] + df_escalas_group['Valor Carroça']

            # Verificando se todos os serviços estão na lista de serviços do tarifário
                        
            verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Fornecedores', 'Valor Final')

            st.session_state.df_pag_final_forn = df_escalas_group[['Data da Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Motorista', 'Tipo Veiculo', 'Veiculo', 'Serviço Conjugado', 
                                                                   'Valor Pedágio', 'Valor Carroça', 'Valor Final']]
            
    # Base NAT

    elif st.session_state.base_luck == 'test_phoenix_natal':

        # Puxando tarifários e tratando colunas de números
    
        with st.spinner('Puxando configurações, tarifários...'):

            puxar_configuracoes()
        
            puxar_tarifario_fornecedores()

        with st.spinner('Gerando mapas de pagamentos...'):

            # Filtrando período solicitado pelo usuário
        
            df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)

            # Transformando Data | Horario Apresentacao dos INs como Data | Horario Voo
        
            df_escalas['Data | Horario Apresentacao'] = df_escalas.apply(lambda row: pd.to_datetime(str(row['Data da Escala']) + ' ' + str(row['Horario Voo'])) 
                                                                         if row['Tipo de Servico']=='IN' else row['Data | Horario Apresentacao'], axis=1)

            # Tratando nomes de serviços IN e OUT

            df_escalas['Servico'] = df_escalas['Servico'].replace(st.session_state.dict_tratar_servico_in_out)

            # Agrupando escalas
        
            df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista', 'Modo'])\
                .agg({'Horario Voo': 'first', 'Data | Horario Apresentacao': 'min', 'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()
            
            # Adicionando apoios no dataframe
    
            df_escalas_group = adicionar_apoios_em_dataframe(df_escalas, df_escalas_group)

            # Excluindo veículos da frota da análise

            df_escalas_group = df_escalas_group[~df_escalas_group['Veiculo'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Veículos']['Parâmetro'])]\
                .reset_index(drop=True)
            
            # Tratando nomes de tipos de veículos

            df_escalas_group['Tipo Veiculo'] = df_escalas_group['Tipo Veiculo'].replace(st.session_state.dict_tp_veic)

            # Identificando transfers conjugados

            df_escalas_group = identificar_trf_conjugados(df_escalas_group)

            # Identificando transfers hotel > hotel conjugados

            df_escalas_group = identificar_trf_htl_conjugados(df_escalas_group)

            # Colocando valores tarifarios
        
            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

            # Gerando valor final, verificando se existem colunas com tarifários específicos pra cada fornecedor

            df_escalas_group['Valor Final'] = df_escalas_group.apply(gerar_coluna_valor_final_rec, axis=1)

            # Precificando 2 apoios como 1 só ou mais de 2 apoios com 2

            df_escalas_group = precificar_apoios_2_em_1(df_escalas_group)

            # Ajustando valor de 4x4 Litoral Sul

            df_escalas_group = ajustar_valor_litoral_sul_4x4(df_escalas_group)

            # Ajustando valor de Damiao e Luiz Antonio nos apoios de pipa

            df_escalas_group = ajustar_valor_luiz_damiao_pipa(df_escalas_group)

            # Ajustando valor de apoio a João Pessoa com Bolero (Pipa) e Cunhaú

            df_escalas_group = ajustar_apoios_bolero_pipa(df_escalas_group)

            # Excluir escalas de passeios duplicadas (quando o nome do passeio não é igual)
            
            for conjunto_passeios in st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Duplicados']['Parâmetro']:

                lista_passeios_ref = conjunto_passeios.split(' & ')

                df_escalas_group = excluir_escalas_duplicadas(df_escalas_group, lista_passeios_ref)

            # Verificando se todos os serviços estão na lista de serviços do tarifário
                        
            verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Fornecedores', 'Valor Final')

            st.session_state.df_pag_final_forn = df_escalas_group[['Data da Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Serviço Conjugado', 'Valor Final']]

    # Base MCZ

    elif st.session_state.base_luck == 'test_phoenix_maceio':

        # Puxando tarifários e tratando colunas de números
    
        with st.spinner('Puxando configurações, tarifários, no shows...'):

            puxar_configuracoes()
        
            puxar_tarifario_fornecedores()

            puxar_controle_no_show()

        with st.spinner('Gerando mapas de pagamentos...'):

            # Filtrando período solicitado pelo usuário
        
            df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)

            # Transformando Data | Horario Apresentacao dos INs como Data | Horario Voo
        
            df_escalas['Data | Horario Apresentacao'] = df_escalas.apply(lambda row: pd.to_datetime(str(row['Data da Escala']) + ' ' + str(row['Horario Voo'])) 
                                                                         if row['Tipo de Servico']=='IN' else row['Data | Horario Apresentacao'], axis=1)
            
            # Ajustando nomes fornecedores

            df_escalas['Fornecedor Motorista'] = df_escalas['Fornecedor Motorista'].replace('WL', 'WL TURISMO')

            df_escalas['Fornecedor Motorista'] = df_escalas['Fornecedor Motorista'].replace('MARCIEL', 'MACIEL')

            # Agrupando escalas

            df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista'])\
                .agg({'Horario Voo': 'first', 'Data | Horario Apresentacao': 'max', 'Guia': transformar_em_string}).reset_index()

            # Adicionando apoios no dataframe
    
            df_escalas_group = adicionar_apoios_em_dataframe(df_escalas, df_escalas_group)

            # Excluindo veículos da frota da análise

            df_escalas_group = df_escalas_group[~df_escalas_group['Veiculo'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Veículos']['Parâmetro'])]\
                .reset_index(drop=True)
            
            # Tratando nomes de tipos de veículos

            df_escalas_group['Tipo Veiculo'] = df_escalas_group['Tipo Veiculo'].replace(st.session_state.dict_tp_veic)

            # Tratando tipos de veículos spin, cobalt e passeio

            df_escalas_group = tratar_tipos_veiculos_spin_cobalt_passeio(df_escalas_group)

            # Excluindo escalas repetidas por ser OUT/IN - FRANCES e OUT/IN - SAO MIGUEL

            for lista_servicos in st.session_state.lista_out_in_frances_sao_miguel:

                df_escalas_group = excluir_escalas_out_in_frances_sao_miguel(df_escalas_group, lista_servicos)

            # Identificando transfers conjugados

            df_escalas_group = identificar_trf_conjugados(df_escalas_group)

            # Identificando reaproveitamentos

            df_escalas_group = identificar_reaproveitamentos(df_escalas_group)

            # Excluindo apoios duplicados

            df_escalas_group = excluir_apoios_duplicados(df_escalas_group)

            # Colocando valores tarifarios
        
            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

            # Colocando valores no show
                
            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_no_show, on='Escala', how='left')

            # Gerando coluna valor levando em conta o tipo de veículo usado, se é conjugado, se foi reaproveitamento e se foi no show

            df_escalas_group['Valor Final'] = df_escalas_group.apply(gerar_coluna_valor_final_mcz, axis=1)

            # Ajustando coluna de Valor No Show para marcação de 'X'

            df_escalas_group['Valor No Show'] = df_escalas_group['Valor No Show'].apply(lambda x: '' if pd.isna(x) else 'X')

            df_escalas_group = df_escalas_group.rename(columns={'Valor No Show': 'No Show'})

            # Verificando se todos os serviços estão na lista de serviços do tarifário
                        
            verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Fornecedores', 'Valor Final')

            st.session_state.df_pag_final_forn = df_escalas_group[['Data da Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Serviço Conjugado', 
                                                                   'Reaproveitamento', 'No Show', 'Valor Final']]

    # Base SSA

    elif st.session_state.base_luck == 'test_phoenix_salvador':

        # Puxando tarifários e tratando colunas de números
    
        with st.spinner('Puxando configurações, tarifários...'):

            puxar_configuracoes()
        
            puxar_tarifario_fornecedores()

        with st.spinner('Gerando mapas de pagamentos...'):

            # Filtrando período solicitado pelo usuário
        
            df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)

            # Transformando Data | Horario Apresentacao dos INs como Data | Horario Voo
        
            df_escalas['Data | Horario Apresentacao'] = df_escalas.apply(lambda row: pd.to_datetime(str(row['Data da Escala']) + ' ' + str(row['Horario Voo'])) 
                                                                         if row['Tipo de Servico']=='IN' else row['Data | Horario Apresentacao'], axis=1)
            
            # Identificando serviços do tipo IN que são NO SHOW

            df_escalas = identificar_no_show_in(df_escalas)

            # Agrupando escalas
        
            df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista'])\
                .agg({'Horario Voo': 'first', 'Data | Horario Apresentacao': 'min', 'Observacao': lambda x: 'NO SHOW' if all(x=='NO SHOW') else ''}).reset_index()
            
            # Adicionando apoios no dataframe
    
            df_escalas_group = adicionar_apoios_em_dataframe(df_escalas, df_escalas_group)

            # Tratando nomes de tipos de veículos

            df_escalas_group['Tipo Veiculo'] = df_escalas_group['Tipo Veiculo'].replace(st.session_state.dict_tp_veic)

            # Identificar apoios pelo nome do veículo

            df_escalas_group['Servico'] = df_escalas_group.apply(lambda row: 'APOIO' if 'APOIO' in row['Veiculo'].upper() else row['Servico'], axis=1)

            # Identificando transfers conjugados

            df_escalas_group = identificar_trf_conjugados(df_escalas_group)

            # Gerando coluna com o valor da carretinha

            df_escalas_group['Carretinha'] = df_escalas_group['Veiculo'].apply(lambda x: 50 if 'C/C' in x else 0)

            # Colocando valores tarifarios
        
            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

            # Gerando valor final, verificando se existem colunas com tarifários específicos pra cada fornecedor

            df_escalas_group['Valor Final'] = df_escalas_group.apply(gerar_coluna_valor_final_rec, axis=1)

            # Considerando 50% do valor p/ TRF IN NO SHOW

            df_escalas_group = df_escalas_group.rename(columns={'Observacao': 'No Show'})

            df_escalas_group['Valor Final'] = df_escalas_group.apply(lambda row: row['Valor Final']*0.5 if row['No Show']=='NO SHOW' else row['Valor Final'], axis=1)

            # Somando valor de carretinha no valor final

            df_escalas_group['Valor Final'] = df_escalas_group['Valor Final'] + df_escalas_group['Carretinha']

            # Verificando se todos os serviços estão na lista de serviços do tarifário
                        
            verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Fornecedores', 'Valor Final')

            st.session_state.df_pag_final_forn = df_escalas_group[['Data da Escala', 'Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'No Show', 
                                                                   'Serviço Conjugado', 'Carretinha', 'Valor Final']]
            
    # Base FEN

    elif st.session_state.base_luck == 'test_phoenix_noronha':

        with st.spinner('Puxando configurações, tarifários...'):

            puxar_configuracoes()
        
            puxar_tarifario_fornecedores()

            puxar_tarifario_lanchas()

            puxar_tarifario_esp_lanchas()

        with st.spinner('Gerando mapas de pagamentos...'):

            # Gerando df_pag de serviços tarifados por pax

            df_escalas_pag_tpp = gerar_df_pag_tpp()

            # Gerando df_pag de Entardecer

            df_escalas_pag_ent = gerar_df_pag_entardecer()

            # Gerando df_pag de Barcos

            df_escalas_pag_barco = gerar_df_pag_barco()

            # Gerando df_pag de Lanchas

            df_escalas_pag_lancha = gerar_df_pag_lancha()

            # Gerando df_pag dos fornecedores que são veículos normais

            df_escalas_pag_normal = gerar_df_pag_escalas_geral()

            st.session_state.df_pag_final_forn = pd.concat([df_escalas_pag_tpp, df_escalas_pag_ent, df_escalas_pag_barco, df_escalas_pag_lancha, df_escalas_pag_normal], ignore_index=True)

            st.session_state.df_pag_final_forn = st.session_state.df_pag_final_forn[['Data da Escala', 'Escala', 'Servico', 'Horário', 'Total ADT', 'Total CHD', 'Valor ADT', 'Valor CHD', 'Valor Final']]

            for coluna in ['Total ADT', 'Total CHD', 'Valor ADT', 'Valor CHD']:

                st.session_state.df_pag_final_forn[coluna] = st.session_state.df_pag_final_forn[coluna].fillna(0)

    # Base AJU

    elif st.session_state.base_luck == 'test_phoenix_aracaju':

        with st.spinner('Puxando configurações, tarifários...'):

            puxar_configuracoes()
        
            puxar_tarifario_fornecedores()

        with st.spinner('Gerando mapas de pagamentos...'):

            # Filtrando período solicitado pelo usuário
        
            df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)

            # Transformando Data | Horario Apresentacao dos INs como Data | Horario Voo
        
            df_escalas['Data | Horario Apresentacao'] = df_escalas.apply(lambda row: pd.to_datetime(str(row['Data da Escala']) + ' ' + str(row['Horario Voo'])) 
                                                                         if row['Tipo de Servico']=='IN' else row['Data | Horario Apresentacao'], axis=1)
            
            # Agrupando escalas
        
            df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista'])\
                .agg({'Horario Voo': 'first', 'Data | Horario Apresentacao': 'min'}).reset_index()
            
            # Adicionando apoios no dataframe
    
            df_escalas_group = adicionar_apoios_em_dataframe(df_escalas, df_escalas_group)

            # Tratando nomes de tipos de veículos

            df_escalas_group['Tipo Veiculo'] = df_escalas_group['Tipo Veiculo'].replace(st.session_state.dict_tp_veic)

            # Identificando transfers conjugados

            df_escalas_group = identificar_trf_conjugados(df_escalas_group)

            # Colocando valores tarifarios
        
            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

            # Gerando valor final, verificando se existem colunas com tarifários específicos pra cada fornecedor

            df_escalas_group['Valor Final'] = df_escalas_group.apply(gerar_coluna_valor_final_rec, axis=1)

            # Verificando se todos os serviços estão na lista de serviços do tarifário
                        
            verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Fornecedores', 'Valor Final')

            st.session_state.df_pag_final_forn = df_escalas_group[['Data da Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Serviço Conjugado', 'Valor Final']]

# Opção de salvar o mapa gerado no Gsheet

if 'df_pag_final_forn_bg_4x4' in st.session_state and len(st.session_state.tipo_de_mapa)>0:

    with row1_2[1]:

        salvar_mapa = st.button('Salvar Mapa de Pagamentos | Buggy, 4x4 e Polo')

    if salvar_mapa and data_inicial and data_final:

        with st.spinner('Salvando mapa de pagamentos...'):

            df_insercao = gerar_df_insercao_mapa_pagamento(data_inicial, data_final, st.session_state.df_pag_final_forn_bg_4x4, 'Histórico de Pagamentos Buggy e 4x4')

            inserir_dataframe_gsheet(df_insercao, st.session_state.id_gsheet, 'Histórico de Pagamentos Buggy e 4x4')

elif 'df_pag_final_forn' in st.session_state and len(st.session_state.tipo_de_mapa)==0:

    with row1_2[1]:

        salvar_mapa = st.button('Salvar Mapa de Pagamentos Fornecedores')

    if salvar_mapa and data_inicial and data_final:

        with st.spinner('Salvando mapa de pagamentos...'):

            df_insercao = gerar_df_insercao_mapa_pagamento(data_inicial, data_final, st.session_state.df_pag_final_forn, 'Histórico de Pagamentos Fornecedores')

            inserir_dataframe_gsheet(df_insercao, st.session_state.id_gsheet, 'Histórico de Pagamentos Fornecedores')

# Gerar Mapas

if 'df_pag_final_forn' in st.session_state or 'df_pag_final_forn_bg_4x4' in st.session_state:

    # Se tiver gerando o mapa normal

    if len(st.session_state.tipo_de_mapa)==0:

        df_pag_final_ref = st.session_state.df_pag_final_forn

        colunas_valores_df_pag_ref = st.session_state.colunas_valores_df_pag_forn

        st.header('Gerar Mapas')

        row2 = st.columns(2)

        # Caixa de seleção de fornecedor

        fornecedor, lista_fornecedores = selecionar_fornecedor_do_mapa(row2, df_pag_final_ref)

        # Quando seleciona o fornecedor

        if fornecedor and data_pagamento and data_inicial and data_final:

            row2_1 = st.columns(4)

            identificar_cnpj_razao_social(fornecedor)

            total_a_pagar, df_pag_fornecedor = plotar_mapa_pagamento(fornecedor, row2_1, df_pag_final_ref)

            botao_download_html_individual(total_a_pagar, df_pag_fornecedor, fornecedor, colunas_valores_df_pag_ref)

        # Quando não tem fornecedor selecionado

        elif data_pagamento:

            row2_1 = st.columns(4)

            with row2_1[0]:

                enviar_informes_geral = st.button(f'Enviar Informes Gerais')

                # Envio de informes para todos os fornecedores da lista

                if enviar_informes_geral and data_pagamento:

                    with st.spinner('Puxando contatos de fornecedores...'):

                        puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Fornecedores', 'df_contatos')

                    lista_htmls, lista_htmls_email, lista_fornecedores_sem_contato, lista_fornecedores_contato_nulo = \
                        gerar_payload_envio_geral(lista_fornecedores, df_pag_final_ref, colunas_valores_df_pag_ref)

                    verificar_fornecedor_sem_contato(lista_fornecedores_sem_contato, st.session_state.id_gsheet, 'Contatos Fornecedores')

                    verificar_fornecedor_contato_nulo(lista_fornecedores_contato_nulo)

                    if len(lista_htmls)>0:

                        enviar_informes_gerais(lista_htmls)

                    if len(lista_htmls_email)>0:

                        enviar_emails_gerais(lista_htmls_email)

                # Geração de html com todos os fornecedores da lista independente de apertar botão

                elif not fornecedor:

                    nome_html = f'Mapas Fornecedores Geral.html'

                    with open(nome_html, "w", encoding="utf-8") as file:

                        pass
                    
                    gerar_html_mapa_fornecedores_geral(lista_fornecedores, df_pag_final_ref, colunas_valores_df_pag_ref)

                    botao_download_html_geral(nome_html, row2_1)

                    with row2_1[2]:

                        enviar_informes_financeiro = st.button(f'Enviar Informes Gerais p/ Financeiro')

                        if enviar_informes_financeiro:

                            lista_htmls, lista_htmls_email = gerar_payload_envio_geral_para_financeiro(lista_fornecedores, df_pag_final_ref, colunas_valores_df_pag_ref)

                            if len(lista_htmls)>0:

                                enviar_informes_gerais(lista_htmls)

                            if len(lista_htmls_email)>0:

                                enviar_emails_gerais(lista_htmls_email)

    # Se tiver gerando mapa de buggy e 4x4

    else:

        df_pag_final_ref = st.session_state.df_pag_final_forn_bg_4x4

        colunas_valores_df_pag_ref = st.session_state.colunas_valores_df_pag_buggy_4x4

        row2 = st.columns(5)

        with row2[2]:

            gerar_mapas_2 = st.button('Gerar Mapas Pós Descontos')

        if gerar_mapas_2:

            st.session_state.omitir_pag_final_bg_4x4 = True

        with row2[3]:

            voltar_para_alterar_descontos = st.button('Voltar p/ Alterar Descontos')

        if voltar_para_alterar_descontos:

            st.session_state.omitir_pag_final_bg_4x4 = False

        # Tabela p/ ajustes de desconto e valor de venda

        if not 'omitir_pag_final_bg_4x4' in st.session_state or st.session_state.omitir_pag_final_bg_4x4==False:

            with row2[0]:

                desconto = st.number_input('Desconto', value=None)

                alterar_desconto = st.button('Alterar Desconto')

                if alterar_desconto and st.session_state.index_escolhido:

                    st.session_state.df_pag_final_forn_bg_4x4.loc[st.session_state.index_escolhido, 'Desconto Reserva'] = desconto

                    calcular_e_ajustar_venda_liquida_valor_final()
   
            with row2[1]:

                venda = st.number_input('Valor Venda', value=None)

                alterar_venda = st.button('Alterar Valor Venda')

                if alterar_venda and st.session_state.index_escolhido:

                    st.session_state.df_pag_final_forn_bg_4x4.loc[st.session_state.index_escolhido, 'Valor Venda'] = venda

                    calcular_e_ajustar_venda_liquida_valor_final()
                                                
            row_height = 32
            header_height = 56  
            num_rows = len(st.session_state.df_pag_final_forn_bg_4x4)
            height_2 = header_height + (row_height * num_rows)

            gb_2 = GridOptionsBuilder.from_dataframe(st.session_state.df_pag_final_forn_bg_4x4)
            gb_2.configure_selection('single')
            gb_2.configure_grid_options(domLayout='autoHeight')
            gridOptions_2 = gb_2.build()

            grid_response_2 = AgGrid(st.session_state.df_pag_final_forn_bg_4x4, gridOptions=gridOptions_2, enable_enterprise_modules=False, fit_columns_on_grid_load=True, height=height_2)

            if not grid_response_2['selected_rows'] is None:

                st.session_state.index_escolhido = grid_response_2['selected_rows'].reset_index()['index'].astype(int).iloc[0]

            else:

                st.session_state.index_escolhido = None

        # Geração de mapas final

        else:

            with row2[0]:

                st.header('Gerar Mapas')

            # Caixa de seleção de fornecedor

            fornecedor, lista_fornecedores = selecionar_fornecedor_do_mapa(row2, df_pag_final_ref)

            # Quando seleciona o fornecedor

            if fornecedor and data_pagamento and data_inicial and data_final:

                row2_1 = st.columns(4)

                identificar_cnpj_razao_social(fornecedor)

                total_a_pagar, df_pag_fornecedor = plotar_mapa_pagamento(fornecedor, row2_1, df_pag_final_ref)

                botao_download_html_individual(total_a_pagar, df_pag_fornecedor, fornecedor, colunas_valores_df_pag_ref)

            # Quando não tem fornecedor selecionado

            elif data_pagamento:

                row2_1 = st.columns(4)

                with row2_1[0]:

                    enviar_informes_geral = st.button(f'Enviar Informes Gerais')

                    # Envio de informes para todos os fornecedores da lista

                    if enviar_informes_geral and data_pagamento:

                        with st.spinner('Puxando contatos de fornecedores...'):

                            puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Fornecedores', 'df_contatos')

                        lista_htmls, lista_htmls_email, lista_fornecedores_sem_contato, lista_fornecedores_contato_nulo = \
                            gerar_payload_envio_geral(lista_fornecedores, df_pag_final_ref, colunas_valores_df_pag_ref)

                        verificar_fornecedor_sem_contato(lista_fornecedores_sem_contato, st.session_state.id_gsheet, 'Contatos Fornecedores')

                        verificar_fornecedor_contato_nulo(lista_fornecedores_contato_nulo)

                        if len(lista_htmls)>0:

                            enviar_informes_gerais(lista_htmls)

                        if len(lista_htmls_email)>0:

                            enviar_emails_gerais(lista_htmls_email)

                    # Geração de html com todos os fornecedores da lista independente de apertar botão

                    elif not fornecedor:

                        nome_html = f'Mapas Fornecedores Geral.html'

                        with open(nome_html, "w", encoding="utf-8") as file:

                            pass
                        
                        gerar_html_mapa_fornecedores_geral(lista_fornecedores, df_pag_final_ref, colunas_valores_df_pag_ref)

                        botao_download_html_geral(nome_html, row2_1)

                        with row2_1[2]:

                            enviar_informes_financeiro = st.button(f'Enviar Informes Gerais p/ Financeiro')

                            if enviar_informes_financeiro:

                                lista_htmls, lista_htmls_email = gerar_payload_envio_geral_para_financeiro(lista_fornecedores, df_pag_final_ref, colunas_valores_df_pag_ref)

                                if len(lista_htmls)>0:

                                    enviar_informes_gerais(lista_htmls)

                                if len(lista_htmls_email)>0:

                                    enviar_emails_gerais(lista_htmls_email)

# Se tiver fornecedor selecionado, dá a opção de enviar o informe individual

if 'df_pag_final_forn' in st.session_state and fornecedor and data_pagamento:

    with row2_1[2]:

        enviar_informes_individual = st.button(f'Enviar Informes | {fornecedor}')

    if enviar_informes_individual:

        with st.spinner('Puxando contatos de fornecedores...'):

            puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Fornecedores', 'df_contatos')

        lista_fornecedores_contato_nulo, lista_fornecedores_sem_contato, contato_fornecedor = gerar_listas_fornecedores_sem_contato(fornecedor)

        verificar_fornecedor_sem_contato(lista_fornecedores_sem_contato, st.session_state.id_gsheet, 'Contatos Fornecedores')

        verificar_fornecedor_contato_nulo(lista_fornecedores_contato_nulo)

        if not '@' in contato_fornecedor:

            enviar_informes_individuais(contato_fornecedor)

        else:

            enviar_email_individual(contato_fornecedor)

    with row2_1[3]:

        enviar_informes_individual_financeiro = st.button(f'Enviar Informes | {fornecedor} p/ Financeiro')

        if enviar_informes_individual_financeiro:

            contato_financeiro = st.session_state.df_config[st.session_state.df_config['Configuração']=='Contato Financeiro']['Parâmetro'].iloc[0]

            if not '@' in contato_financeiro:

                enviar_informes_individuais(contato_financeiro)

            else:

                enviar_email_individual(contato_financeiro)  
