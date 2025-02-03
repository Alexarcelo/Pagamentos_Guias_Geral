import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import gspread
from google.oauth2 import service_account
from datetime import time, timedelta
import numpy as np
from babel.numbers import format_currency
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def gerar_df_phoenix(vw_name, base_luck):
    
    config = {'user': 'user_automation_jpa', 'password': 'luck_jpa_2024', 'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com', 'database': base_luck}

    conexao = mysql.connector.connect(**config)

    cursor = conexao.cursor()

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

    st.session_state.df_escalas_bruto = gerar_df_phoenix('vw_pagamento_guias', st.session_state.base_luck)

    st.session_state.view_phoenix = 'vw_pagamento_guias'

    st.session_state.df_escalas = st.session_state.df_escalas_bruto[~(st.session_state.df_escalas_bruto['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO', 'RASCUNHO'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Status da Reserva'])) & ~(pd.isna(st.session_state.df_escalas_bruto['Escala'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Guia']))].reset_index(drop=True)
    
    st.session_state.df_cnpj_fornecedores = st.session_state.df_escalas_bruto[~pd.isna(st.session_state.df_escalas_bruto['Guia'])]\
        [['Guia', 'CNPJ/CPF Fornecedor Guia', 'Razao Social/Nome Completo Fornecedor Guia']].drop_duplicates().reset_index(drop=True)
    
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

def tratar_colunas_data_df(df, lista_colunas):

    for coluna in lista_colunas:

        df[coluna] = pd.to_datetime(df[coluna], format='%d/%m/%Y').dt.date

def tratar_colunas_lista_df(df, lista_colunas):

    for coluna in lista_colunas:

        df[coluna] = df[coluna].apply(lambda x: x.split(' & '))

def tratar_coluna_apoio_box():

    st.session_state.df_apoios_box['Tipo de Apoio (H ou F)'] = st.session_state.df_apoios_box['Tipo de Apoio (H ou F)'].replace({'F': 'APOIO AO BOX FULL', 'H': 'APOIO AO BOX HALF'})

    st.session_state.df_apoios_box = st.session_state.df_apoios_box.rename(columns={'Tipo de Apoio (H ou F)': 'Servico'})

    st.session_state.df_apoios_box.loc[st.session_state.df_apoios_box['Servico']=='APOIO AO BOX FULL', 'Valor Final'] = \
        st.session_state.df_config[st.session_state.df_config['Parâmetro']=='APOIO AO BOX FULL']['Valor Parâmetro'].iloc[0]

    st.session_state.df_apoios_box.loc[st.session_state.df_apoios_box['Servico']=='APOIO AO BOX HALF', 'Valor Final'] = \
        st.session_state.df_config[st.session_state.df_config['Parâmetro']=='APOIO AO BOX HALF']['Valor Parâmetro'].iloc[0]

def puxar_tarifario():

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Guias', 'df_tarifario')

    tratar_colunas_numero_df(st.session_state.df_tarifario, st.session_state.lista_colunas_nao_numericas)

def puxar_programacao_passeios():

    puxar_aba_simples(st.session_state.id_gsheet, 'Programação Passeios Espanhol', 'df_programacao_passeios_espanhol')

    tratar_colunas_data_df(st.session_state.df_programacao_passeios_espanhol, st.session_state.lista_colunas_data)

    tratar_colunas_lista_df(st.session_state.df_programacao_passeios_espanhol, st.session_state.lista_colunas_lista)

def puxar_apoios_box():

    puxar_aba_simples(st.session_state.id_gsheet, 'Apoios ao Box', 'df_apoios_box')

    tratar_colunas_data_df(st.session_state.df_apoios_box, st.session_state.lista_colunas_data)

    tratar_coluna_apoio_box()

    st.session_state.df_apoios_box[['Modo', 'Veiculo', 'Motorista', 'Motoguia', 'Idioma', 'Apenas Recepcao', 'Barco Carneiros']] = \
        ['REGULAR', '', '', '', '', '', 0]
    
    st.session_state.df_apoios_box = st.session_state.df_apoios_box[['Data da Escala', 'Modo', 'Servico', 'Veiculo', 'Motorista', 'Guia', 'Motoguia', 'Idioma', 'Apenas Recepcao', 
                                                                     'Barco Carneiros', 'Valor Final']]
    
    st.session_state.df_apoios_box = st.session_state.df_apoios_box[(st.session_state.df_apoios_box['Data da Escala']>=st.session_state.data_inicial) & 
                                                                    (st.session_state.df_apoios_box['Data da Escala']<=st.session_state.data_final)].reset_index(drop=True)

def puxar_configuracoes():

    puxar_aba_simples(st.session_state.id_gsheet, 'Configurações Guias', 'df_config')

    tratar_colunas_numero_df(st.session_state.df_config, st.session_state.lista_colunas_nao_numericas)

def puxar_servicos_navio():

    puxar_aba_simples(st.session_state.id_gsheet, 'Serviço de Guia - Navio', 'df_servicos_navio')

    tratar_colunas_data_df(st.session_state.df_servicos_navio, st.session_state.lista_colunas_data)

    tratar_colunas_numero_df(st.session_state.df_servicos_navio, st.session_state.lista_colunas_nao_numericas)

    st.session_state.df_servicos_navio[['Modo', 'Servico', 'Veiculo', 'Motorista', 'Motoguia', 'Idioma', 'Apenas Recepcao', 'Barco Carneiros']] = \
        ['REGULAR', 'Serviço de Guia - Navio', '', '', '', '', '', 0]

    st.session_state.df_servicos_navio = st.session_state.df_servicos_navio[['Data da Escala', 'Modo', 'Servico', 'Veiculo', 'Motorista', 'Guia', 'Motoguia', 'Idioma', 
                                                                             'Apenas Recepcao', 'Barco Carneiros', 'Valor Final']]
    
    st.session_state.df_servicos_navio = st.session_state.df_servicos_navio[(st.session_state.df_servicos_navio['Data da Escala']>=st.session_state.data_inicial) & 
                                                                            (st.session_state.df_servicos_navio['Data da Escala']<=st.session_state.data_final)].reset_index(drop=True)

def puxar_ubers():

    puxar_aba_simples(st.session_state.id_gsheet, 'Uber Guias', 'df_uber')

    tratar_colunas_numero_df(st.session_state.df_uber, st.session_state.lista_colunas_nao_numericas)

def puxar_eventos():

    puxar_aba_simples(st.session_state.id_gsheet, 'Eventos', 'df_eventos')

    tratar_colunas_data_df(st.session_state.df_eventos, st.session_state.lista_colunas_data)

def puxar_hora_extra():

    puxar_aba_simples(st.session_state.id_gsheet, 'Hora Extra Guias', 'df_hora_extra')

    tratar_colunas_numero_df(st.session_state.df_hora_extra, st.session_state.lista_colunas_nao_numericas)

def transformar_em_string(serie_dados):

    return ', '.join(list(set(serie_dados.dropna())))

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

        if 'Evento' in df_escalas_group.columns:

            st.dataframe(df_escalas_group[df_escalas_group[coluna_valores_none]=='Sem Tarifa!'][['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Idioma', 'Evento', 
                                                                                                 coluna_valores_none]], hide_index=True)
            
        else:

            st.dataframe(df_escalas_group[df_escalas_group[coluna_valores_none]=='Sem Tarifa!'][['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Idioma', coluna_valores_none]], 
                         hide_index=True)

        st.stop()

def identificar_passeios_regulares_saindo_de_porto(df_escalas_group):

    mask_passeios_porto_de_galinhas = (df_escalas_group['Servico'].str.contains(r'\(PORTO DE GALINHAS\)', na=False)) & (df_escalas_group['Tipo de Servico']=='TOUR') & (df_escalas_group['Modo']=='REGULAR')

    df_escalas_group.loc[mask_passeios_porto_de_galinhas, 'Passeios Saindo de Porto'] = 'X'

    df_escalas_group['Passeios Saindo de Porto'] = df_escalas_group['Passeios Saindo de Porto'].fillna('')

    return df_escalas_group

def filtrando_idiomas_passeios_programacao_espanhol(df_escalas_group):

    df_programacao_datas = st.session_state.df_programacao_passeios_espanhol.explode('Servico').reset_index(drop=True)

    servicos_programacao = df_programacao_datas['Servico'].unique().tolist()

    mask_idioma_programacao = (df_escalas_group['Idioma']!='pt-br') & (df_escalas_group['Passeios Saindo de Porto']=='X') & (df_escalas_group['Servico'].isin(servicos_programacao))

    df_escalas_saindo_de_porto_idioma = df_escalas_group[mask_idioma_programacao].reset_index()

    df_escalas_saindo_de_porto_idioma = pd.merge(df_escalas_saindo_de_porto_idioma, df_programacao_datas, on=['Data da Escala', 'Servico'], how='inner')

    df_escalas_group.loc[df_escalas_saindo_de_porto_idioma['index'], 'Idioma'] = 'pt-br'

    return df_escalas_group

def filtrar_idiomas_so_para_guias_que_falam_o_idioma(df_escalas_group):
    
    condicao = (df_escalas_group['Idioma']!='pt-br') & (~df_escalas_group['Guia'].isin(st.session_state.df_guias_idioma['Guias'].unique())) & (df_escalas_group['Tipo de Servico']=='TOUR')

    df_escalas_group.loc[condicao, 'Idioma'] = 'pt-br'

    return df_escalas_group

def calculo_diarias_motoguias_trf(df_escalas_group):

    df_escalas_motoguias_trf = df_escalas_group[(df_escalas_group['Motoguia']=='X') & (df_escalas_group['Tipo de Servico'].isin(['OUT', 'IN']))][['Guia', 'Data da Escala', 'Motoguia']].drop_duplicates()

    df_escalas_group['Indice_Original'] = df_escalas_group.index

    df_guia_data = pd.merge(df_escalas_group, df_escalas_motoguias_trf, on=['Guia', 'Data da Escala', 'Motoguia'], how='inner')

    df_guia_data = df_guia_data.groupby(['Data da Escala', 'Guia']).agg({'Indice_Original': lambda x: list(x)}).reset_index()

    for lista_index in df_guia_data['Indice_Original']:

        df_escalas_group.loc[lista_index[1:], 'Valor Final'] = 0

    df_escalas_group = df_escalas_group.drop(columns='Indice_Original')

    return df_escalas_group

def retirar_passeios_repetidos(df_escalas_group):

    df_escalas_passeios_repetidos = df_escalas_group[df_escalas_group['Tipo de Servico']=='TOUR'].groupby(['Data da Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico'])\
        .agg({'Escala': 'count', 'Idioma': lambda x: 'X' if any(x!='pt-br') else ''}).reset_index()
    
    df_escalas_passeios_repetidos = df_escalas_passeios_repetidos[df_escalas_passeios_repetidos['Escala']>1].reset_index(drop=True)

    df_escalas_group['Indice_Original'] = df_escalas_group.index

    df_ref = pd.merge(df_escalas_group, df_escalas_passeios_repetidos, on=['Data da Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico'], how='inner')

    df_ref = df_ref.groupby(['Data da Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico']).agg({'Indice_Original': lambda x: list(x), 'Idioma_y': 'first'}).reset_index()

    for _, row in df_ref.iterrows():

        lista_index = row['Indice_Original']

        if row['Idioma_y']=='X':

            df_escalas_group.loc[lista_index[0], 'Idioma'] = 'en-us'

        df_escalas_group = df_escalas_group.drop(index=lista_index[1:])
     
    df_escalas_group = df_escalas_group.drop(columns='Indice_Original').reset_index(drop=True)

    return df_escalas_group

def precificar_extra_barco_carneiros(df_escalas_group):

    df_escalas_group['Barco Carneiros'] = 0

    lista_escalas_extra_barco = st.session_state.df_extra_barco['Escala'].unique().tolist()

    df_escalas_group.loc[df_escalas_group['Escala'].isin(lista_escalas_extra_barco), 'Barco Carneiros'] = \
        st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Barco Carneiros']['Valor Parâmetro'].iloc[0]

    return df_escalas_group

def precificar_apenas_recepcao(df_escalas_group):

    df_escalas_group['Apenas Recepcao'] = ''

    lista_escalas_apenas_recepcao = st.session_state.df_apenas_recepcao['Escala'].unique().tolist()

    df_escalas_group.loc[df_escalas_group['Escala'].isin(lista_escalas_apenas_recepcao), ['Apenas Recepcao', 'Valor Final']] = \
        ['X', st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Apenas Recepção']['Valor Parâmetro'].iloc[0]]

    return df_escalas_group

def excluir_escalas_duplicadas(df_escalas_group, lista_servicos):

    df_data_veiculo = df_escalas_group[df_escalas_group['Servico'].isin(lista_servicos)][['Data da Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Escala']].drop_duplicates().reset_index(drop=True)

    df_data_veiculo = df_data_veiculo.groupby(['Data da Escala', 'Veiculo', 'Motorista', 'Guia']).agg({'Escala': lambda x: list(x), 'Servico': 'count'}).reset_index()

    df_data_veiculo = df_data_veiculo[df_data_veiculo['Servico']>1]

    excluir_escalas = []

    for lista_escalas in df_data_veiculo['Escala']:

        excluir_escalas.extend(lista_escalas[1:])

    df_escalas_group = df_escalas_group[~df_escalas_group['Escala'].isin(excluir_escalas)].reset_index(drop=True)

    return df_escalas_group

def gerar_df_pag_final_recife(df_escalas_group):

    st.session_state.df_pag_final_guias = df_escalas_group[['Data da Escala', 'Modo', 'Servico', 'Veiculo', 'Motorista', 'Guia', 'Motoguia', 'Idioma', 'Apenas Recepcao', 'Barco Carneiros', 'Valor Final']]

    st.session_state.df_pag_final_guias = pd.concat([st.session_state.df_pag_final_guias, st.session_state.df_apoios_box, st.session_state.df_servicos_navio], ignore_index=True)

def identificar_trf_ln_diurno_noturno(df_escalas_group):

    mask_ln_noturnos = (df_escalas_group['Servico'].isin([' OUT -  LITORAL NORTE ', 'IN  - LITORAL NORTE '])) & \
        ((pd.to_datetime(df_escalas_group['Horario Voo']).dt.time >= time(17,0)) | (pd.to_datetime(df_escalas_group['Data | Horario Apresentacao']).dt.time <= time(6,0)))
    
    mask_ln_diurnos = (df_escalas_group['Servico'].isin([' OUT -  LITORAL NORTE ', 'IN  - LITORAL NORTE '])) & \
        ~((pd.to_datetime(df_escalas_group['Horario Voo']).dt.time >= time(17,0)) | (pd.to_datetime(df_escalas_group['Data | Horario Apresentacao']).dt.time <= time(6,0)))

    df_escalas_group.loc[mask_ln_noturnos, 'Servico'] = df_escalas_group['Servico'] + ' - NOTURNO'

    df_escalas_group.loc[mask_ln_diurnos, 'Servico'] = df_escalas_group['Servico'] + ' - DIURNO'

    return df_escalas_group

def criar_colunas_escala_veiculo_mot_guia(df):

    df['Apoio'] = df['Apoio'].str.replace(r'Escala Auxiliar: | Veículo: | Motorista: | Guia: ', '', regex=True)

    df[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = ''

    df[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = df['Apoio'].str.split(',', expand=True)
    
    return df

def adicionar_apoios_em_dataframe(df):

    df_escalas_com_apoio = df[(df['Apoio']!='')].reset_index(drop=True)

    df_escalas_com_apoio['Apoio'] = df_escalas_com_apoio['Apoio'].apply(lambda x: x.split(' | ') if ' | ' in x else [x])

    df_apoios = df_escalas_com_apoio.explode('Apoio').reset_index(drop=True)

    df_apoios = df_apoios[~df_apoios['Apoio'].str.contains('Guia: null')].reset_index(drop=True)

    if len(df_apoios)>0:

        df_apoios = criar_colunas_escala_veiculo_mot_guia(df_apoios)

        df_apoios_group = df_apoios.groupby(['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']).agg({'Data da Escala': 'first', 'Data | Horario Apresentacao': 'first'}).reset_index()

        df_apoios_group = df_apoios_group.rename(columns={'Veiculo Apoio': 'Veiculo', 'Motorista Apoio': 'Motorista', 'Guia Apoio': 'Guia', 'Escala Apoio': 'Escala'})

        df_apoios_group = df_apoios_group[['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Data | Horario Apresentacao']]

        if 'Total ADT | CHD' in df.columns:

            df_apoios_group[['Servico', 'Tipo de Servico', 'Modo', 'Apoio', 'Idioma', 'Total ADT | CHD', 'Horario Voo']] = ['APOIO', 'TRANSFER', 'REGULAR', None, '', 0, time(0,0)]

        elif 'Idioma' in df.columns:

            df_apoios_group[['Servico', 'Tipo de Servico', 'Modo', 'Apoio', 'Idioma', 'Horario Voo']] = ['APOIO', 'TRANSFER', 'REGULAR', None, '', time(0,0)]

        else:

            df_apoios_group[['Servico', 'Tipo de Servico', 'Modo', 'Apoio', 'Horario Voo']] = ['APOIO', 'TRANSFER', 'REGULAR', None, time(0,0)]

        df = pd.concat([df, df_apoios_group], ignore_index=True)

    return df

def identificar_motoguias(df):

    df['Motoguia'] = ''

    df.loc[(df['Motorista']==df['Guia']), 'Motoguia'] = 'X'

    return df

def precificar_valor_de_servicos_em_eventos(df_escalas_pag, df_escalas):

    df_escalas_pag['Evento'] = ''

    for _, row in st.session_state.df_eventos.iterrows():

        data_inicial_evento = row['Data Inicial']

        data_final_evento = row['Data Final']

        nome_evento = row['Nome Evento']

        df_periodo_evento = df_escalas_pag[(df_escalas_pag['Data da Escala']>=data_inicial_evento) & (df_escalas_pag['Data da Escala']<=data_final_evento)].reset_index()

        df_escalas_periodo_evento = df_escalas[df_escalas['Escala'].isin(df_periodo_evento['Escala'].unique())].reset_index()

        df_escalas_periodo_evento = df_escalas_periodo_evento.groupby('Escala').agg({'Observacao': lambda obs: any(nome_evento in str(o) for o in obs)}).reset_index()

        lista_escalas_eventos = df_escalas_periodo_evento[df_escalas_periodo_evento['Observacao']==True]['Escala']

        df_escalas_pag.loc[df_escalas_pag['Escala'].isin(lista_escalas_eventos), 'Valor Serviço'] = df_escalas_pag.loc[df_escalas_pag['Escala'].isin(lista_escalas_eventos), 'Valor Evento']

        df_escalas_pag.loc[df_escalas_pag['Escala'].isin(lista_escalas_eventos), 'Evento'] = nome_evento

    return df_escalas_pag

def precificar_servicos_msc(df_escalas_group):

    df_msc = df_escalas_group[df_escalas_group['Parceiro'].str.upper().str.contains('MSC')].reset_index()

    for escala_ref in df_msc['Escala']:

        df_escalas_group['Valor Serviço'] = np.where((df_escalas_group['Escala'] == escala_ref) & (df_escalas_group['Guia'].isin(st.session_state.df_guias_tarifario_msc['Guias'].unique())),
            df_escalas_group['Valor MSC'], np.where((df_escalas_group['Escala'] == escala_ref) & ~(df_escalas_group['Guia'].isin(st.session_state.df_guias_tarifario_msc['Guias'].unique())), 
                                                    df_escalas_group['Valor MI'], df_escalas_group['Valor Serviço']))
        
    return df_escalas_group

def incrementar_valor_idioma_nao_msc(df_escalas_group):

    maks_idioma_nao_msc = (df_escalas_group['Idioma'] != 'pt-br') & (~df_escalas_group['Parceiro'].str.upper().str.contains('MSC'))

    df_escalas_group.loc[maks_idioma_nao_msc, 'Valor Serviço'] = df_escalas_group.loc[maks_idioma_nao_msc, 'Valor Serviço'] * 1.2

    return df_escalas_group

def incrementar_valor_idioma_msc(df_escalas_group):

    maks_idioma_msc = (df_escalas_group['Idioma'] != 'pt-br') & (df_escalas_group['Parceiro'].str.upper().str.contains('MSC'))
    
    df_escalas_group['n_idioma'] = df_escalas_group['Idioma'].apply(lambda x: len(x.split(', ')) if x else 0)

    df_escalas_group.loc[maks_idioma_msc, 'Valor Serviço'] = df_escalas_group.loc[maks_idioma_msc, 'Valor Serviço'] * (1 + df_escalas_group.loc[maks_idioma_msc, 'n_idioma'] * 0.2)

    return df_escalas_group

def retirar_idioma_tour_reg_8_paxs(df_escalas_group):

    df_tour_regular_idioma = df_escalas_group[(df_escalas_group['Tipo de Servico']=='TOUR') & (df_escalas_group['Modo']=='REGULAR') & (df_escalas_group['Idioma']!='pt-br') & 
                                              (df_escalas_group['Total ADT | CHD']<8)].reset_index()
    
    df_escalas_group.loc[df_tour_regular_idioma['index'], 'Idioma'] = 'pt-br'

    return df_escalas_group

def calcular_adicional_motoguia_tour(df):

    df['Adicional Passeio Motoguia'] = 0

    df.loc[(df['Motorista']==df['Guia']) & (df['Tipo de Servico']=='TOUR'), 'Adicional Passeio Motoguia'] = \
        st.session_state.df_config[st.session_state.df_config['Configuração']=='Adicional Motoguia']['Valor Parâmetro'].iloc[0]

    return df

def calcular_adicional_20h_pipatour(df):

    df['Adicional Motoguia Após 20:00'] = 0

    df.loc[(df['Servico']=='Pipatour ') & (df['Motorista']==df['Guia']), 'Adicional Motoguia Após 20:00'] = \
        st.session_state.df_config[st.session_state.df_config['Configuração']=='Adicional Motoguia Após 20:00']['Valor Parâmetro'].iloc[0]

    return df

def calcular_adicional_motoguia_ref_apoio(df):

    df['Adicional Diária Motoguia TRF|APOIO'] = 0

    df_motoguias_trf = df[(df['Motorista']==df['Guia']) & (df['Tipo de Servico'].isin(['TRANSFER', 'IN', 'OUT']))].reset_index()

    df_group = df_motoguias_trf.groupby(['Data da Escala', 'Guia']).agg({'Servico': 'count', 'index': 'first'})

    df.loc[df_group[df_group['Servico']==1]['index'], 'Adicional Diária Motoguia TRF|APOIO'] = \
        st.session_state.df_config[(st.session_state.df_config['Configuração']=='Adicional Diária Motoguia TRF|APOIO') & (st.session_state.df_config['Parâmetro']=='1 Serviço')]['Valor Parâmetro'].iloc[0]
    
    df.loc[df_group[df_group['Servico']>1]['index'], 'Adicional Diária Motoguia TRF|APOIO'] = \
        st.session_state.df_config[(st.session_state.df_config['Configuração']=='Adicional Diária Motoguia TRF|APOIO') & 
                                   (st.session_state.df_config['Parâmetro']=='Mais de 1 Serviço')]['Valor Parâmetro'].iloc[0]

    return df

def calcular_adicional_apos_20h_trf(df):

    df_trf_motoguia = df[(df['Tipo de Servico']!='TOUR') & (df['Servico']!='APOIO') & ((df['Motorista']==df['Guia']))].reset_index()

    df_trf_motoguia['Horario Voo'] = pd.to_datetime(df_trf_motoguia['Horario Voo']).dt.time

    df_trf_motoguia['Data | Horario Apresentacao'] = pd.to_datetime(df_trf_motoguia['Data | Horario Apresentacao']).dt.time

    dict_masks = {'Natal|Camurupim': time(19,0), 'Pipa|Touros': time(17,0), 'Gostoso': time(16,30)}

    for mascara in dict_masks:

        df_trf_ref = df_trf_motoguia[(df_trf_motoguia['Servico'].str.contains(mascara, case=False, na=False)) & (df_trf_motoguia['Horario Voo']>=dict_masks[mascara])].reset_index(drop=True) 
        
        df_trf_group = df_trf_ref.groupby(['Data da Escala', 'Guia']).agg({'Data | Horario Apresentacao': 'min', 'Horario Voo': 'max', 'index': 'first'}).reset_index()

        df_trf_group = df_trf_group[(df_trf_group['Data | Horario Apresentacao']<=time(16,0)) & (df_trf_group['Horario Voo']>=dict_masks[mascara])]

        if len(df_trf_group)>0:

            df.loc[df_trf_group['index'], 'Adicional Motoguia Após 20:00'] = \
                st.session_state.df_config[(st.session_state.df_config['Configuração']=='Adicional Diária TRF Motoguia Após 20:00')]['Valor Parâmetro'].iloc[0]

    return df

def colunas_voos_mais_tarde_cedo(df):

    df['Horario Voo Mais Tarde'] = df['Horario Voo'].apply(lambda x: max(x.split(', ')))

    df['Horario Voo Mais Tarde'] = pd.to_datetime(df['Horario Voo Mais Tarde']).dt.time

    df['Horario Voo Mais Cedo'] = df['Horario Voo'].apply(lambda x: min(x.split(', ')))

    df['Horario Voo Mais Cedo'] = pd.to_datetime(df['Horario Voo Mais Cedo']).dt.time

    return df

def filtro_tipo_servico(lista):
    encontrou_out = False
    for item in lista:
        if item == 'OUT':
            encontrou_out = True
        if item == 'IN' and encontrou_out:
            return True
    return False

def verificar_juncoes_in_out(df):

    df['Serviço Conjugado'] = ''

    df_in_out = df[df['Tipo de Servico'].isin(['IN', 'OUT'])].sort_values(by = ['Guia', 'Motorista', 'Veiculo', 'Data | Horario Apresentacao']).reset_index()

    df_in_out_group = df_in_out.groupby(['Data da Escala', 'Guia', 'Motorista', 'Veiculo']).agg({'index': lambda x: list(x), 'Tipo de Servico': lambda x: list(x), 'Escala': 'count'})\
        .reset_index()
    
    df_in_out_group = df_in_out_group[df_in_out_group['Tipo de Servico'].apply(filtro_tipo_servico)]

    for _, row in df_in_out_group.iterrows():

        lista_tipos_servicos = row['Tipo de Servico']

        for index in range(1, len(lista_tipos_servicos)):

            tipo_primeiro_trf = row['Tipo de Servico'][index-1]

            tipo_segundo_trf = row['Tipo de Servico'][index]

            if tipo_primeiro_trf=='OUT' and tipo_segundo_trf=='IN':

                lista_index_principal = row['index'][index-1:index+1]

                df.loc[lista_index_principal, 'Serviço Conjugado'] = 'X'

    return df

def calcular_valor_servico(row):

    if row['Tipo de Servico'] in ['TOUR', 'TRANSFER'] and row['Modo']!='REGULAR' and row['Est. Origem']=='':

        return row['Valor Privativo']
    
    elif row['Tipo de Servico'] in ['TOUR', 'TRANSFER'] and row['Modo']!='REGULAR' and row['Est. Origem']!='':

        return row['Valor Privativo BARA']

    elif row['Tipo de Servico'] in ['TOUR', 'TRANSFER'] and row['Modo']=='REGULAR':

        return row['Valor Regular']
    
    elif row['Tipo de Servico'] in ['IN', 'OUT'] and pd.notna(row['Valor TRF Interestadual']):

        return row['Valor TRF Interestadual']
    
    elif row['Tipo de Servico'] in ['IN', 'OUT'] and row['Diurno / Madrugada']=='DIURNO':

        return row['Valor TRF Diurno']
    
    elif row['Tipo de Servico'] in ['IN', 'OUT'] and row['Diurno / Madrugada']=='MADRUGADA':

        return row['Valor TRF Madrugada']
    
    else:

        return None
    
def ajustar_pag_giuliano_junior_neto(df):

    df_acordo_motoguias = st.session_state.df_config[st.session_state.df_config['Configuração']=='Acordo Motoguias'].reset_index(drop=True)

    for _, row in df_acordo_motoguias.iterrows():

        motoguia = row['Parâmetro']

        valor_ajuste = row['Valor Parâmetro']

        mask_motoguia = (df['Guia']==motoguia) & (df['Motoguia']=='X') & (df['Valor Final']<valor_ajuste)

        df.loc[mask_motoguia, 'Valor Final'] = valor_ajuste
    
    return df

def ajustar_valor_transferistas(df_pag_guias_in_out_final):

    df_acordo_transferistas = st.session_state.df_config[st.session_state.df_config['Configuração']=='Acordo Transferistas'].reset_index(drop=True)

    for _, row in df_acordo_transferistas.iterrows():

        transferista = row['Parâmetro']

        valor_ajuste = row['Valor Parâmetro']

        mask_transferistas = (df_pag_guias_in_out_final['Guia']==transferista) & (df_pag_guias_in_out_final['Valor Final']<valor_ajuste) & (df_pag_guias_in_out_final['Tipo de Servico'].isin(['IN', 'OUT']))

        df_pag_guias_in_out_final.loc[mask_transferistas, 'Valor Final'] = valor_ajuste

    return df_pag_guias_in_out_final

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

def criar_output_html(nome_html, html, guia, soma_servicos):

    with open(nome_html, "w", encoding="utf-8") as file:

        file.write(f'<p style="font-size:40px;">{guia}</p>')

        file.write(f'<p style="font-size:30px;">Serviços prestados entre {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}</p>')

        file.write(f'<p style="font-size:30px;">CPF / CNPJ: {st.session_state.cnpj}</p>')

        file.write(f'<p style="font-size:30px;">Razão Social / Nome Completo: {st.session_state.razao_social}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

        file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

def verificar_guia_sem_contato(lista_guias_sem_contato, id_gsheet, aba_gsheet):

    if len(lista_guias_sem_contato)>0:

        df_itens_faltantes = pd.DataFrame(lista_guias_sem_contato, columns=['Guias'])

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

        st.error('Os guias acima não estão na lista dos contatos. Por favor, cadastre o contato deles e tente novamente.')

        st.stop()

def verificar_guia_contato_nulo(lista_guias_sem_contato):

    if len(lista_guias_sem_contato)>0:

        st.error(f"Os guias {', '.join(lista_guias_sem_contato)} estão na planilha de contatos, mas estão com o contato vazio. Preencha e tente novamente")

        st.stop()

def gerar_payload_envio_geral(lista_guias):

    lista_htmls = []

    lista_htmls_email = []

    lista_guias_sem_contato = []

    lista_guias_contato_nulo = []

    for guia_ref in lista_guias:

        if guia_ref in st.session_state.df_contatos['Guias'].unique().tolist():

            contato_guia = st.session_state.df_contatos.loc[st.session_state.df_contatos['Guias']==guia_ref, 'Contato'].values[0]

            if contato_guia=='':

                lista_guias_contato_nulo.append(guia_ref)

        else:

            lista_guias_sem_contato.append(guia_ref)

        identificar_cnpj_razao_social(guia_ref)

        df_pag_guia = st.session_state.df_pag_final_guias[st.session_state.df_pag_final_guias['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista']).reset_index(drop=True)

        df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_guia['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in st.session_state.colunas_valores_df_pag:

            df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_guia)

        nome_html = f'{guia_ref}.html'

        criar_output_html(nome_html, html, guia_ref, soma_servicos)

        with open(nome_html, "r", encoding="utf-8") as file:

            html_content_guia_ref = file.read()

        if '@' in contato_guia:

            lista_htmls_email.append([html_content_guia_ref, contato_guia])

        else:

            lista_htmls.append([html_content_guia_ref, contato_guia])

    return lista_htmls, lista_htmls_email, lista_guias_sem_contato, lista_guias_contato_nulo

def enviar_informes_gerais(lista_htmls):

    payload = {"informe_html": lista_htmls}
    
    response = requests.post(st.session_state.id_webhook, json=payload)
        
    if response.status_code == 200:
        
        st.success(f"Mapas de Pagamentos enviados pelo Whatsapp com sucesso!")
        
    else:
        
        st.error(f"Erro. Favor contactar o suporte")

        st.error(f"{response}")

def inserir_html(nome_html, html, guia, soma_servicos):

    with open(nome_html, "a", encoding="utf-8") as file:

        file.write('<div style="page-break-before: always;"></div>\n')

        file.write(f'<p style="font-size:40px;">{guia}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:40px;">O valor total dos serviços é {soma_servicos}</p>')

def gerar_html_mapa_guias_geral(lista_guias):

    for guia_ref in lista_guias:

        df_pag_guia = st.session_state.df_pag_final_guias[st.session_state.df_pag_final_guias['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista']).reset_index(drop=True)

        df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_guia['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in st.session_state.colunas_valores_df_pag:

            df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_guia)

        inserir_html(nome_html, html, guia_ref, soma_servicos)

def gerar_listas_guias_sem_contato(guia):

    lista_guias_sem_contato = []

    lista_guias_contato_nulo = []

    if guia in st.session_state.df_contatos['Guias'].unique().tolist():

        contato_guia = st.session_state.df_contatos.loc[st.session_state.df_contatos['Guias']==guia, 'Contato'].values[0]

        if contato_guia=='':

            lista_guias_contato_nulo.append(guia)

    else:

        lista_guias_sem_contato.append(guia)

    return lista_guias_contato_nulo, lista_guias_sem_contato, contato_guia

def enviar_informes_individuais(contato_guia):
        
    payload = {"informe_html": st.session_state.html_content, 
                "telefone": contato_guia}
    
    response = requests.post(st.session_state.id_webhook, json=payload)
        
    if response.status_code == 200:
        
        st.success(f"Mapas de Pagamento enviados pelo Whatsapp com sucesso!")
        
    else:
        
        st.error(f"Erro. Favor contactar o suporte")

        st.error(f"{response}")   

def selecionar_guia_do_mapa(row2):

    with row2[0]:

        if 'Excluir Guias' in st.session_state.df_config['Configuração'].unique():

            lista_guias = st.session_state.df_pag_final_guias[~st.session_state.df_pag_final_guias['Guia']\
                .str.contains(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Guias']['Parâmetro'].iloc[0])]['Guia'].dropna().unique().tolist()

        else:

            lista_guias = st.session_state.df_pag_final_guias['Guia'].dropna().unique().tolist()

        guia = st.selectbox('Guia', sorted(lista_guias), index=None)

    return guia, lista_guias

def identificar_cnpj_razao_social(guia):

    st.session_state.cnpj = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Guia']==guia]['CNPJ/CPF Fornecedor Guia'].iloc[0]

    st.session_state.razao_social = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Guia']==guia]['Razao Social/Nome Completo Fornecedor Guia'].iloc[0]

def plotar_mapa_pagamento(guia, row2_1):

    df_pag_guia = st.session_state.df_pag_final_guias[st.session_state.df_pag_final_guias['Guia']==guia].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista']).reset_index(drop=True)

    df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

    container_dataframe = st.container()

    container_dataframe.dataframe(df_pag_guia, hide_index=True, use_container_width = True)

    with row2_1[0]:

        total_a_pagar = df_pag_guia['Valor Final'].sum()

        st.subheader(f'Valor Total: R${int(total_a_pagar)}')

    return total_a_pagar, df_pag_guia

def botao_download_html_individual(total_a_pagar, df_pag_guia, guia):

    soma_servicos = format_currency(total_a_pagar, 'BRL', locale='pt_BR')

    for item in st.session_state.colunas_valores_df_pag:

        df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

    html = definir_html(df_pag_guia)

    nome_html = f'{guia}.html'

    criar_output_html(nome_html, html, guia, soma_servicos)

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

def gerar_df_insercao_mapa_pagamento(data_inicial, data_final):

    puxar_aba_simples(st.session_state.id_gsheet, 'Histórico de Pagamentos Guias', 'df_historico_pagamentos')

    st.session_state.df_historico_pagamentos['Data da Escala'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data da Escala'], format='%d/%m/%Y').dt.date

    df_historico_fora_do_periodo = st.session_state.df_historico_pagamentos[~((st.session_state.df_historico_pagamentos['Data da Escala'] >= data_inicial) & 
                                                                                (st.session_state.df_historico_pagamentos['Data da Escala'] <= data_final))].reset_index(drop=True)
    
    df_insercao = pd.concat([df_historico_fora_do_periodo, st.session_state.df_pag_final_guias], ignore_index=True)

    df_insercao['Data da Escala'] = df_insercao['Data da Escala'].astype(str)

    return df_insercao

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

def enviar_email_individual(contato_guia):

    assunto = f'Mapa de Pagamento {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}'

    enviar_email_gmail([contato_guia], assunto, st.session_state.html_content, st.session_state.remetente_email, st.session_state.senha_email)

def gerar_payload_envio_geral_para_financeiro(lista_guias):

    lista_htmls = []

    lista_htmls_email = []

    contato_financeiro = st.session_state.df_config[st.session_state.df_config['Configuração']=='Contato Financeiro']['Parâmetro'].iloc[0]

    for guia_ref in lista_guias:

        identificar_cnpj_razao_social(guia_ref)

        df_pag_guia = st.session_state.df_pag_final_guias[st.session_state.df_pag_final_guias['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista']).reset_index(drop=True)

        df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_guia['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in st.session_state.colunas_valores_df_pag:

            df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_guia)

        nome_html = f'{guia_ref}.html'

        criar_output_html(nome_html, html, guia_ref, soma_servicos)

        with open(nome_html, "r", encoding="utf-8") as file:

            html_content_guia_ref = file.read()

        if '@' in contato_financeiro:

            lista_htmls_email.append([html_content_guia_ref, contato_financeiro])

        else:

            lista_htmls.append([html_content_guia_ref, contato_financeiro])

    return lista_htmls, lista_htmls_email

st.set_page_config(layout='wide')

# Inicializando view_phoenix no session_state

if not 'view_phoenix' in st.session_state:

    st.session_state.view_phoenix = ''

# Pegando parâmetros individuais de cada base

if not 'base_luck' in st.session_state:
    
    base_fonte = st.query_params["base_luck"]

    if base_fonte=='mcz':

        st.session_state.base_luck = 'test_phoenix_maceio'

        st.session_state.lista_colunas_nao_numericas = ['Servico', 'Configuração', 'Parâmetro', 'Escala']

        st.session_state.id_gsheet = '1EpOI0E936CTuPnklf7fOXKUTPoSmTdieWjy07WS6N7w'

        st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentoluckmaceio"

        st.session_state.colunas_valores_df_pag_forn = ['Valor Final']

        st.session_state.dict_tp_veic = {'Micrão': 'Micro', 'Ônibus': 'Bus', 'Ônibus DD': 'Bus DD', 'Ônibus LD': 'Bus LD'}

        st.session_state.lista_out_in_frances_sao_miguel = [['OUT- FRANCÊS', 'OUT - BARRA DE SÃO MIGUEL'], ['IN - FRANCÊS', 'IN - BARRA DE SÃO MIGUEL']]

        st.session_state.dict_conjugados = {'OUT - BARRA DE SANTO ANTÔNIO': 'Barra de Santo Antonio', 'IN - BARRA DE SANTO ANTÔNIO ': 'Barra de Santo Antonio', 
                                            'OUT - BARRA DE SÃO MIGUEL': 'Barra de Sao Miguel', 'IN - BARRA DE SÃO MIGUEL': 'Barra de Sao Miguel', 
                                            'OUT - GRANDE MACEIÓ': 'Grande Maceio', 'IN - GRANDE MACEIÓ': 'Grande Maceio', 
                                            'OUT - MARAGOGI / JAPARATINGA': 'Maragogi', 'IN - MARAGOGI / JAPARATINGA': 'Maragogi', 
                                            'OUT - ORLA DE MACEIÓ (OU PRÓXIMOS) ': 'Orla Maceio', 'IN - ORLA DE MACEIÓ (OU PRÓXIMOS)': 'Orla Maceio', 
                                            'OUT - PARIPUEIRA': 'Paripueira', 'IN - PARIPUEIRA ': 'Paripueira', 
                                            'OUT - SÃO MIGUEL DOS MILAGRES ': 'Milagres', 'IN -  SÃO MIGUEL DOS MILAGRES': 'Milagres', 
                                            'OUT JEQUIÁ DA PRAIA ': 'Jequia', 'IN JEQUIÁ DA PRAIA ': 'Jequia', 
                                            'OUT- FRANCÊS': 'Frances', 'IN - FRANCÊS': 'Frances'}
        
    elif base_fonte=='rec':

        st.session_state.base_luck = 'test_phoenix_recife'

        st.session_state.lista_colunas_nao_numericas = ['Servico', 'Configuração', 'Parâmetro', 'Data da Escala', 'Guia', 'Escala']

        st.session_state.lista_colunas_data = ['Data da Escala']

        st.session_state.lista_colunas_lista = ['Servico']

        st.session_state.id_gsheet = '1RwFPP9nQttGztxicHeJGTG6UqoL7fPKCWSdhhEdRVhE'

        st.session_state.id_gsheet_hoteis_piedade = '1az0u1yGWqIXE9KcUro6VznsVj7d5fozhH3dDsT1eI6A'

        st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentoluckrecife"

        st.session_state.colunas_valores_df_pag = ['Valor Final', 'Barco Carneiros']

        st.session_state.colunas_valores_df_pag_forn = ['Valor Pedágio', 'Valor Carroça', 'Valor Final']

        st.session_state.colunas_valores_df_pag_forn_add = ['Valor ADT', 'Valor CHD', 'Valor Final']

        st.session_state.colunas_numeros_inteiros_df_pag_forn_add = ['Total ADT', 'Total CHD', 'Total INF']

        st.session_state.dict_tp_veic = {'Executivo': 'Utilitario', 'Monovolume': 'Utilitario', 'SUV': 'Utilitario', 'Sedan': 'Utilitario', 'Ônibus': 'Bus', 'Micrão': 'Micro'}

        st.session_state.dict_conjugados = {'OUT (BOA VIAGEM | PIEDADE)': 'Boa Viagem', 'IN (BOA VIAGEM | PIEDADE)': 'Boa Viagem', 
                                            'OUT (PIEDADE)': 'Piedade', 'IN (PIEDADE)': 'Piedade', 
                                            'OUT (CABO DE STO AGOSTINHO)': 'Cabo', 'IN (CABO DE STO AGOSTINHO)': 'Cabo', 
                                            'OUT (CARNEIROS I TAMANDARÉ)': 'Carneiros', 'IN (CARNEIROS I TAMANDARÉ)': 'Carneiros', 
                                            'OUT (MARAGOGI | JAPARATINGA)': 'Maragogi', 'IN (MARAGOGI | JAPARATINGA)': 'Maragogi', 
                                            'OUT (OLINDA)': 'Olinda', 'IN (OLINDA)': 'Olinda', 
                                            'OUT (PORTO DE GALINHAS)': 'Porto', 'IN (PORTO DE GALINHAS)': 'Porto', 
                                            'OUT (SERRAMBI)': 'Serrambi', 'IN (SERRAMBI)': 'Serrambi', 
                                            'OUT RECIFE (CENTRO)': 'Recife', 'IN RECIFE (CENTRO)': 'Recife'}
        
        st.session_state.dict_trf_hotel_conjugado = {'TRF BOA VIAGEM OU PIEDADE / CABO DE STO AGOSTINHO OU PAIVA': 1, 'TRF CABO DE STO AGOSTINHO/BOA VIAGEM OU PIEDADE': 2,  
                                                     'TRF PIEDADE / CABO DE STO AGOSTINHO OU PAIVA': 3, 'TRF CABO DE STO AGOSTINHO/PIEDADE': 4,  
                                                     'TRF BOA VIAGEM OU PIEDADE / CARNEIROS OU TAMANDARÉ': 5, 'TRF CARNEIROS OU TAMANDARÉ / BOA VIAGEM OU PIEDADE': 6, 
                                                     'TRF PIEDADE / CARNEIROS OU TAMANDARÉ': 7, 'TRF CARNEIROS OU TAMANDARÉ / PIEDADE': 8, 
                                                     'TRF BOA VIAGEM OU PIEDADE / MARAGOGI OU JAPARATINGA': 9, 'TRF MARAGOGI OU JAPARATINGA / BOA VIAGEM OU PIEDADE': 10, 
                                                     'TRF PIEDADE / MARAGOGI OU JAPARATINGA': 11, 'TRF MARAGOGI OU JAPARATINGA / PIEDADE': 12, 
                                                     'TRF BOA VIAGEM OU PIEDADE / PORTO DE GALINHAS': 13, 'TRF PORTO DE GALINHAS / BOA VIAGEM OU PIEDADE': 14, 
                                                     'TRF PIEDADE / PORTO DE GALINHAS': 15, 'TRF PORTO DE GALINHAS / PIEDADE': 16, 
                                                     'TRF CABO DE STO AGOSTINHO OU PAIVA / PORTO DE GALINHAS': 17, 'TRF PORTO DE GALINHAS / CABO DE STO AGOSTINHO OU PAIVA': 18, 
                                                     'TRF PORTO DE GALINHAS / MARAGOGI OU JAPARATINGA': 19, 'TRF MARAGOGI OU JAPARATINGA / PORTO DE GALINHAS': 20}
        
        st.session_state.dict_trf_in_hotel_conjugado = {'IN (CABO DE STO AGOSTINHO)': 1, 'TRF CABO DE STO AGOSTINHO/BOA VIAGEM OU PIEDADE': 2, 'TRF CABO DE STO AGOSTINHO/PIEDADE': 3, 
                                                        'TRF CABO STO AGOSTINHO OU PAIVA / RECIFE (CENTRO)': 4,  
                                                        'IN (CARNEIROS I TAMANDARÉ)': 5, 'TRF CARNEIROS OU TAMANDARÉ / BOA VIAGEM OU PIEDADE': 6, 'TRF CARNEIROS OU TAMANDARÉ / PIEDADE': 7, 
                                                        'TRF CARNEIROS OU TAMANDARÉ / RECIFE (CENTRO)': 8, 
                                                        'IN (MARAGOGI | JAPARATINGA)': 9, 'TRF MARAGOGI OU JAPARATINGA / BOA VIAGEM OU PIEDADE': 10, 'TRF MARAGOGI OU JAPARATINGA / PIEDADE': 11, 
                                                        'TRF MARAGOGI OU JAPARATINGA / RECIFE ': 12, 
                                                        'IN (OLINDA)': 13, 'TRF OLINDA/RECIFE': 14, 
                                                        'IN (PORTO DE GALINHAS)': 15, 'TRF PORTO DE GALINHAS / BOA VIAGEM OU PIEDADE': 16, 'TRF PORTO DE GALINHAS / PIEDADE': 17, 
                                                        'TRF PORTO DE GALINHAS / RECIFE (CENTRO)': 18, 
                                                        'IN (SERRAMBI)': 19, 'TRF SERRAMBI / BOA VIAGEM OU PIEDADE': 20, 'TRF SERRAMBI / PIEDADE': 21, 'TRF SERRAMBI / RECIFE (CENTRO)': 22}
        
        st.session_state.dict_trf_hotel_out_conjugado = {'TRF BOA VIAGEM OU PIEDADE / CABO DE STO AGOSTINHO OU PAIVA': 1, 'TRF PIEDADE / CABO DE STO AGOSTINHO OU PAIVA': 2, 
                                                         'TRF PORTO DE GALINHAS / CABO DE STO AGOSTINHO OU PAIVA': 3, 'OUT (CABO DE STO AGOSTINHO)': 4, 
                                                         'TRF BOA VIAGEM OU PIEDADE / CARNEIROS OU TAMANDARÉ': 5, 'TRF PIEDADE / CARNEIROS OU TAMANDARÉ': 6, 'OUT (CARNEIROS I TAMANDARÉ)': 7,
                                                         'TRF BOA VIAGEM OU PIEDADE / MARAGOGI OU JAPARATINGA': 8, 'TRF PIEDADE / MARAGOGI OU JAPARATINGA': 9, 
                                                         'TRF PORTO DE GALINHAS / MARAGOGI OU JAPARATINGA': 10, 'OUT (MARAGOGI | JAPARATINGA)': 11,
                                                         'TRF BOA VIAGEM OU PIEDADE / PORTO DE GALINHAS': 12, 'TRF PIEDADE / PORTO DE GALINHAS': 13, 
                                                         'TRF CABO DE STO AGOSTINHO OU PAIVA / PORTO DE GALINHAS': 14, 'TRF MARAGOGI OU JAPARATINGA / PORTO DE GALINHAS': 15, 'OUT (PORTO DE GALINHAS)': 16}
        
        st.session_state.dict_nomes_fornecedores_ajuste = {'SV ': 'SALVATORE', 'HELENO VAN': 'HELENO VAN', 'SOARES': 'SOARES'}

    elif base_fonte=='ssa':

        st.session_state.base_luck = 'test_phoenix_salvador'

        st.session_state.lista_colunas_nao_numericas = ['Servico', 'Configuração', 'Parâmetro', 'Escala']

        st.session_state.lista_colunas_data = ['Data Inicial', 'Data Final']

        st.session_state.id_gsheet = '1TsOGz9-O1QcZiTnpT1tYiI0ZEBy1UCZ6qtV6iahLxFA'

        st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentolucksalvador"

        st.session_state.colunas_valores_df_pag = ['Valor Hora Extra', 'Valor Uber', 'Valor Final']

        st.session_state.colunas_valores_df_pag_forn = ['Carretinha', 'Valor Final']

        st.session_state.remetente_email = 'admluckssa@luckreceptivo.com.br'
        
        st.session_state.senha_email = 'acqjmbopixwtjbly'

        st.session_state.dict_tp_veic = {'Carro': 'Utilitario', 'Carro Executivo': 'Utilitario', 'Executivo': 'Utilitario', 'Minivan': 'Van', 'Sedan': 'Utilitario', 'Ônibus': 'Bus'}

        st.session_state.dict_conjugados = {' OUT -  LITORAL NORTE ': 'Litoral Norte', 'BAIXIO IN ': 'Baixio', 'BAIXIO OUT ': 'Baixio', 'IN  - LITORAL NORTE ': 'Litoral Norte'}

    elif base_fonte=='aju':

        st.session_state.base_luck = 'test_phoenix_aracaju'

        st.session_state.lista_colunas_nao_numericas = ['Servico', 'Configuração', 'Parâmetro']

        st.session_state.id_gsheet = '1R1Z67GNiGmYkEqyh-xP1GxyqD6k6-jxzzO81GXBIQJI'

        st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentoluckaracaju"

        st.session_state.colunas_valores_df_pag_forn = ['Valor Final']

        st.session_state.dict_tp_veic = {'BUS DD': 'Bus DD', 'Executivo': 'Utilitario', 'Micrão': 'Micro', 'MICRO-ÔNIBUS': 'Micro', 'MITSUBSHI': 'Utilitario', 'Ônibus': 'Bus', 'Ônibus DD': 'Bus DD'}

        st.session_state.dict_conjugados = {'Transfer OUT Aeroporto - Makai Resort (Barra dos Coqueiros)': 'Makai', 'Transfer IN Aeroporto - Makai Resort (Barra dos Coqueiros)': 'Makai', 
                                            'Transfer OUT Aeroporto - Região Orla': 'Aracaju', 'Transfer IN Aeroporto - Região Orla': 'Aracaju', 
                                            'Transfer OUT Aeroporto - Região Sul': 'Aracaju', 'Transfer IN Aeroporto - Região Sul': 'Aracaju'}

    elif base_fonte=='fen':

        st.session_state.base_luck = 'test_phoenix_noronha'

        st.session_state.lista_colunas_nao_numericas = ['Servico', 'Configuração', 'Parâmetro', 'Escala']

        st.session_state.id_gsheet = '1aGO6ni3zLJwzAXuhXNUZfIjjZ87japcg3GPsUEReMIs'

        st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentolucknoronha"

        st.session_state.colunas_valores_df_pag_forn = ['Valor ADT', 'Valor CHD', 'Valor Final']

        st.session_state.colunas_numeros_inteiros_df_pag_forn = ['Total ADT', 'Total CHD']

        st.session_state.lista_servicos_tarifarios_por_pax = ['ACTE MERGULHO BATISMO', 'MERGULHO BATISMO DE PRAIA', 'MERGULHO BATISMO EMBARCADO (MANHÃ)', 'MERGULHO BATISMO EMBARCADO (TARDE)', 
                                                              'MERGULHO CREDENCIADO C/ EQUIPAMENTO', 'MERGULHO CREDENCIADO S/ EQUIPAMENTO', 'PASSEIO DE BARCO', 'PASSEIO DE CANOA']
        
        st.session_state.lista_servicos_barcos = ['PASSEIO DE BARCO PRIVATIVO', 'BARCO PRIVATICO PRAIA CONCEICAO / PORTO']

    elif base_fonte=='nat':

        st.session_state.base_luck = 'test_phoenix_natal'

        st.session_state.lista_colunas_nao_numericas = ['Servico', 'Configuração', 'Parâmetro']

        st.session_state.id_gsheet = '1tsaBFwE3KS84r_I5-g3YGP7tTROe1lyuCw_UjtxofYI'

        st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentolucknatal"

        st.session_state.colunas_valores_df_pag = ['Adicional Passeio Motoguia', 'Adicional Motoguia Após 20:00', 'Adicional Diária Motoguia TRF|APOIO', 'Valor Serviço', 'Valor Final']

        st.session_state.colunas_valores_df_pag_forn = ['Valor Final']

        st.session_state.colunas_valores_df_pag_forn_add = ['Valor ADT', 'Valor CHD', 'Valor Final']

        st.session_state.colunas_numeros_inteiros_df_pag_forn_add = ['Total ADT', 'Total CHD']

        st.session_state.dict_tp_veic = {'Ônibus': 'Bus', 'Sedan': 'Utilitario', '4X4': 'Utilitario', 'Executivo': 'Utilitario', 'Micrão': 'Micro', 'Executivo Blindado': 'Utilitario', 
                                         'Monovolume': 'Utilitario'}
        
        st.session_state.dict_tratar_servico_in_out = {'In Natal - Hotéis Parceiros ': 'IN - Natal ', 'IN Touros - Hotéis Parceiros': 'IN - Touros', 'IN Pipa - Hotéis Parceiros ': 'IN - Pipa', 
                                                       'OUT Natal - Hotéis Parceiros ': 'OUT - Natal', 'OUT Pipa - Hotéis Parceiros': 'OUT - Pipa', 'OUT Touros - hotéis Parceiros': 'OUT - Touros'}
        
        st.session_state.dict_conjugados = {'OUT - Pipa': 'Pipa', 'IN - Pipa': 'Pipa', 'OUT - Touros': 'Touros', 'IN - Touros': 'Touros', 'OUT - Natal': 'Natal', 'IN - Natal ': 'Natal', 
                                            'OUT - Tripulacao': 'Tripulacao', 'IN - Tripulacao': 'Tripulacao', 'OUT - São Miguel Gostoso': 'Sao Miguel', 'IN - São Miguel Gostoso': 'Sao Miguel'}
        
        st.session_state.dict_trf_hotel_conjugado = {'TRF  Pipa/Natal': 1, 'TRF Natal/Pipa ': 2, 'TRF Natal/Touros': 3, 'TRF Touros/Natal': 4, 'TRF Natal/São Miguel': 5, 'TRF São Miguel/Natal': 6}

        st.session_state.lista_passeios_apoio_bolero_cunhau = ['Passeio à João Pessoa com Bolero (PIPA)', 'Passeio à Barra do Cunhaú (NAT)', 'Tour à Barra do Cunhaú (PIPA)']

    elif base_fonte=='jpa':

        st.session_state.base_luck = 'test_phoenix_joao_pessoa'

        st.session_state.lista_colunas_nao_numericas = ['Servico', 'Configuração', 'Parâmetro']

        st.session_state.id_gsheet = '1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E'

        st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentoluckjoaopessoa"

        st.session_state.colunas_valores_df_pag = ['Valor Final']

        st.session_state.colunas_valores_df_pag_forn = ['Valor Final']

        st.session_state.colunas_valores_df_pag_buggy_4x4 = ['Valor Venda', 'Desconto Reserva', 'Venda Líquida de Desconto', 'Valor Net', 'Valor Final']

        st.session_state.colunas_valores_df_pag_motoristas = ['Valor Diária', 'Valor 50%', 'Ajuda de Custo', 'Valor Final']

        st.session_state.excluir_servicos_df_sales = ['EXTRA']

        st.session_state.dict_tp_veic = {'Monovolume': 'Utilitario', 'Ônibus': 'Bus'}

        st.session_state.dict_conjugados = {'HOTÉIS JOÃO PESSOA / AEROPORTO JOÃO PESSOA': 'João Pessoa', 'AEROPORTO JOÃO PESSOA / HOTEIS JOÃO PESSOA': 'João Pessoa'}

if st.session_state.base_luck in ['test_phoenix_recife', 'test_phoenix_salvador', 'test_phoenix_natal', 'test_phoenix_joao_pessoa']:

    # Puxando dados do Phoenix

    if not 'df_escalas' in st.session_state or st.session_state.view_phoenix!='vw_pagamento_guias':
        
        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix()

    st.title('Mapa de Pagamento - Guias')

    st.divider()

    row1 = st.columns(2)

    # Container de datas e botão de gerar mapa

    with row1[0]:

        container_datas = st.container(border=True)

        container_datas.subheader('Período')

        data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

        data_final = container_datas.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

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

        # Base REC

        if st.session_state.base_luck == 'test_phoenix_recife':

            with st.spinner('Puxando configurações, tarifários, programação de passeios espanhol, extras barco Carneiros, guias apenas recepção, apoios ao box, serviços navio e guias idioma...'):

                puxar_configuracoes()

                puxar_tarifario()

                puxar_programacao_passeios()

                puxar_aba_simples(st.session_state.id_gsheet, 'Extra Barco', 'df_extra_barco')

                puxar_aba_simples(st.session_state.id_gsheet, 'Apenas Recepção', 'df_apenas_recepcao')

                puxar_aba_simples(st.session_state.id_gsheet, 'Guias Idioma', 'df_guias_idioma')

                puxar_apoios_box()

                puxar_servicos_navio()

            with st.spinner('Gerando mapas de pagamentos...'):

                # Gerando dataframe com escalas dentro do período selecionado

                df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)

                # Ajustando serviço GUIA BILINGUE pra o robô poder identificar como serviço com idioma diferente de português p/ base de Recife

                df_escalas.loc[df_escalas['Adicional'].str.contains('GUIA BILINGUE', na=False), 'Idioma'] = 'en-us'

                # Agrupando escalas

                df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Tipo de Servico', 'Modo'])\
                    .agg({'Apoio': transformar_em_string,  'Idioma': transformar_em_string}).reset_index()
                
                # Identificando passeios regulares saindo de PORTO

                df_escalas_group = identificar_passeios_regulares_saindo_de_porto(df_escalas_group)

                # Filtrando passeios que tem idioma diferente e estão dentro da programação de pagamento de idioma estrangeiro

                df_escalas_group = filtrando_idiomas_passeios_programacao_espanhol(df_escalas_group)

                # Filtrando pagamento de idioma só pra guias que realmente falam outros idiomas

                df_escalas_group = filtrar_idiomas_so_para_guias_que_falam_o_idioma(df_escalas_group)

                # Identificando e precificando motoguias

                df_escalas_group.loc[df_escalas_group['Motorista']==df_escalas_group['Guia'], ['Motoguia', 'Valor Final']] = \
                    ['X', st.session_state.df_config[st.session_state.df_config['Configuração']=='Valor Motoguia']['Valor Parâmetro'].iloc[0]]
                
                df_escalas_group['Motoguia'] = df_escalas_group['Motoguia'].fillna('')
                
                # Transformando vários trf feitos em um dia por um motoguia em valor de diária
                
                df_escalas_group = calculo_diarias_motoguias_trf(df_escalas_group)

                # Retirando escalas repetidas sem perder a informação de que existe paxs estrangeiro em alguma das escalas

                df_escalas_group = retirar_passeios_repetidos(df_escalas_group)

                # Precificando extra pago pelo rodízio no barco carneiros

                df_escalas_group = precificar_extra_barco_carneiros(df_escalas_group)

                # Precificando trf que foram feitos apenas recepção

                df_escalas_group = precificar_apenas_recepcao(df_escalas_group)

                # Excluir escalas de passeios duplicadas (quando o nome do passeio não é igual)
                
                for conjunto_passeios in st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Duplicados']['Parâmetro']:

                    lista_passeios_ref = conjunto_passeios.split(' & ')

                    df_escalas_group = excluir_escalas_duplicadas(df_escalas_group, lista_passeios_ref)

                # Colocando valores de serviços

                df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

                # Precificar valor final de todos os serviços
                
                df_escalas_group.loc[pd.isna(df_escalas_group['Valor Final']), 'Valor Final'] = df_escalas_group.loc[pd.isna(df_escalas_group['Valor Final'])]\
                    .apply(lambda row: row['Valor Idioma'] if row['Idioma'] != 'pt-br' else row['Valor'], axis=1)
                    
                # Verificando se todos os serviços estão na lista de serviços do tarifário
                    
                verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Guias', 'Valor Final')

                # Somando Barco Carneiros na coluna Valor Final
            
                df_escalas_group['Valor Final'] = df_escalas_group['Valor Final'] + df_escalas_group['Barco Carneiros']

                # Excluindo escalas duplicadas, porque as vezes a logística pra não ter que desescalar e escalar novamente, cria uma nova escala quando na verdade deveria inserir reserve_service
                # em uma existente

                df_escalas_group = df_escalas_group.drop_duplicates().reset_index(drop=True)

                # Gerando dataframe final

                gerar_df_pag_final_recife(df_escalas_group)

        # Base SSA

        elif st.session_state.base_luck == 'test_phoenix_salvador':

            with st.spinner('Puxando configurações, tarifários, ubers, eventos, horas extras, guias dentro do tarifário luck...'):

                puxar_configuracoes()

                puxar_tarifario()

                puxar_ubers()

                puxar_eventos()

                puxar_hora_extra()

                puxar_aba_simples(st.session_state.id_gsheet, 'Lista Guias Tarifário Luck', 'df_guias_tarifario_msc')

            with st.spinner('Gerando mapas de pagamentos...'):

                # Gerando dataframe com escalas dentro do período selecionado e retirando serviços que não devem entrar no mapa de pagamento (definidos nas configurações)

                df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                        (~st.session_state.df_escalas['Servico'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Serviços']['Parâmetro']))]\
                                                            .reset_index(drop=True)

                # Agrupando escalas

                df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Tipo de Servico', 'Modo'])\
                    .agg({'Apoio': transformar_em_string,  'Idioma': transformar_em_string, 'Horario Voo': 'max', 'Data | Horario Apresentacao': 'min', 'Parceiro': transformar_em_string}).reset_index()
                
                # Alterando TRF LITORAL NORTE p/ Diurno e Noturno

                df_escalas_group = identificar_trf_ln_diurno_noturno(df_escalas_group)

                # Adicionando Apoios no dataframe de pagamentos

                df_escalas_group = adicionar_apoios_em_dataframe(df_escalas_group)

                # Identificando motoguias

                df_escalas_group = identificar_motoguias(df_escalas_group)

                # Adicionando valor de uber por escala

                df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_uber[['Escala', 'Valor Uber']], on='Escala', how='left')

                df_escalas_group['Valor Uber'] = df_escalas_group['Valor Uber'].fillna(0)

                # Adicionando valor de hora extra por escala

                df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_hora_extra[['Escala', 'Valor Hora Extra']], on='Escala', how='left')

                df_escalas_group['Valor Hora Extra'] = df_escalas_group['Valor Hora Extra'].fillna(0)

                # Colocando valores de serviços

                df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

                # Gerando Valor Serviço

                df_escalas_group['Valor Serviço'] = df_escalas_group.apply(lambda row: row['Valor Motoguia'] if row['Motoguia'] == 'X' else row['Valor'], axis=1)

                # Precificar serviços feitos em período de evento

                df_escalas_group = precificar_valor_de_servicos_em_eventos(df_escalas_group, df_escalas)

                # Precificar serviços MSC

                df_escalas_group = precificar_servicos_msc(df_escalas_group)

                # Ajustando valores idioma sem MSC

                df_escalas_group = incrementar_valor_idioma_nao_msc(df_escalas_group)

                # Ajustando valores idiomas de serviços MSC

                df_escalas_group = incrementar_valor_idioma_msc(df_escalas_group)

                # Excluir escalas de passeios duplicadas (quando o nome do passeio não é igual)
                
                for conjunto_passeios in st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Duplicados']['Parâmetro']:

                    lista_passeios_ref = conjunto_passeios.split(' & ')

                    df_escalas_group = excluir_escalas_duplicadas(df_escalas_group, lista_passeios_ref)

                # Verificando se todos os serviços estão na lista de serviços do tarifário
                    
                verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Guias', 'Valor Serviço')

                # Gerando Valor Total

                df_escalas_group['Valor Final'] = df_escalas_group['Valor Uber'] + df_escalas_group['Valor Serviço'] + df_escalas_group['Valor Hora Extra']

                # Gerando dataframe final

                st.session_state.df_pag_final_guias = df_escalas_group[['Data da Escala', 'Servico', 'Veiculo', 'Motorista', 'Guia', 'Motoguia', 'Idioma', 'Valor Uber', 'Valor Hora Extra', 'Valor Final']]

        # Base NAT

        elif st.session_state.base_luck == 'test_phoenix_natal':

            with st.spinner('Puxando configurações, tarifários...'):

                puxar_configuracoes()

                puxar_tarifario()

            with st.spinner('Gerando mapas de pagamentos...'):

                # Filtrando período solicitado pelo usuário

                df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)

                # Adicionando somatório de ADT e CHD

                df_escalas['Total ADT | CHD'] = df_escalas['Total ADT'] + df_escalas['Total CHD']

                # Forçando idioma espanhol nos voos espanhois

                df_escalas.loc[df_escalas['Voo'].isin(st.session_state.df_config[st.session_state.df_config['Configuração']=='Voo Espanhol']['Parâmetro']), 'Idioma'] = 'es-es'

                # Ajustando idiomas estrangeiros

                df_escalas['Idioma'] = df_escalas['Idioma'].replace({'all': 'en-us', 'it-ele': 'en-us'})

                # Agrupando escalas

                df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Tipo de Servico', 'Modo'])\
                    .agg({'Apoio': transformar_em_string,  'Idioma': transformar_em_string, 'Total ADT | CHD': 'sum', 'Horario Voo': 'max', 'Data | Horario Apresentacao': 'min'}).reset_index()
                
                # Retirando informação da coluna Idioma para TOUR REGULAR com menos de 8 paxs

                df_escalas_group = retirar_idioma_tour_reg_8_paxs(df_escalas_group)

                # Calculando adicional p/ tours como motoguia

                df_escalas_group = calcular_adicional_motoguia_tour(df_escalas_group)

                # Calculando adicional motoguia após 20:00 p/ Pipatour

                df_escalas_group = calcular_adicional_20h_pipatour(df_escalas_group)

                # Adicionando Apoios no dataframe de pagamentos

                df_escalas_group = adicionar_apoios_em_dataframe(df_escalas_group)

                df_escalas_group['Adicional Passeio Motoguia'] = df_escalas_group['Adicional Passeio Motoguia'].fillna(0)

                df_escalas_group['Adicional Motoguia Após 20:00'] = df_escalas_group['Adicional Motoguia Após 20:00'].fillna(0)

                # Calculando adicional p/ motoguias em diversos TRF/APOIO

                df_escalas_group = calcular_adicional_motoguia_ref_apoio(df_escalas_group)

                # Calculando adicional motoguia após 20:00 p/ Transfers Natal, Camurupim, Pipa, Touros ou São Miguel do Gostoso

                df_escalas_group = calcular_adicional_apos_20h_trf(df_escalas_group)

                # Colocando valores tarifarios
        
                df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

                # Definindo valores de diárias

                df_escalas_group['Valor Serviço'] = df_escalas_group.apply(lambda row: row['Valor Padrão'] if row['Idioma'] in ['pt-br', ''] else 
                                                                        row['Valor Inglês'] if 'en-us' in row['Idioma'] else 
                                                                        row ['Valor Espanhol'] if 'es-es' in row['Idioma'] else 0, axis=1)
                
                # Verificando se todos os serviços estão na lista de serviços do tarifário
                    
                verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Guias', 'Valor Serviço')

                # Somando valores pra calcular o valor total de cada linha

                df_escalas_group['Valor Final'] = df_escalas_group['Adicional Passeio Motoguia'] + df_escalas_group['Adicional Motoguia Após 20:00'] + \
                    df_escalas_group['Adicional Diária Motoguia TRF|APOIO'] + df_escalas_group['Valor Serviço']
                
                # Ajustando pagamentos de DIDI e RODRIGO SALES

                df_escalas_group.loc[df_escalas_group['Guia'].isin(['DIDI', 'RODRIGO SALES']), 'Valor Final'] = df_escalas_group['Valor Serviço'] * 0.5

                # Excluir escalas de passeios duplicadas (quando o nome do passeio não é igual)
                
                for conjunto_passeios in st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Duplicados']['Parâmetro']:

                    lista_passeios_ref = conjunto_passeios.split(' & ')

                    df_escalas_group = excluir_escalas_duplicadas(df_escalas_group, lista_passeios_ref)

                # Gerando dataframe final

                st.session_state.df_pag_final_guias = df_escalas_group[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Veiculo', 'Motorista', 'Guia', 'Idioma', 'Adicional Passeio Motoguia', 
                                                                        'Adicional Motoguia Após 20:00', 'Adicional Diária Motoguia TRF|APOIO', 'Valor Serviço', 'Valor Final']]
            
        # Base JPA

        elif st.session_state.base_luck == 'test_phoenix_joao_pessoa':

            with st.spinner('Puxando configurações, tarifários...'):

                puxar_configuracoes()

                puxar_tarifario()

            with st.spinner('Gerando mapas de pagamentos...'):

                # Filtrando período solicitado pelo usuário

                df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                        (~st.session_state.df_escalas['Guia'].isin(['', 'SEM GUIA'])) & (~st.session_state.df_escalas['Servico'].str.upper().str.contains('4X4|BUGGY'))]\
                                                            .reset_index(drop=True)
                
                # Agrupando escalas

                df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Tipo de Servico', 'Modo'])\
                    .agg({'Apoio': transformar_em_string,  'Horario Voo': transformar_em_string, 'Data | Horario Apresentacao': 'min', 'Est. Origem': transformar_em_string}).reset_index()

                # Criando colunas com horários de voos mais tarde e mais cedo

                df_escalas_group = colunas_voos_mais_tarde_cedo(df_escalas_group)

                # Adicionando Apoios no dataframe de pagamentos

                df_escalas_group = adicionar_apoios_em_dataframe(df_escalas_group)

                # Identificando motoguias

                df_escalas_group = identificar_motoguias(df_escalas_group)

                # Deixando apenas BA´RA HOTEL na coluna Est. Origem quando o serviço não for regular

                df_escalas_group.loc[(df_escalas_group['Est. Origem'] != 'BA´RA HOTEL') | (df_escalas_group['Modo'] == 'REGULAR'), 'Est. Origem'] = ''

                # Identificando voos diurnos e na madrugada

                df_escalas_group['Diurno / Madrugada'] = df_escalas_group.apply(lambda row: 'MADRUGADA' if row['Tipo de Servico'] in ['IN', 'OUT'] and 
                                                                                ((row['Data | Horario Apresentacao'].time()<=time(4,0)) or (row['Horario Voo Mais Tarde']<=time(4))) 
                                                                                else 'DIURNO', axis=1)
                
                # Diminuindo 1 dia dos OUTs da madrugada, mas que tem horário no final do dia anterior

                mask_out_madrugada = (df_escalas_group['Tipo de Servico']=='OUT') & (df_escalas_group['Diurno / Madrugada']=='MADRUGADA') & \
                    (pd.to_datetime(df_escalas_group['Data | Horario Apresentacao']).dt.time>time(4))

                df_escalas_group.loc[mask_out_madrugada, 'Data | Horario Apresentacao'] = df_escalas_group.loc[mask_out_madrugada, 'Data | Horario Apresentacao'] - timedelta(days=1)

                # Verificando junções de OUTs e INs

                df_escalas_group = verificar_juncoes_in_out(df_escalas_group)

                # Colocando valores tarifarios
        
                df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

                # Criando coluna Valor Final pra escolher quais das colunas do tarifário vai ser usada pra tarifar cada serviço
            
                df_escalas_group['Valor Final'] = df_escalas_group.apply(calcular_valor_servico, axis=1)

                # Verificando se todos os serviços estão na lista de serviços do tarifário
                    
                verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Guias', 'Valor Final')

                # Ajustando valor final em 50% de aumento p/ serviço de motoguia

                df_escalas_group.loc[df_escalas_group['Motoguia']=='X', 'Valor Final'] = df_escalas_group.loc[df_escalas_group['Motoguia']=='X', 'Valor Final']*1.5

                # Ajustando pagamento de Giuliano, Junior e Neto

                df_escalas_group = ajustar_pag_giuliano_junior_neto(df_escalas_group)

                # Ajustando valor mínimo de transferistas

                df_escalas_group = ajustar_valor_transferistas(df_escalas_group)

                # Ajustando valor final em 50% de redução p/ trf conjugado

                df_escalas_group.loc[df_escalas_group['Serviço Conjugado']=='X', 'Valor Final'] = df_escalas_group.loc[df_escalas_group['Serviço Conjugado']=='X', 'Valor Final']*0.5

                st.session_state.df_pag_final_guias = df_escalas_group[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Est. Origem', 'Veiculo', 'Motorista', 'Guia', 'Motoguia', 
                                                                        'Serviço Conjugado', 'Valor Final']]

    # Opção de salvar o mapa gerado no Gsheet

    if 'df_pag_final_guias' in st.session_state:

        with row1_2[1]:

            salvar_mapa = st.button('Salvar Mapa de Pagamentos')

        if salvar_mapa and data_inicial and data_final:

            with st.spinner('Salvando mapa de pagamentos...'):

                df_insercao = gerar_df_insercao_mapa_pagamento(data_inicial, data_final)

                inserir_dataframe_gsheet(df_insercao, st.session_state.id_gsheet, 'Histórico de Pagamentos Guias')

    # Gerar Mapas

    if 'df_pag_final_guias' in st.session_state:

        st.header('Gerar Mapas')

        row2 = st.columns(2)

        # Caixa de seleção de guia

        guia, lista_guias = selecionar_guia_do_mapa(row2)

        # Quando seleciona o guia

        if guia and data_pagamento and data_inicial and data_final:

            row2_1 = st.columns(4)

            identificar_cnpj_razao_social(guia)

            total_a_pagar, df_pag_guia = plotar_mapa_pagamento(guia, row2_1)

            botao_download_html_individual(total_a_pagar, df_pag_guia, guia)

        # Quando não tem guia selecionado

        elif data_pagamento:

            row2_1 = st.columns(4)

            with row2_1[0]:

                enviar_informes_geral = st.button(f'Enviar Informes Gerais')

                # Envio de informes para todos os guias da lista

                if enviar_informes_geral and data_pagamento:

                    with st.spinner('Puxando contatos de guias...'):

                        puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Guias', 'df_contatos')

                    lista_htmls, lista_htmls_email, lista_guias_sem_contato, lista_guias_contato_nulo = gerar_payload_envio_geral(lista_guias)

                    verificar_guia_sem_contato(lista_guias_sem_contato, st.session_state.id_gsheet, 'Contatos Guias')

                    verificar_guia_contato_nulo(lista_guias_contato_nulo)

                    if len(lista_htmls)>0:

                        enviar_informes_gerais(lista_htmls)

                    if len(lista_htmls_email)>0:

                        enviar_emails_gerais(lista_htmls_email)

                # Geração de html com todos os guias da lista independente de apertar botão

                elif not guia:

                    nome_html = f'Mapas Guias Geral.html'

                    with open(nome_html, "w", encoding="utf-8") as file:

                        pass
                    
                    gerar_html_mapa_guias_geral(lista_guias)

                    botao_download_html_geral(nome_html, row2_1)

                    with row2_1[2]:

                        enviar_informes_financeiro = st.button(f'Enviar Informes Gerais p/ Financeiro')

                        if enviar_informes_financeiro:

                            lista_htmls, lista_htmls_email = gerar_payload_envio_geral_para_financeiro(lista_guias)

                            if len(lista_htmls)>0:

                                enviar_informes_gerais(lista_htmls)

                            if len(lista_htmls_email)>0:

                                enviar_emails_gerais(lista_htmls_email)

    # Se tiver guia selecionado, dá a opção de enviar o informe individual

    if 'html_content' in st.session_state and guia and data_pagamento:

        with row2_1[2]:

            enviar_informes_individual = st.button(f'Enviar Informes | {guia}')

        if enviar_informes_individual:

            with st.spinner('Puxando contatos de guias...'):

                puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Guias', 'df_contatos')

            lista_guias_contato_nulo, lista_guias_sem_contato, contato_guia = gerar_listas_guias_sem_contato(guia)

            verificar_guia_sem_contato(lista_guias_sem_contato, st.session_state.id_gsheet, 'Contatos Guias')

            verificar_guia_contato_nulo(lista_guias_contato_nulo)

            if not '@' in contato_guia:

                enviar_informes_individuais(contato_guia)

            else:

                enviar_email_individual(contato_guia)

        with row2_1[3]:

            enviar_informes_individual_financeiro = st.button(f'Enviar Informes | {guia} p/ Financeiro')

            if enviar_informes_individual_financeiro:

                contato_financeiro = st.session_state.df_config[st.session_state.df_config['Configuração']=='Contato Financeiro']['Parâmetro'].iloc[0]

                if not '@' in contato_financeiro:

                    enviar_informes_individuais(contato_financeiro)

                else:

                    enviar_email_individual(contato_financeiro)

else:

    st.error('Esse painel funciona apenas p/ as bases de Recife, Salvador, João Pessoa e Natal.')
