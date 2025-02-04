import streamlit as st
import pandas as pd
import mysql.connector
import decimal
from datetime import timedelta, time
from babel.numbers import format_currency
import gspread 
import requests
from google.oauth2 import service_account
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def gerar_df_phoenix(vw_name, base_luck):

    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
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

    st.session_state.view_phoenix='vw_pagamento_motoristas_aut'

    st.session_state.df_escalas = gerar_df_phoenix('vw_payment_guide', st.session_state.base_luck)

    st.session_state.df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Status do Servico']!='CANCELADO') & (~pd.isna(st.session_state.df_escalas['Escala']))].reset_index(drop=True)
    
    st.session_state.df_escalas['Data | Horario Apresentacao'] = pd.to_datetime(st.session_state.df_escalas['Data | Horario Apresentacao'], errors='coerce')
    
    st.session_state.df_escalas['Guia'] = st.session_state.df_escalas['Guia'].fillna('')

def puxar_infos_gdrive(id_gsheet, nome_df_1, aba_1, nome_df_2, aba_2, nome_df_3, aba_3):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(aba_1)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_1] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    st.session_state[nome_df_1]['Valor'] = pd.to_numeric(st.session_state[nome_df_1]['Valor'], errors='coerce')

    sheet = spreadsheet.worksheet(aba_2)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_2] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    sheet = spreadsheet.worksheet(aba_3)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_3] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def preencher_data_hora_voo_tt(df):

    df.loc[df['Tipo de Servico'].isin(['TOUR', 'TRANSFER']), 'Data Voo'] = df.loc[df['Tipo de Servico'].isin(['TOUR', 'TRANSFER']), 'Data | Horario Apresentacao'].dt.date

    df.loc[df['Tipo de Servico'].isin(['TOUR', 'TRANSFER']), 'Horario Voo'] = df.loc[df['Tipo de Servico'].isin(['TOUR', 'TRANSFER']), 'Data | Horario Apresentacao'].dt.time

    df['Horario Voo'] = pd.to_datetime(df['Horario Voo'], format='%H:%M:%S').dt.time

    df['Data | Horario Voo'] = pd.to_datetime(df['Data Voo'].astype(str) + ' ' + df['Horario Voo'].astype(str))

    return df

def verificar_veiculos_sem_diaria(df_filtrado):

    df_veiculos_sem_diaria = df_filtrado[pd.isna(df_filtrado['Valor Final'])]

    if len(df_veiculos_sem_diaria)>0:

        st.error(f'Algum dos veículos das escalas abaixo não tem valor de diária cadastrada. Cadastre e tente novamente, por favor')

        st.dataframe(df_veiculos_sem_diaria, hide_index=True)   

        st.stop()

def verificar_reservas_sem_voo(df_filtrado):

    if len(df_filtrado[df_filtrado['Data Voo']==''])>0:

        lista_reservas = ', '.join(df_filtrado[df_filtrado['Data Voo']=='']['Reserva'].unique().tolist())

        df_filtrado.loc[df_filtrado['Data Voo']=='', 'Data Voo'] = df_filtrado['Data | Horario Apresentacao'].dt.date

        df_filtrado.loc[pd.isna(df_filtrado['Horario Voo']), 'Horario Voo'] = df_filtrado['Data | Horario Apresentacao'].dt.time

        st.error(f'As reservas {lista_reservas} estão sem voo no IN ou OUT. O robô vai gerar os pagamentos, criando um horário fictício para o voo')

def ajustar_data_escala_voos_madrugada(df_filtrado):

    mask_in_out_voos_madrugada = (df_filtrado['Tipo de Servico'].isin(['IN', 'OUT'])) & ((df_filtrado['Horario Voo']<=time(4,0)) | (df_filtrado['Data | Horario Apresentacao'].dt.time<=time(4,0)))

    df_filtrado.loc[mask_in_out_voos_madrugada, 'Data da Escala'] = df_filtrado.loc[mask_in_out_voos_madrugada, 'Data da Escala'] - timedelta(days=1)

    return df_filtrado

def transformar_em_string(serie_dados):

    return ', '.join(list(set(serie_dados.dropna())))

def agrupar_escalas(df_filtrado):

    df_pag_geral = df_filtrado.groupby(['Escala', 'Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Veiculo', 'Motorista'])\
        .agg({'Data | Horario Voo': 'max', 'Data | Horario Apresentacao': 'max', 'Guia': 'first', 'Apoio': transformar_em_string}).reset_index()
    
    df_pag_geral = df_pag_geral.sort_values(by = ['Data da Escala', 'Data | Horario Apresentacao']).reset_index(drop=True)

    return df_pag_geral

def ajustar_data_tt_madrugada(df_pag_geral):

    mask_tt_madrugada = (df_pag_geral['Servico'].str.upper().str.contains('BY NIGHT|SÃO JOÃO|CATAMARÃ DO FORRÓ|PEGAR QUADRILHA')) & (df_pag_geral['Tipo de Servico']=='TOUR')

    df_pag_geral.loc[mask_tt_madrugada, 'Data | Horario Voo'] = (df_pag_geral.loc[mask_tt_madrugada, 'Data | Horario Apresentacao'] + timedelta(days=1))\
        .apply(lambda dt: dt.replace(hour=1, minute=0, second=0))

    return df_pag_geral

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

def coletar_dados_data_out_in_row(row, coluna_dados_out, coluna_dados_in, index, tipo_primeiro_trf):

    data_hora_out = pd.to_datetime(row[coluna_dados_out][index-1], unit='s')

    data_hora_in = pd.to_datetime(row[coluna_dados_out][index], unit='s')

    return data_hora_out, data_hora_in

def identificar_trf_conjugados(df):

    df['Regiao'] = df['Servico'].apply(map_regiao)

    df['Serviço Conjugado'] = ''

    df_in_out = df[df['Servico'].isin(st.session_state.dict_conjugados)].sort_values(by=['Motorista', 'Guia', 'Veiculo', 'Data | Horario Apresentacao']).reset_index()

    df_in_out_group = df_in_out.groupby(['Data da Escala', 'Guia', 'Motorista', 'Veiculo']).agg({'index': lambda x: list(x), 'Tipo de Servico': lambda x: list(x), 'Servico': lambda x: list(x), 
                                                                                                 'Data | Horario Apresentacao': lambda x: list(x), 'Data | Horario Voo': lambda x: list(x)}).reset_index()
    
    df_in_out_group = df_in_out_group[df_in_out_group['Tipo de Servico'].apply(filtro_tipo_servico_out_in)]

    for _, row in df_in_out_group.iterrows():

        lista_tipos_servicos = row['Tipo de Servico']

        for index in range(1, len(lista_tipos_servicos)):

            tipo_primeiro_trf, tipo_segundo_trf = coletar_dados_row(row, 'Tipo de Servico', index)

            if tipo_primeiro_trf=='OUT' and tipo_segundo_trf=='IN':

                data_hora_out, data_hora_in = coletar_dados_data_out_in_row(row, 'Data | Horario Apresentacao', 'Horario Voo', index, tipo_primeiro_trf)

                if data_hora_in - data_hora_out < timedelta(hours=4, minutes=15):

                    df = identificar_alterar_nome_servico_conjugado(row, index, df)

    return df

def verificar_passeios_sem_apoio(str_servicos):

    lista_servicos = str_servicos.split(', ')

    for item in lista_servicos:

        if item in st.session_state.df_passeios_sem_apoio['Servico'].unique().tolist():

            return 'X'
        
    return ''

def verificar_trf_apoio_ent_interestadual(df):

    df['Qtd. Serviços'] = df['Serviço Conjugado'].apply(lambda x: 0.5 if x =='X' else 1)

    df = df.groupby(['Data da Escala', 'Motorista']).agg({'Valor': 'max', 'Data | Horario Voo': 'max', 'Data | Horario Apresentacao': 'min', 'Qtd. Serviços': 'sum', 
                                                          'Tipo de Servico': transformar_em_string, 'Servico': transformar_em_string, 'Região': transformar_em_string, 'Veiculo': lambda x: list(x)})\
                                                            .reset_index()
    
    df['len_Servico'] = df['Servico'].apply(lambda x: len(x.split(', ')))

    df[['Apenas TRF/APOIO/ENTARDECER', 'Interestadual/Intermunicipal', 'Passeios sem Apoio']] = ''

    df = df[df['Qtd. Serviços']>1]

    mask_ent_aluguel_jpa = (~df['Tipo de Servico'].str.contains('TOUR')) | (df['Servico'].isin(['ENTARDECER NA PRAIA DO JACARÉ ', 'ALUGUEL DENTRO DE JPA'])) | \
        ((df['len_Servico']==2) & (df['Servico'].isin(['ENTARDECER NA PRAIA DO JACARÉ , ALUGUEL DENTRO DE JPA', 'ALUGUEL DENTRO DE JPA, ENTARDECER NA PRAIA DO JACARÉ '])))

    df.loc[mask_ent_aluguel_jpa, 'Apenas TRF/APOIO/ENTARDECER'] = 'X'

    df.loc[df['Região'].str.contains('INTERESTADUAL'), 'Interestadual/Intermunicipal'] = 'X'

    df['Passeios sem Apoio'] = df['Servico'].apply(verificar_passeios_sem_apoio)

    return df

def verificar_acrescimo(row):
    
    if pd.notna(row['Data | Horario Apresentacao']) and pd.notna(row['Data | Horario Voo']):

        data_apr = row['Data | Horario Apresentacao'].date()

        hora_apr = row['Data | Horario Apresentacao'].time()

        data_voo = row['Data | Horario Voo'].date()

        hora_voo = row['Data | Horario Voo'].time()

        if (time(4) < hora_apr <= time(18)) and ((data_voo == data_apr + timedelta(days=1)) or hora_voo <= time(4)):

            row['Acréscimo 50%'] = 'X'

            row['Valor 50%'] = row['Valor']*0.5

    return row

def precificar_acrescimo_50(df):

    df['Acréscimo 50%'] = ''

    df['Valor 50%'] = 0

    df = df.apply(verificar_acrescimo, axis=1)

    return df

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

        df_apoios_group = df_apoios_group[~df_apoios_group['Motorista Apoio'].str.contains('FARIAS|GIULIANO|NETO|JUNIOR')].reset_index(drop=True)
        
        df_apoios_group['Data | Horario Voo']=df_apoios_group['Data | Horario Apresentacao']

        df_apoios_group = df_apoios_group.rename(columns={'Veiculo Apoio': 'Veiculo', 'Motorista Apoio': 'Motorista', 'Guia Apoio': 'Guia', 'Escala Apoio': 'Escala'})

        df_apoios_group = df_apoios_group[['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Data | Horario Apresentacao']]

        df_apoios_group[['Servico', 'Tipo de Servico', 'Modo', 'Apoio', 'Horario Voo']] = ['APOIO', 'APOIO', 'REGULAR', None, time(0,0)]

        df = pd.concat([df, df_apoios_group], ignore_index=True)

    return df

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

def criar_output_html(nome_html, html, motorista, soma_servicos):

    with open(nome_html, "w", encoding="utf-8") as file:

        file.write(f'<p style="font-size:40px;">{motorista}</p>')

        file.write(f'<p style="font-size:30px;">Serviços prestados entre {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}</p>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:40px;">O valor total dos serviços é {soma_servicos}</p>')

        file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

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

def definir_nomes_servicos_veiculos_por_dia(row):

    lista_servicos = row['Servico'].split(', ')

    lista_veiculos = row['Veiculo']

    lista_servicos_veiculos = []

    encontrou_conjugado = 0

    for index in range(len(lista_servicos)):

        lista_servicos_veiculos.append(f"Serviço: {lista_servicos[index]} | Veículo: {lista_veiculos[index+encontrou_conjugado]}")

        if ' + ' in lista_servicos[index]:

            encontrou_conjugado+=1

    return '<br><br>'.join(lista_servicos_veiculos)

def gerar_df_insercao_mapa_pagamento(data_inicial, data_final):

    puxar_aba_simples(st.session_state.id_gsheet, 'Histórico de Pagamentos Motoristas Autônomos', 'df_historico_pagamentos')

    st.session_state.df_historico_pagamentos['Data da Escala'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data da Escala'], format='%d/%m/%Y').dt.date

    st.session_state.df_historico_pagamentos['Data/Horário de Início'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data/Horário de Início'], format='%d/%m/%Y %H:%M:%S')

    st.session_state.df_historico_pagamentos['Data/Horário de Término'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data/Horário de Término'], format='%d/%m/%Y %H:%M:%S')

    df_historico_fora_do_periodo = st.session_state.df_historico_pagamentos[~((st.session_state.df_historico_pagamentos['Data da Escala'] >= data_inicial) & 
                                                                              (st.session_state.df_historico_pagamentos['Data da Escala'] <= data_final))].reset_index(drop=True)
    
    df_insercao = pd.concat([df_historico_fora_do_periodo, st.session_state.df_pag_final_guias], ignore_index=True)

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

def selecionar_motorista_do_mapa(row2):

    with row2[0]:

        lista_motoristas = st.session_state.df_pag_final_motoristas['Motorista'].dropna().unique().tolist()

        motorista = st.selectbox('Motorista', sorted(lista_motoristas), index=None)

    return motorista, lista_motoristas

def plotar_mapa_pagamento(motorista, row2_1):

    df_pag_motorista = st.session_state.df_pag_final_motoristas[st.session_state.df_pag_final_motoristas['Motorista']==motorista].sort_values(by=['Data da Escala']).reset_index(drop=True)

    df_pag_motorista['Data da Escala'] = pd.to_datetime(df_pag_motorista['Data da Escala']).dt.strftime('%d/%m/%Y')

    df_pag_motorista['Data/Horário de Início'] = pd.to_datetime(df_pag_motorista['Data/Horário de Início']).dt.strftime('%d/%m/%Y %H:%M:%S')

    df_pag_motorista['Data/Horário de Término'] = pd.to_datetime(df_pag_motorista['Data/Horário de Término']).dt.strftime('%d/%m/%Y %H:%M:%S')

    container_dataframe = st.container()

    container_dataframe.dataframe(df_pag_motorista, hide_index=True, use_container_width = True)

    with row2_1[0]:

        total_a_pagar = df_pag_motorista['Valor Final'].sum()

        st.subheader(f'Valor Total: R${int(total_a_pagar)}')

    return total_a_pagar, df_pag_motorista

def botao_download_html_individual(total_a_pagar, df_pag_motorista, motorista):

    soma_servicos = format_currency(total_a_pagar, 'BRL', locale='pt_BR')

    for item in st.session_state.colunas_valores_df_pag_motoristas:

        df_pag_motorista[item] = df_pag_motorista[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

    html = definir_html(df_pag_motorista)

    nome_html = f'{motorista}.html'

    criar_output_html(nome_html, html, motorista, soma_servicos)

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

def gerar_payload_envio_geral(lista_motoristas):

    lista_htmls = []

    lista_htmls_email = []

    lista_motoristas_sem_contato = []

    lista_motoristas_contato_nulo = []

    for motorista_ref in lista_motoristas:

        if motorista_ref in st.session_state.df_contatos['Motoristas'].unique().tolist():

            contato_motorista = st.session_state.df_contatos.loc[st.session_state.df_contatos['Motoristas']==motorista_ref, 'Contato'].values[0]

            if contato_motorista=='':

                lista_motoristas_contato_nulo.append(motorista_ref)

        else:

            lista_motoristas_sem_contato.append(motorista_ref)

        df_pag_motorista = st.session_state.df_pag_final_motoristas[st.session_state.df_pag_final_motoristas['Motorista']==motorista_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        df_pag_motorista['Data da Escala'] = pd.to_datetime(df_pag_motorista['Data da Escala']).dt.strftime('%d/%m/%Y')

        df_pag_motorista['Data/Horário de Início'] = pd.to_datetime(df_pag_motorista['Data/Horário de Início']).dt.strftime('%d/%m/%Y %H:%M:%S')

        df_pag_motorista['Data/Horário de Término'] = pd.to_datetime(df_pag_motorista['Data/Horário de Término']).dt.strftime('%d/%m/%Y %H:%M:%S')

        soma_servicos = format_currency(df_pag_motorista['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in st.session_state.colunas_valores_df_pag_motoristas:

            df_pag_motorista[item] = df_pag_motorista[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_motorista)

        nome_html = f'{motorista_ref}.html'

        criar_output_html(nome_html, html, motorista_ref, soma_servicos)

        with open(nome_html, "r", encoding="utf-8") as file:

            html_content_motorista_ref = file.read()

        if '@' in contato_motorista:

            lista_htmls_email.append([html_content_motorista_ref, contato_motorista])

        else:

            lista_htmls.append([html_content_motorista_ref, contato_motorista])

    return lista_htmls, lista_htmls_email, lista_motoristas_sem_contato, lista_motoristas_contato_nulo

def verificar_motorista_sem_contato(lista_motoristas_sem_contato, id_gsheet, aba_gsheet):

    if len(lista_motoristas_sem_contato)>0:

        df_itens_faltantes = pd.DataFrame(lista_motoristas_sem_contato, columns=['Motoristas'])

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

        st.error('Os motoristas acima não estão na lista dos contatos. Por favor, cadastre o contato deles e tente novamente.')

        st.stop()

def verificar_motorista_contato_nulo(lista_motoristas_sem_contato):

    if len(lista_motoristas_sem_contato)>0:

        st.error(f"Os motoristas {', '.join(lista_motoristas_sem_contato)} estão na planilha de contatos, mas estão com o contato vazio. Preencha e tente novamente")

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

def inserir_html(nome_html, html, motorista, soma_servicos):

    with open(nome_html, "a", encoding="utf-8") as file:

        file.write('<div style="page-break-before: always;"></div>\n')

        file.write(f'<p style="font-size:40px;">{motorista}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:40px;">O valor total dos serviços é {soma_servicos}</p>')

def gerar_html_mapa_motoristas_geral(lista_motoristas):

    for motorista_ref in lista_motoristas:

        df_pag_motorista = st.session_state.df_pag_final_motoristas[st.session_state.df_pag_final_motoristas['Motorista']==motorista_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        df_pag_motorista['Data da Escala'] = pd.to_datetime(df_pag_motorista['Data da Escala']).dt.strftime('%d/%m/%Y')

        df_pag_motorista['Data/Horário de Início'] = pd.to_datetime(df_pag_motorista['Data/Horário de Início']).dt.strftime('%d/%m/%Y %H:%M:%S')

        df_pag_motorista['Data/Horário de Término'] = pd.to_datetime(df_pag_motorista['Data/Horário de Término']).dt.strftime('%d/%m/%Y %H:%M:%S')

        soma_servicos = format_currency(df_pag_motorista['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in st.session_state.colunas_valores_df_pag_motoristas:

            df_pag_motorista[item] = df_pag_motorista[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_motorista)

        inserir_html(nome_html, html, motorista_ref, soma_servicos)

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

def gerar_payload_envio_geral_para_financeiro(lista_motoristas):

    lista_htmls = []

    lista_htmls_email = []

    contato_financeiro = st.session_state.df_config[st.session_state.df_config['Configuração']=='Contato Financeiro']['Parâmetro'].iloc[0]

    for motorista_ref in lista_motoristas:

        df_pag_motorista = st.session_state.df_pag_final_motoristas[st.session_state.df_pag_final_motoristas['Guia']==motorista_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        df_pag_motorista['Data da Escala'] = pd.to_datetime(df_pag_motorista['Data da Escala']).dt.strftime('%d/%m/%Y')

        df_pag_motorista['Data/Horário de Início'] = pd.to_datetime(df_pag_motorista['Data/Horário de Início']).dt.strftime('%d/%m/%Y %H:%M:%S')

        df_pag_motorista['Data/Horário de Término'] = pd.to_datetime(df_pag_motorista['Data/Horário de Término']).dt.strftime('%d/%m/%Y %H:%M:%S')

        soma_servicos = format_currency(df_pag_motorista['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in st.session_state.colunas_valores_df_pag_motoristas:

            df_pag_motorista[item] = df_pag_motorista[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_motorista)

        nome_html = f'{motorista_ref}.html'

        criar_output_html(nome_html, html, motorista_ref, soma_servicos)

        with open(nome_html, "r", encoding="utf-8") as file:

            html_content_motorista_ref = file.read()

        if '@' in contato_financeiro:

            lista_htmls_email.append([html_content_motorista_ref, contato_financeiro])

        else:

            lista_htmls.append([html_content_motorista_ref, contato_financeiro])

    return lista_htmls, lista_htmls_email

def gerar_listas_motoristas_sem_contato(motorista):

    lista_motoristas_sem_contato = []

    lista_motoristas_contato_nulo = []

    if motorista in st.session_state.df_contatos['Motoristas'].unique().tolist():

        contato_motorista = st.session_state.df_contatos.loc[st.session_state.df_contatos['Motoristas']==motorista, 'Contato'].values[0]

        if contato_motorista=='':

            lista_motoristas_contato_nulo.append(motorista)

    else:

        lista_motoristas_sem_contato.append(motorista)

    return lista_motoristas_contato_nulo, lista_motoristas_sem_contato, contato_motorista

def enviar_informes_individuais(contato_motorista):
        
    payload = {"informe_html": st.session_state.html_content, 
                "telefone": contato_motorista}
    
    response = requests.post(st.session_state.id_webhook, json=payload)
        
    if response.status_code == 200:
        
        st.success(f"Mapas de Pagamento enviados pelo Whatsapp com sucesso!")
        
    else:
        
        st.error(f"Erro. Favor contactar o suporte")

        st.error(f"{response}")   

def enviar_email_individual(contato_motorista):

    assunto = f'Mapa de Pagamento {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}'

    enviar_email_gmail([contato_motorista], assunto, st.session_state.html_content, st.session_state.remetente_email, st.session_state.senha_email)

st.set_page_config(layout='wide')

if st.session_state.base_luck=='test_phoenix_joao_pessoa':

    # Puxando dados do Phoenix da 'vw_payment_guide'

    if not 'df_escalas' in st.session_state or st.session_state.view_phoenix!='vw_pagamento_motoristas_aut':

        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix()

    # Título da página

    st.title('Mapa de Pagamento - Motoristas')

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

    # Script pra gerar mapa de pagamento

    if data_final and data_inicial and gerar_mapa:

        # Puxando infos das planilhas

        with st.spinner('Puxando valores de diárias por veículo, ajudas de custo, passeios sem apoio...'):

            puxar_infos_gdrive(st.session_state.id_gsheet, 'df_veiculo_categoria', 'Tarifário Veículos', 'df_regiao', 'Passeios | Interestaduais', 'df_passeios_sem_apoio', 'Passeios sem Apoio')

        with st.spinner('Gerando mapas de pagamento...'):

            # Selecionando apenas os motoristas autônomos e período solicitados
        
            df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                    (st.session_state.df_escalas['Motorista'].str.contains('MOT AUT', na=False))].reset_index()
            
            # Ajustando Data | Horario Apresentacao de IN pra igualar ao do Voo
            
            df_escalas['Data | Horario Apresentacao'] = df_escalas.apply(lambda row: pd.to_datetime(str(row['Data da Escala']) + ' ' + str(row['Horario Voo'])) 
                                                                        if row['Tipo de Servico']=='IN' and not pd.isna(row['Horario Voo']) else row['Data | Horario Apresentacao'], axis=1)
            
            # Preenchendo Data Voo e Horario Voo com a data e horário de apresentação

            df_escalas = preencher_data_hora_voo_tt(df_escalas)

            # Verificando se existem reservas sem voo
        
            verificar_reservas_sem_voo(df_escalas)

            # Diminuindo 1 dia da data da escala, quando os voos são na madrugada
        
            df_escalas = ajustar_data_escala_voos_madrugada(df_escalas)

            # Agrupando escalas

            df_escalas_group = agrupar_escalas(df_escalas)

            # Adicionando Apoios no dataframe de pagamentos

            df_escalas_group = adicionar_apoios_em_dataframe(df_escalas_group)

            # Ajustar data de passeios que terminam na madrugada
        
            df_escalas_group = ajustar_data_tt_madrugada(df_escalas_group)

            # Adicionando valor de diária por veículo
        
            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_veiculo_categoria, on='Veiculo', how='left')

            # Inserindo região
        
            df_escalas_group = pd.merge(df_escalas_group, st.session_state.df_regiao, on = 'Servico', how = 'left')

            # Identificando transfers conjugados

            df_escalas_group = identificar_trf_conjugados(df_escalas_group)

            # Verificando se fez apenas TRF/APOIO/ENTARDECER e se teve serviço Interestadual/Intermunicipal ou passeios sem apoio
        
            df_escalas_group = verificar_trf_apoio_ent_interestadual(df_escalas_group)

            # Identificando Acréscimo 50%
        
            df_escalas_group = precificar_acrescimo_50(df_escalas_group)

            # Precificando ajudas de custo
        
            df_escalas_group['Ajuda de Custo'] = df_escalas_group.apply(lambda row: 25 if row['Interestadual/Intermunicipal']=='X' or 'ALUGUEL FORA DE JPA' in row['Servico'] else 
                                                                        15 if row['Apenas TRF/APOIO/ENTARDECER']=='X' or row['Passeios sem Apoio']=='X' else 0, axis=1)
            
            # Ajustando nomes de serviços e veículos utilizados por dia
            
            df_escalas_group['Serviços / Veículos'] = df_escalas_group.apply(definir_nomes_servicos_veiculos_por_dia, axis=1)

            # Calculando Valor Total da diária
        
            df_escalas_group['Valor Final'] = df_escalas_group['Valor'] + df_escalas_group['Valor 50%'] + df_escalas_group['Ajuda de Custo']

            # Renomeando colunas e ajustando estética
        
            df_escalas_group = df_escalas_group.rename(columns = {'Data | Horario Voo': 'Data/Horário de Término', 
                                                                    'Data | Horario Apresentacao': 'Data/Horário de Início', 'Valor': 'Valor Diária'})
        
            st.session_state.df_pag_final_motoristas = df_escalas_group[['Data da Escala', 'Motorista', 'Data/Horário de Início', 'Data/Horário de Término', 'Qtd. Serviços', 'Serviços / Veículos', 'Valor Diária', 
                                                                'Valor 50%', 'Ajuda de Custo', 'Valor Final']]
            
            # Verificando se tem veículo sem diária cadastrada
        
            verificar_veiculos_sem_diaria(df_escalas_group)

            st.session_state.df_pag_final_motoristas = st.session_state.df_pag_final_motoristas[(st.session_state.df_pag_final_motoristas['Data da Escala'] >= data_inicial) & 
                (st.session_state.df_pag_final_motoristas['Data da Escala'] <= data_final)].reset_index(drop=True)

    # Opção de salvar o mapa gerado no Gsheet

    if 'df_pag_final_motoristas' in st.session_state:

        with row1_2[1]:

            salvar_mapa = st.button('Salvar Mapa de Pagamentos')

        if salvar_mapa and data_inicial and data_final:

            with st.spinner('Salvando mapa de pagamentos...'):

                df_insercao = gerar_df_insercao_mapa_pagamento(data_inicial, data_final)

                inserir_dataframe_gsheet(df_insercao, st.session_state.id_gsheet, 'Histórico de Pagamentos Motoristas Autônomos')

    # Gerar Mapas

    if 'df_pag_final_motoristas' in st.session_state:

        st.header('Gerar Mapas')

        row2 = st.columns(2)

        # Caixa de seleção de motorista

        motorista, lista_motoristas = selecionar_motorista_do_mapa(row2)

        # Quando seleciona o motorista

        if motorista and data_pagamento and data_inicial and data_final:

            row2_1 = st.columns(4)

            total_a_pagar, df_pag_motorista = plotar_mapa_pagamento(motorista, row2_1)

            botao_download_html_individual(total_a_pagar, df_pag_motorista, motorista)

        # Quando não tem guia selecionado

        elif data_pagamento:

            row2_1 = st.columns(4)

            with row2_1[0]:

                enviar_informes_geral = st.button(f'Enviar Informes Gerais')

                # Envio de informes para todos os guias da lista

                if enviar_informes_geral and data_pagamento:

                    with st.spinner('Puxando contatos de motoristas...'):

                        puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Motoristas', 'df_contatos')

                    lista_htmls, lista_htmls_email, lista_motoristas_sem_contato, lista_motoristas_contato_nulo = gerar_payload_envio_geral(lista_motoristas)

                    verificar_motorista_sem_contato(lista_motoristas_sem_contato, st.session_state.id_gsheet, 'Contatos Motoristas')

                    verificar_motorista_contato_nulo(lista_motoristas_contato_nulo)

                    if len(lista_htmls)>0:

                        enviar_informes_gerais(lista_htmls)

                    if len(lista_htmls_email)>0:

                        enviar_emails_gerais(lista_htmls_email)

                # Geração de html com todos os guias da lista independente de apertar botão

                elif not motorista:

                    nome_html = f'Mapas Motoristas Geral.html'

                    with open(nome_html, "w", encoding="utf-8") as file:

                        pass
                    
                    gerar_html_mapa_motoristas_geral(lista_motoristas)

                    botao_download_html_geral(nome_html, row2_1)

                    with row2_1[2]:

                        enviar_informes_financeiro = st.button(f'Enviar Informes Gerais p/ Financeiro')

                        if enviar_informes_financeiro:

                            lista_htmls, lista_htmls_email = gerar_payload_envio_geral_para_financeiro(lista_motoristas)

                            if len(lista_htmls)>0:

                                enviar_informes_gerais(lista_htmls)

                            if len(lista_htmls_email)>0:

                                enviar_emails_gerais(lista_htmls_email)

    # Se tiver guia selecionado, dá a opção de enviar o informe individual

    if 'html_content' in st.session_state and motorista and data_pagamento:

        with row2_1[2]:

            enviar_informes_individual = st.button(f'Enviar Informes | {motorista}')

        if enviar_informes_individual:

            with st.spinner('Puxando contatos de motoristas...'):

                puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Motoristas', 'df_contatos')

            lista_motoristas_contato_nulo, lista_motoristas_sem_contato, contato_motorista = gerar_listas_motoristas_sem_contato(motorista)

            verificar_motorista_sem_contato(lista_motoristas_sem_contato, st.session_state.id_gsheet, 'Contatos Motoristas')

            verificar_motorista_contato_nulo(lista_motoristas_contato_nulo)

            if not '@' in contato_motorista:

                enviar_informes_individuais(contato_motorista)

            else:

                enviar_email_individual(contato_motorista)

        with row2_1[3]:

            enviar_informes_individual_financeiro = st.button(f'Enviar Informes | {motorista} p/ Financeiro')

            if enviar_informes_individual_financeiro:

                contato_financeiro = st.session_state.df_config[st.session_state.df_config['Configuração']=='Contato Financeiro']['Parâmetro'].iloc[0]

                if not '@' in contato_financeiro:

                    enviar_informes_individuais(contato_financeiro)

                else:

                    enviar_email_individual(contato_financeiro)

else:

    st.error('Esse painel funciona apenas p/ a base de João Pessoa.')
