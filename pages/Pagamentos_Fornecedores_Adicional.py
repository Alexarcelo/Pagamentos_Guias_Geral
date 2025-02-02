import streamlit as st
import pandas as pd
import mysql.connector
import decimal
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

    if vw_name=='vw_router':
        request_name = f'SELECT `Data Execucao`, `Reserva`, `Status da Reserva`, `Status do Servico`, `Servico`, `Est Origem`, `Id_Servico` FROM {vw_name}'
    elif vw_name=='vw_reembolsos':
        request_name = f'SELECT `reserve_service_id`, `status` FROM {vw_name}'
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

    st.session_state.view_phoenix = 'vw_pagamento_fornecedores_adicionais'

    st.session_state.df_escalas_bruto = gerar_df_phoenix('vw_pagamento_fornecedores', st.session_state.base_luck)

    st.session_state.df_escalas = st.session_state.df_escalas_bruto[~(st.session_state.df_escalas_bruto['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO', 'RASCUNHO'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Status da Reserva'])) & ~(pd.isna(st.session_state.df_escalas_bruto['Escala']))]\
                                                                        .reset_index(drop=True)
    
    if st.session_state.base_luck=='test_phoenix_recife':

        st.session_state.df_router = gerar_df_phoenix('vw_router', st.session_state.base_luck)

        st.session_state.df_router = st.session_state.df_router[~(st.session_state.df_router['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO', 'RASCUNHO'])) & 
                                                                ~(st.session_state.df_router['Status do Servico'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO', 'RASCUNHO'])) & 
                                                                ~(pd.isna(st.session_state.df_router['Status da Reserva']))].reset_index(drop=True)

        st.session_state.df_reembolsos = gerar_df_phoenix('vw_reembolsos', st.session_state.base_luck)

        st.session_state.df_reembolsos = st.session_state.df_reembolsos.rename(columns={'reserve_service_id': 'Id_Servico'})

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

    puxar_aba_simples(st.session_state.id_gsheet, 'Configurações Fornecedores (Adicional)', 'df_config')

    tratar_colunas_numero_df(st.session_state.df_config, st.session_state.lista_colunas_nao_numericas)

def puxar_tarifario_fornecedores():

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Fornecedores (Adicional)', 'df_tarifario')

    tratar_colunas_numero_df(st.session_state.df_tarifario, st.session_state.lista_colunas_nao_numericas)

def transformar_em_string(serie_dados):

    return ', '.join(list(set(serie_dados.dropna())))

def gerar_escalas_agrupadas(data_inicial, data_final):

    if st.session_state.base_luck=='test_phoenix_recife':

        df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                (~pd.isna(st.session_state.df_escalas['adicional'])) & (st.session_state.df_escalas['adicional'].str.contains('CATAMAR|BUGGY')) & 
                                                (~st.session_state.df_escalas['Servico'].str.contains('CITY'))].reset_index(drop=True)
        
        df_router = st.session_state.df_router[(st.session_state.df_router['Data Execucao'] >= data_inicial) & (st.session_state.df_router['Data Execucao'] <= data_final) & 
                                            (st.session_state.df_router['Servico'].str.contains('BUGGY PONTA A PONTA'))].reset_index(drop=True)

        df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'adicional': transformar_em_string, 'Total ADT': 'sum', 'Total CHD': 'sum', 'Total INF': 'sum'}).reset_index()
        
        return df_escalas_group, df_router
    
    elif st.session_state.base_luck=='test_phoenix_natal':

        df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                 (~pd.isna(st.session_state.df_escalas['adicional'])) & 
                                                 (~st.session_state.df_escalas['adicional'].isin(['', 'Água Mineral (Luck Natal)', 'Cadeirinha de bebê  (Luck Natal)', 
                                                                                                  'Deslocamento de Hoteis Distante (Luck Natal)'])) & 
                                                 (st.session_state.df_escalas['adicional'].str.upper().str.contains('LANCHA|BARCO|JARDINEIRA'))].reset_index(drop=True)

        df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'adicional': transformar_em_string, 'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

        df_escalas_group = df_escalas_group[~df_escalas_group['adicional'].isin(['', 'Água Mineral (Luck Natal)', 'Cadeirinha de bebê  (Luck Natal)', 'Deslocamento de Hoteis Distante (Luck Natal)'])]\
            .reset_index(drop=True)
        
        return df_escalas_group

def gerar_df_pag_buggy_pp(df):

    df = pd.merge(df, st.session_state.df_tarifario[['Servico', 'Valor ADT']], on='Servico', how='left')

    df = df[['Data Execucao', 'Servico', 'Id_Servico', 'Est Origem', 'Reserva', 'Valor ADT']]

    df = df.rename(columns={'Est Origem': 'Hotel', 'Valor ADT': 'Valor Final', 'Data Execucao': 'Data da Escala'})

    return df

def zerar_buggys_com_reembolso(df):

    df = pd.merge(df, st.session_state.df_reembolsos[['Id_Servico', 'status']], on='Id_Servico', how='left')

    df = df.rename(columns={'status': 'Reembolso'})

    df['Reembolso'] = df['Reembolso'].apply(lambda x: 'X' if x==2 or x==4 else '')

    df['Valor Final'] = df.apply(lambda row: 0 if row['Reembolso']=='X' else row['Valor Final'], axis=1)

    return df

def gerar_df_pag_catamara_buggy(df):

    df['Servico'] = df['Servico'].apply(lambda x: 'CATAMARAN CARNEIROS' if 'CARNEIROS' in x else 'BUGGY CABO' if x=='PRAIAS DO CABO DE STO AGOSTINHO (PORTO DE GALINHAS)' else x)

    df = pd.merge(df, st.session_state.df_tarifario, on='Servico', how='left')

    df['Valor Final'] = df.apply(lambda row: row['Total ADT']*row['Valor ADT']+row['Total CHD']*row['Valor CHD'] if row['Servico']=='CATAMARAN CARNEIROS' 
                                                 else (row['Total ADT']+row['Total CHD']+row['Total INF'])*row['Valor ADT'] if row['Servico']=='BUGGY CABO' else None, axis=1)

    df = df.drop(columns=['adicional'])

    return df

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

def gerar_df_pag_final(df_pag_buggy_pp, df_escalas_group):

    df_pag_final_forn_add = pd.concat([df_pag_buggy_pp, df_escalas_group], ignore_index=True)

    df_pag_final_forn_add = df_pag_final_forn_add[['Data da Escala', 'Escala', 'Servico', 'Hotel', 'Reserva', 'Total ADT', 'Total CHD', 'Total INF', 'Valor ADT', 'Valor CHD', 'Valor Final', 'Reembolso']]\
        .sort_values(by='Data da Escala').reset_index(drop=True)
    
    verificar_tarifarios(df_pag_final_forn_add, st.session_state.id_gsheet, 'Tarifário Guias', 'Valor Final')

    for coluna in ['Escala', 'Servico', 'Hotel', 'Reserva', 'Reembolso']:

        df_pag_final_forn_add[coluna] = df_pag_final_forn_add[coluna].fillna('')

    for coluna in ['Total ADT', 'Total CHD', 'Total INF', 'Valor ADT', 'Valor CHD', 'Valor Final']:

        df_pag_final_forn_add[coluna] = df_pag_final_forn_add[coluna].fillna(0)

    return df_pag_final_forn_add

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

    with open(nome_html, "w", encoding="utf-8") as file:

        file.write(f'<p style="font-size:40px;">{fornecedor}</p>')

        file.write(f'<p style="font-size:30px;">Serviços prestados entre {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}</p>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

        file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

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

        lista_fornecedores = df_pag_final['Servico'].dropna().unique().tolist()

        fornecedor = st.selectbox('Serviço', sorted(lista_fornecedores), index=None)

    return fornecedor, lista_fornecedores

def plotar_mapa_pagamento(fornecedor, row2_1, df_pag_final):

    df_pag_fornecedor = df_pag_final[df_pag_final['Servico'].isin(fornecedor)].sort_values(by=['Data da Escala']).reset_index(drop=True)

    df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

    container_dataframe = st.container()

    container_dataframe.dataframe(df_pag_fornecedor, hide_index=True, use_container_width = True)

    with row2_1[0]:

        total_a_pagar = df_pag_fornecedor['Valor Final'].sum()

        st.subheader(f'Valor Total: R${int(total_a_pagar)}')

    return total_a_pagar, df_pag_fornecedor

def botao_download_html_individual(total_a_pagar, df_pag_fornecedor, fornecedor, colunas_valores_df_pag):

    soma_servicos = format_currency(total_a_pagar, 'BRL', locale='pt_BR')

    for item in colunas_valores_df_pag:

        df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

    for item in st.session_state.colunas_numeros_inteiros_df_pag_forn_add:

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

        if fornecedor_ref in st.session_state.df_contatos['Fornecedor'].unique().tolist():

            contato_fornecedor = st.session_state.df_contatos.loc[st.session_state.df_contatos['Fornecedor']==fornecedor_ref, 'Contato'].values[0]

            if contato_fornecedor=='':

                lista_fornecedores_contato_nulo.append(fornecedor_ref)

        else:

            lista_fornecedores_sem_contato.append(fornecedor_ref)

        df_pag_fornecedor = df_pag_final[df_pag_final['Servico']==fornecedor_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_fornecedor['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in colunas_valores_df_pag:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        for item in st.session_state.colunas_numeros_inteiros_df_pag_forn_add:

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

        df_itens_faltantes = pd.DataFrame(lista_fornecedores_sem_contato, columns=['Fornecedor'])

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

        df_pag_fornecedor = df_pag_final[df_pag_final['Servico']==fornecedor_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_fornecedor['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in colunas_valores_df_pag:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR') if pd.notna(x) else x)

        for item in st.session_state.colunas_numeros_inteiros_df_pag_forn_add:

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

        df_pag_fornecedor = df_pag_final[df_pag_final['Servico']==fornecedor_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_fornecedor['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in colunas_valores_df_pag:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        for item in st.session_state.colunas_numeros_inteiros_df_pag_forn_add:

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

def gerar_listas_fornecedores_sem_contato(fornecedor):

    lista_fornecedores_sem_contato = []

    lista_fornecedores_contato_nulo = []

    if fornecedor in st.session_state.df_contatos['Fornecedor'].unique().tolist():

        contato_fornecedor = st.session_state.df_contatos.loc[st.session_state.df_contatos['Fornecedor']==fornecedor, 'Contato'].values[0]

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

def calcular_valor_final(df_escalas_group):

    df_pag_fornecedores = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

    df_pag_fornecedores['Valor Final'] = (df_pag_fornecedores['Total ADT'] * df_pag_fornecedores['Valor ADT']) + (df_pag_fornecedores['Total CHD'] * df_pag_fornecedores['Valor CHD'])

    df_pag_fornecedores['Servico'] = df_pag_fornecedores['Servico'].replace({'Passeio à Perobas - Touros ': 'Passeio à Perobas', 'Passeio à Maracajaú - Touros': 'Passeio à Maracajaú'})

    return df_pag_fornecedores

st.set_page_config(layout='wide')

if st.session_state.base_luck in ['test_phoenix_recife', 'test_phoenix_natal']:

    if not 'df_escalas' in st.session_state or st.session_state.view_phoenix!='vw_pagamento_fornecedores_adicionais':

        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix()

    st.title('Mapa de Pagamento - Fornecedores *(Adicional)*')

    st.divider()

    row1 = st.columns(2)

    # Container de datas e botão de gerar mapa

    with row1[0]:

        container_datas = st.container(border=True)

        container_datas.subheader('Período')

        data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

        data_final = container_datas.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

        if st.session_state.base_luck=='test_phoenix_joao_pessoa':

            tipo_de_mapa = container_datas.multiselect('Gerar Mapas de Buggy, 4x4 e Polo', ['Sim'], default=None)

        else:

            tipo_de_mapa = []

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

            with st.spinner('Puxando configurações, tarifários...'):

                puxar_configuracoes()

                puxar_tarifario_fornecedores()

            with st.spinner('Gerando mapas de pagamentos...'):

                df_escalas_group, df_router = gerar_escalas_agrupadas(data_inicial, data_final)

                df_pag_buggy_pp = gerar_df_pag_buggy_pp(df_router)

                df_pag_buggy_pp = zerar_buggys_com_reembolso(df_pag_buggy_pp)

                df_escalas_group = gerar_df_pag_catamara_buggy(df_escalas_group)

                st.session_state.df_pag_final_forn_add = gerar_df_pag_final(df_pag_buggy_pp, df_escalas_group)

        # Base NAT

        if st.session_state.base_luck == 'test_phoenix_natal':

            with st.spinner('Puxando configurações, tarifários...'):

                puxar_configuracoes()

                puxar_tarifario_fornecedores()

            with st.spinner('Gerando mapas de pagamentos...'):

                df_escalas_group = gerar_escalas_agrupadas(data_inicial, data_final)

                df_pag_fornecedores = calcular_valor_final(df_escalas_group)

                verificar_tarifarios(df_pag_fornecedores, st.session_state.id_gsheet, 'Tarifário Guias', 'Valor Final')

                st.session_state.df_pag_final_forn_add = df_pag_fornecedores[['Data da Escala', 'Escala', 'Servico', 'Total ADT', 'Total CHD', 'Valor ADT', 'Valor CHD', 'Valor Final']]

    # Opção de salvar o mapa gerado no Gsheet

    if 'df_pag_final_forn_add' in st.session_state and len(tipo_de_mapa)==0:

        with row1_2[1]:

            salvar_mapa = st.button('Salvar Mapa de Pagamentos Fornecedores (Adicional)')

        if salvar_mapa and data_inicial and data_final:

            with st.spinner('Salvando mapa de pagamentos...'):

                df_insercao = gerar_df_insercao_mapa_pagamento(data_inicial, data_final, st.session_state.df_pag_final_forn_add, 'Histórico de Pagamentos Fornecedores (Adicional)')

                inserir_dataframe_gsheet(df_insercao, st.session_state.id_gsheet, 'Histórico de Pagamentos Fornecedores (Adicional)')

    # Gerar Mapas

    if 'df_pag_final_forn_add' in st.session_state:

        # Se tiver gerando o mapa normal

        if len(tipo_de_mapa)==0:

            df_pag_final_ref = st.session_state.df_pag_final_forn_add

            colunas_valores_df_pag_ref = st.session_state.colunas_valores_df_pag_forn_add

            st.header('Gerar Mapas')

            row2 = st.columns(2)

            # Caixa de seleção de fornecedor

            fornecedor, lista_fornecedores = selecionar_fornecedor_do_mapa(row2, df_pag_final_ref)

            # Quando seleciona o fornecedor

            if fornecedor and data_pagamento and data_inicial and data_final:

                row2_1 = st.columns(4)

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

    if 'html_content' in st.session_state and fornecedor and data_pagamento:

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

else:

    st.error('Esse painel funciona apenas p/ as bases de Recife e Natal.')
