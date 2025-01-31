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

        st.session_state.cnpj = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista']==fornecedor]['CNPJ/CPF Fornecedor Motorista'].iloc[0]

        st.session_state.razao_social = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista']==fornecedor]\
            ['Razao Social/Nome Completo Fornecedor Motorista'].iloc[0]

def plotar_mapa_pagamento(fornecedor, row2_1, df_pag_final):

    if st.session_state.base_luck=='test_phoenix_noronha':

        df_pag_fornecedor = df_pag_final[df_pag_final['Servico'].isin(fornecedor)].sort_values(by=['Data da Escala']).reset_index(drop=True)

    else:

        df_pag_fornecedor = df_pag_final[df_pag_final['Fornecedor Motorista']==fornecedor].sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

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

def gerar_payload_envio_geral(lista_fornecedores, df_pag_final, colunas_valores_df_pag):

    lista_htmls = []

    lista_htmls_email = []

    lista_fornecedores_sem_contato = []

    lista_fornecedores_contato_nulo = []

    for fornecedor_ref in lista_fornecedores:

        if 'CARRO' in fornecedor_ref and st.session_state.base_luck=='test_phoenix_noronha':

            fornecedor_ref = fornecedor_ref.split(' - ')[0]

        if fornecedor_ref in st.session_state.df_contatos['Fornecedor'].unique().tolist():

            contato_fornecedor = st.session_state.df_contatos.loc[st.session_state.df_contatos['Fornecedor']==fornecedor_ref, 'Contato'].values[0]

            if contato_fornecedor=='':

                lista_fornecedores_contato_nulo.append(fornecedor_ref)

        else:

            lista_fornecedores_sem_contato.append(fornecedor_ref)

        identificar_cnpj_razao_social(fornecedor_ref)

        if st.session_state.base_luck=='test_phoenix_noronha':

            df_pag_fornecedor = df_pag_final[df_pag_final['Servico']==fornecedor_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

        else:

            df_pag_fornecedor = df_pag_final[df_pag_final['Fornecedor']==fornecedor_ref].sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

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

        df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_fornecedor['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in colunas_valores_df_pag:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR') if pd.notna(x) else x)

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

        df_pag_fornecedor = df_pag_final[df_pag_final['Fornecedor Motorista']==fornecedor_ref].sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

        df_pag_fornecedor['Data da Escala'] = pd.to_datetime(df_pag_fornecedor['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_fornecedor['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in colunas_valores_df_pag:

            df_pag_fornecedor[item] = df_pag_fornecedor[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

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

def enviar_informes_individuais(contato_guia):
        
    payload = {"informe_html": st.session_state.html_content, 
                "telefone": contato_guia}
    
    response = requests.post(st.session_state.id_webhook, json=payload)
        
    if response.status_code == 200:
        
        st.success(f"Mapas de Pagamento enviados pelo Whatsapp com sucesso!")
        
    else:
        
        st.error(f"Erro. Favor contactar o suporte")

        st.error(f"{response}")   

def enviar_email_individual(contato_guia):

    assunto = f'Mapa de Pagamento {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}'

    enviar_email_gmail([contato_guia], assunto, st.session_state.html_content, st.session_state.remetente_email, st.session_state.senha_email)

st.set_page_config(layout='wide')

# Título da página

st.title('Mapa de Pagamento - Fornecedores *(Histórico)*')

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

    container_data_pgto = st.container(border=True)

    container_data_pgto.subheader('Data de Pagamento')

    data_pagamento = container_data_pgto.date_input('Data de Pagamento', value=None ,format='DD/MM/YYYY', key='data_pagamento')

    if not data_pagamento:

        st.warning('Preencha a data de pagamento para visualizar os mapas de pagamentos.')

st.divider()

# Geração de dataframe com os mapas de pagamentos

if gerar_mapa and data_inicial and data_final:

    with st.spinner('Gerando mapas de pagamentos...'):

        if len(tipo_de_mapa)>0 and st.session_state.base_luck=='test_phoenix_joao_pessoa':

            puxar_aba_simples(st.session_state.id_gsheet, 'Histórico de Pagamentos Buggy e 4x4', 'df_historico_pagamentos')

            colunas_valores_df_pag_ref = st.session_state.colunas_valores_df_pag_buggy_4x4

        else:

            puxar_aba_simples(st.session_state.id_gsheet, 'Histórico de Pagamentos Fornecedores', 'df_historico_pagamentos')

            colunas_valores_df_pag_ref = st.session_state.colunas_valores_df_pag_forn

        st.session_state.df_historico_pagamentos['Data da Escala'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data da Escala'], format='%d/%m/%Y').dt.date

        for coluna in colunas_valores_df_pag_ref:

            st.session_state.df_historico_pagamentos[coluna] = (st.session_state.df_historico_pagamentos[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

            st.session_state.df_historico_pagamentos[coluna] = pd.to_numeric(st.session_state.df_historico_pagamentos[coluna])

        st.session_state.df_pag_final_historico = st.session_state.df_historico_pagamentos[(st.session_state.df_historico_pagamentos['Data da Escala'] >= data_inicial) & 
                                                                                           (st.session_state.df_historico_pagamentos['Data da Escala'] <= data_final)].reset_index(drop=True)

# Gerar Mapas

if 'df_pag_final_forn' in st.session_state or 'df_pag_final_forn_bg_4x4' in st.session_state:

    # Se tiver gerando o mapa normal

    if len(tipo_de_mapa)==0:

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
