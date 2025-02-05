import streamlit as st
import pandas as pd
import gspread
from google.oauth2 import service_account
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

def criar_output_html(nome_html, html, guia, soma_servicos):

    with open(nome_html, "w", encoding="utf-8") as file:

        file.write(f'<p style="font-size:40px;">{guia}</p>')

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

def selecionar_guia_do_mapa(row2):

    with row2[0]:

        if 'Excluir Guias' in st.session_state.df_config['Configuração'].unique():

            lista_guias = st.session_state.df_pag_final_historico[~st.session_state.df_pag_final_historico['Guia']\
                .str.contains(st.session_state.df_config[st.session_state.df_config['Configuração']=='Excluir Guias']['Parâmetro'].iloc[0])]['Guia'].dropna().unique().tolist()

        else:

            lista_guias = st.session_state.df_pag_final_historico['Guia'].dropna().unique().tolist()

        guia = st.selectbox('Guia', sorted(lista_guias), index=None)

    return guia, lista_guias

def identificar_cnpj_razao_social(guia):

    st.session_state.cnpj = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Guia']==guia]['CNPJ/CPF Fornecedor Guia'].iloc[-1]

    st.session_state.razao_social = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Guia']==guia]['Razao Social/Nome Completo Fornecedor Guia'].iloc[-1]

def plotar_mapa_pagamento(guia, row2_1):

    df_pag_guia = st.session_state.df_pag_final_historico[st.session_state.df_pag_final_historico['Guia']==guia].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista']).reset_index(drop=True)

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

        df_pag_guia = st.session_state.df_pag_final_historico[st.session_state.df_pag_final_historico['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista']).reset_index(drop=True)

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

def inserir_html(nome_html, html, guia, soma_servicos):

    with open(nome_html, "a", encoding="utf-8") as file:

        file.write('<div style="page-break-before: always;"></div>\n')

        file.write(f'<p style="font-size:40px;">{guia}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:40px;">O valor total dos serviços é {soma_servicos}</p>')

def gerar_html_mapa_guias_geral(lista_guias):

    for guia_ref in lista_guias:

        df_pag_guia = st.session_state.df_pag_final_historico[st.session_state.df_pag_final_historico['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista']).reset_index(drop=True)

        df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

        soma_servicos = format_currency(df_pag_guia['Valor Final'].sum(), 'BRL', locale='pt_BR')

        for item in st.session_state.colunas_valores_df_pag:

            df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_guia)

        inserir_html(nome_html, html, guia_ref, soma_servicos)

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

def gerar_payload_envio_geral_para_financeiro(lista_guias):

    lista_htmls = []

    lista_htmls_email = []

    contato_financeiro = st.session_state.df_config[st.session_state.df_config['Configuração']=='Contato Financeiro']['Parâmetro'].iloc[0]

    for guia_ref in lista_guias:

        identificar_cnpj_razao_social(guia_ref)

        df_pag_guia = st.session_state.df_pag_final_historico[st.session_state.df_pag_final_historico['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista']).reset_index(drop=True)

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

def enviar_email_individual(contato_guia):

    assunto = f'Mapa de Pagamento {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}'

    enviar_email_gmail([contato_guia], assunto, st.session_state.html_content, st.session_state.remetente_email, st.session_state.senha_email)

st.set_page_config(layout='wide')

# Título da página

st.title('Mapa de Pagamento - Guias *(Histórico)*')

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

        gerar_mapa = container_datas.button('Gerar Mapa de Pagamentos')

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

        puxar_aba_simples(st.session_state.id_gsheet, 'Histórico de Pagamentos Guias', 'df_historico_pagamentos')

        st.session_state.df_historico_pagamentos['Data da Escala'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data da Escala'], format='%d/%m/%Y').dt.date

        for coluna in st.session_state.colunas_valores_df_pag:

            st.session_state.df_historico_pagamentos[coluna] = (st.session_state.df_historico_pagamentos[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

            st.session_state.df_historico_pagamentos[coluna] = pd.to_numeric(st.session_state.df_historico_pagamentos[coluna])

        st.session_state.df_pag_final_historico = st.session_state.df_historico_pagamentos[(st.session_state.df_historico_pagamentos['Data da Escala'] >= data_inicial) & 
                                                                                           (st.session_state.df_historico_pagamentos['Data da Escala'] <= data_final)].reset_index(drop=True)

# Gerar Mapas

if 'df_pag_final_historico' in st.session_state:

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

if 'html_content' in st.session_state and 'df_pag_final_historico' in st.session_state and guia and data_pagamento:

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
