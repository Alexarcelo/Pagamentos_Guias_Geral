import pandas as pd
import mysql.connector
import decimal
import streamlit as st

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

def definir_html(df_ref):

    html=df_ref.to_html(index=False)

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

def criar_output_html(nome_html, html, valor_total):

    with open(nome_html, "w", encoding="utf-8") as file:

        file.write(f'<p style="font-size:40px;">Mapa de Pagamento - {motorista} - R${valor_total}</p>\n\n')
        
        file.write(html)

def transformar_em_string(serie_dados):

    return ' + '.join(list(set(serie_dados.dropna())))

st.set_page_config(layout='wide')

if st.session_state.base_luck=='test_phoenix_natal':

    if 'df_escalas' not in st.session_state:

        with st.spinner('Puxando dados do Phoenix...'):

            st.session_state.df_escalas = gerar_df_phoenix('vw_motoristas_out', st.session_state.base_luck)

    st.title('Mapa de Pagamento - Motoristas Terceirizados s/ Guia no OUT')

    st.divider()

    row0 = st.columns(2)

    with row0[1]:

        container_dados = st.container()

        atualizar_dados = container_dados.button('Carregar Dados do Phoenix', use_container_width=True)

    if atualizar_dados:

        with st.spinner('Puxando dados do Phoenix...'):

            st.session_state.df_escalas = gerar_df_phoenix('vw_motoristas_out', st.session_state.base_luck)

    with row0[0]:

        data_inicial = st.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

        data_final = st.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

    if data_inicial and data_final:

        df_escalas_filtrado = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & 
                                                          (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)
        

        
        lista_motoristas = df_escalas_filtrado['Motorista'].unique().tolist()

        with row0[1]:

            motorista = st.selectbox('Motoristas', sorted(lista_motoristas), index=None)

        if motorista:

            st.divider()

            df_escalas_motorista = df_escalas_filtrado[(df_escalas_filtrado['Motorista']==motorista)].groupby(['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Servico'], as_index=False)\
                .agg({'Voo': transformar_em_string})
            
            container_motorista = st.container()

            container_motorista.dataframe(df_escalas_motorista[['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Servico', 'Voo']].sort_values(by='Data da Escala'), hide_index=True, 
                                          use_container_width=True)
            
            nome_html = f'{motorista}.html'

            valor_total = len(df_escalas_motorista)*9

            html = definir_html(df_escalas_motorista)

            criar_output_html(nome_html, html, valor_total)
            
            with row0[1]:

                st.subheader(f'Valor à pagar = R${valor_total}')

            with open(nome_html, "r", encoding="utf-8") as file:

                html_content = file.read()

            st.download_button(
                label="Baixar Arquivo HTML",
                data=html_content,
                file_name=nome_html,
                mime="text/html"
            )

else:

    st.error('Esse painel funciona apenas p/ a base de Natal.')
