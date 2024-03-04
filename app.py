import streamlit as st
import requests
from requests.exceptions import RequestException
from exceptions import ErroServidor, ErroParametroPesquisaRecebimento, ErroVigenciaInexistente, ErroParametroProtocoloInvalido
from datetime import datetime
import configparser
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
import cx_Oracle
import tempfile
import os
from glob import glob
import time
from random import randint
import platform
import warnings
from PIL import Image

warnings.filterwarnings('ignore')

sBarraDelimitadora = '\\' if platform.system() == 'Windows' else '/'

surl_login      = os.getenv('URL_LOGIN')
surl_consulta   = os.getenv('URL_CONSULTA')
sUsuario        = os.getenv('USER_CNJ')
sSenha          = os.getenv('PASSWORD_CNJ') 

## Data Base
DB_ORACLE_SERVER        = os.getenv('DB_ORACLE_SERVER')
DB_ORACLE_SERVER_PORT   = os.getenv('DB_ORACLE_SERVER_PORT')
DB_ORACLE_SERVICE_NAME  = os.getenv('DB_ORACLE_SERVICE_NAME')
DB_ORACLE_USER          = os.getenv('DB_ORACLE_USER')
DB_ORACLE_PASSWORD      = os.getenv('DB_ORACLE_PASSWORD')

DB_ORACLEDW_SERVER          = os.getenv('DB_ORACLEDW_SERVER')
DB_ORACLEDW_SERVER_PORT     = os.getenv('DB_ORACLEDW_SERVER_PORT')
DB_ORACLEDW_SERVICE_NAME    = os.getenv('DB_ORACLEDW_SERVICE_NAME')
DB_ORACLEDW_USER            = os.getenv('DB_ORACLEDW_USER')
DB_ORACLEDW_PASSWORD        = os.getenv('DB_ORACLEDW_PASSWORD')

dsn_tns = cx_Oracle.makedsn(DB_ORACLE_SERVER, DB_ORACLE_SERVER_PORT, service_name=DB_ORACLE_SERVICE_NAME) # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
cnxn    = cx_Oracle.connect(user=DB_ORACLE_USER, password=DB_ORACLE_PASSWORD, dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
cursor  = cnxn.cursor()

dsn_tnsDW = cx_Oracle.makedsn(DB_ORACLEDW_SERVER, DB_ORACLEDW_SERVER_PORT, service_name=DB_ORACLEDW_SERVICE_NAME) # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
cnxnDW    = cx_Oracle.connect(user=DB_ORACLEDW_USER, password=DB_ORACLEDW_PASSWORD, dsn=dsn_tnsDW) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
cursorDW  = cnxnDW.cursor()

sQryVigencias      = """
                        SELECT  '2999-12-12' AS "VIGENCIA"
                               ,0 AS "TIPO"
                               ,' ' AS "OBSERVACAO"
                               ,'Nenhuma' AS "COMPLETO"
                         FROM DUAL
                        UNION ALL        
                        SELECT  TO_CHAR(VIGENCIA,'YYYY-MM-DD') AS "VIGENCIA"
                               ,FL_CARGA_CORRETIVA AS "TIPO"
                               ,DS_TXT_OBSERVACAO AS "OBSERVACAO"
                               ,TO_CHAR(VIGENCIA,'YYYY-MM-DD') || ' - ' || 
                                 CASE WHEN FL_CARGA_CORRETIVA = 1 THEN ' (CORRETIVA)'
                                      ELSE ' (ORDINARIA)'
                                 END || 
                                 CASE WHEN LENGTH(DS_TXT_OBSERVACAO)>0 THEN ': ' ELSE '' END || DS_TXT_OBSERVACAO AS "COMPLETO"
                          FROM DWTJPE.SELO_VIGENCIA_ATIVA
                         WHERE VIGENCIA >= (SELECT ADD_MONTHS(SYSDATE, -25) FROM DUAL)
                           AND FL_STATUS_ENVIADO = 1
                          ORDER BY VIGENCIA DESC
                    """

sQryProtocolos = """
                    SELECT a.ARQ_DT_VIGENCIA, e.ENV_NM_PROTOCOLO 
                      FROM dbselojn01.ENVIO e, dbselojn01.ARQUIVO a 
                     WHERE a.ARQ_ID = e.ARQ_ID 
                       AND a.ARQ_DT_VIGENCIA = TO_DATE(':pCompetencia','YYYY-MM-DD')
                """

# Dados da requisicao para o DATAJUD
payload          = json.dumps({"Accept": "application/json, text/plain, */*",
                               "Accept-Encoding": "gzip, deflate, br",
                               "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                               "Connection": "keep-alive",
                               "Content-Type": "application/json",
                               "Cookie": "<cookie>",
                               "Host": "replicacao.cnj.jus.br",
                               "Referer": "https://replicacao.cnj.jus.br/",
                               "Sec-Fetch-Dest": "empty",
                               "Sec-Fetch-Mode": "cors",
                               "Sec-Fetch-Site": "same-origin",
                               "Sec-GPC": "1",
                               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"})

lColunasRetorno  = ['seqProtocolo', 'numProtocolo', 'codHash', 'tipStatusProtocolo', 'datDataEnvioProtocolo', 'codIpEnvio', 'qtdProcessosLote',
                    'qtdProcessosSucesso', 'qtdProcessosErro', 'siglaOrgao', 'grau', 'tamanhoArquivo', 'urlArquivo', 'flgExcluido']
dStatusProcesso  = {'1':'Aguardando processamento', '3':'Processado com sucesso', '5':'Duplicado', '7':'Erro no arquivo'}

def ajustarData(nTimeStamp):
    dtObj = datetime.fromtimestamp(nTimeStamp / 1e3)
    return dtObj

def obterDadosCompetencia(sCompetencia:str):
    # Consulta as últimas vigências do DW
    sQryDadosVigencia = """
                        SELECT TO_CHAR(a.ARQ_DT_VIGENCIA,'YYYY-MM-DD') AS "ARQ_DT_VIGENCIA", 
                               TO_CHAR(MIN(e.ENV_DH_ENVIO),'YYYY-MM-DD') AS "INICIO" , 
                               TO_CHAR(MAX(e.ENV_DH_ENVIO),'YYYY-MM-DD') AS "FIM"
                          FROM dbselojn01.ENVIO e, dbselojn01.ARQUIVO a 
                         WHERE a.ARQ_ID = e.ARQ_ID 
                           AND a.ARQ_DT_VIGENCIA = TO_DATE(':paramCompetencia','YYYY-MM-DD')
                         GROUP BY a.ARQ_DT_VIGENCIA 
                         ORDER BY a.ARQ_DT_VIGENCIA DESC 
                    """
    dfRetorno = pd.read_sql(sQryDadosVigencia.replace(':paramCompetencia',sCompetencia), cnxn)
    
    if len(dfRetorno)==0:
        raise ErroVigenciaInexistente

    print(len(dfRetorno))
    return dfRetorno.iloc[0]['INICIO'], dfRetorno.iloc[0]['FIM']

def obterRetorno(sFiltro:str='', sProtocolo:str = '', sCompetencia:str=''):
    sParamProtocolo  = f'protocolo={sProtocolo}'
    sParamDataInicio = 'dataInicio='
    sParamDataFim    = 'dataFim='
    sParamPagina     = 'page=<page>'
    sParamConector   = '&'
    nTotalPaginacao  = 40

    if sProtocolo == '' and sCompetencia == 'Nenhuma':
        raise ErroParametroPesquisaRecebimento
    if sProtocolo != '' and (len(sProtocolo)<30 or sProtocolo[0:4] != 'TJPE'):
        raise ErroParametroProtocoloInvalido
    try:
        if sCompetencia != 'Nenhuma':
            sDataIni, sDataFim = obterDadosCompetencia(sCompetencia[0:10])
            sParamDataInicio   = f'dataInicio={sDataIni}'
            sParamDataFim      = f'dataFim={sDataFim}'
            
            # Obtem a lista de protocolos a serem filtrados
            dfProtocolos = pd.read_sql(sQryProtocolos.replace(':pCompetencia',sCompetencia[0:10]), cnxn)
            lProtocolos  = list(dfProtocolos['ENV_NM_PROTOCOLO'])
        else:
            lProtocolos  = [sProtocolo]

        # Abrir a página
        a_session = requests.Session()
        a_session.get(surl_login)
        session_cookies     = a_session.cookies
        cookies_dictionary  = session_cookies.get_dict()

        print(lProtocolos)
        
        # Realizar o login
        print(surl_consulta+sParamProtocolo + sParamConector + sParamDataInicio + sParamConector + sParamDataFim + sParamConector + sParamPagina.replace('<page>','1'))
        payload_login   = payload.replace('<cookie>',list(cookies_dictionary.keys())[0] + '='+list(cookies_dictionary.values())[0])
        response        = requests.get(surl_consulta+sParamProtocolo + sParamConector + sParamDataInicio + sParamConector + sParamDataFim + sParamConector + sParamPagina.replace('<page>','1'),
                                    auth = HTTPBasicAuth(sUsuario, sSenha), data=payload_login)

        data            = json.loads(response.text)
        df_retorno      = pd.json_normalize(data, 'resultado')
        nTotalRegistros = data.get('totalRegistros')
        nTotalIteracoes = int(nTotalRegistros / nTotalPaginacao) + (1 if (nTotalRegistros % nTotalPaginacao) > 0 else 0)
        print(nTotalIteracoes)

        # Só inclui os protocolos que realmente foram enviados no período
        lDataFrames = []
        # print(lProtocolos)
        print('iniciou iteracao 1')
        for index, row in df_retorno.iterrows():
            print(row['numProtocolo'])
            if row['numProtocolo'].strip() in lProtocolos:
                lProtocolos.remove(row['numProtocolo'].strip())
                lDataFrames.append(df_retorno.query(f"numProtocolo == '{row['numProtocolo'].strip()}'").copy())

        print(f'terminou iteracao 1,  {len(lProtocolos)}')

        if sProtocolo == '':    
            for item in range(2, nTotalIteracoes+1):
                # consulta as demais páginas
                payload_login   = payload.replace('<cookie>',list(cookies_dictionary.keys())[0] + '='+list(cookies_dictionary.values())[0])
                response        = requests.get(surl_consulta+sParamProtocolo + sParamConector + sParamDataInicio + sParamConector + sParamDataFim + sParamConector + sParamPagina.replace('<page>',str(item)),
                                            auth = HTTPBasicAuth(sUsuario, sSenha), data=payload_login)

                data            = json.loads(response.text)
                df_retorno      = pd.json_normalize(data, 'resultado')
                
                print(f'iniciou iteracao {item}')
                for index, row in df_retorno.iterrows():
                    print(row['numProtocolo'])
                    if row['numProtocolo'].strip() in lProtocolos:
                        lProtocolos.remove(row['numProtocolo'].strip())
                        lDataFrames.append(df_retorno.query(f"numProtocolo == '{row['numProtocolo'].strip()}'").copy())
                print(f'terminou iteracao {item}, {len(lProtocolos)}')
        df_RetornoFinal = pd.DataFrame()
        if len(lDataFrames)>0:
            df_RetornoFinal         = pd.concat(lDataFrames, axis=0)
            df_RetornoFinal['data'] = df_RetornoFinal['datDataEnvioProtocolo'].apply(ajustarData)
            df_RetornoFinal['descStatusProtocolo'] = ''
            df_RetornoFinal.loc[df_RetornoFinal['tipStatusProtocolo'] == 1,'descStatusProtocolo'] = 'Aguardando processamento'
            df_RetornoFinal.loc[df_RetornoFinal['tipStatusProtocolo'] == 3,'descStatusProtocolo'] = 'Processado com sucesso'
            df_RetornoFinal.loc[df_RetornoFinal['tipStatusProtocolo'] == 4,'descStatusProtocolo'] = 'Enviado'
            df_RetornoFinal.loc[df_RetornoFinal['tipStatusProtocolo'] == 5,'descStatusProtocolo'] = 'Duplicado'
            df_RetornoFinal.loc[df_RetornoFinal['tipStatusProtocolo'] == 6,'descStatusProtocolo'] = 'Processado com erro'
            df_RetornoFinal.loc[df_RetornoFinal['tipStatusProtocolo'] == 7,'descStatusProtocolo'] = 'Erro no arquivo'
            df_RetornoFinal.loc[df_RetornoFinal['tipStatusProtocolo'] == 8,'descStatusProtocolo'] = 'Erro ao gravar no AWS'

        print(len(df_RetornoFinal))
        return df_RetornoFinal.query(sFiltro) if len(sFiltro)>0 else df_RetornoFinal
    except RequestException as error:
        raise ErroServidor

def gravarPrtocolocos(sProtocolo:str, sGrau:str, sData:str, sPathDir:str = ''):
    sPath = sPathDir if len(sPathDir)>0 else 'data'
    sQry = f"""
                SELECT a.ARQ_TX_CONTEUDO
                  FROM DBSELOJN01.ARQUIVO a 
                       INNER JOIN DBSELOJN01.ENVIO e ON e.ARQ_ID = a.ARQ_ID 
                 WHERE 1=1
                   AND e.ENV_NM_PROTOCOLO in ('{sProtocolo}') 
                 ORDER BY e.ENV_DH_ENVIO
            """
    cursor.execute(sQry)
    blob,              = cursor.fetchone()
    offset             = 1
    num_bytes_in_chunk = 65536
    with open(sPath+f"/{sGrau}_{sData}_{sProtocolo}.xml", "w", encoding='utf-8') as f:
      while True:
          data = blob.read(offset, num_bytes_in_chunk)
          if data:
              f.write(data)
          if len(data) < num_bytes_in_chunk:
              break
          offset += len(data)
      f.close()

def obterEnvios():
    # Consulta as últimas vigências do DW
    sQryLimitesVigencia = """
                        SELECT TO_CHAR(a.ARQ_DT_VIGENCIA,'YYYY-MM-DD') AS "ARQ_DT_VIGENCIA", 
                               TO_CHAR(MIN(e.ENV_DH_ENVIO),'DD/MM/YYYY') AS "INICIO" , 
                               TO_CHAR(MAX(e.ENV_DH_ENVIO)+1,'DD/MM/YYYY') AS "FIM"
                          FROM dbselojn01.ENVIO e, dbselojn01.ARQUIVO a 
                         WHERE a.ARQ_ID = e.ARQ_ID 
                           AND a.ARQ_DT_VIGENCIA >= (SELECT ADD_MONTHS(SYSDATE, -25) FROM DUAL)
                         GROUP BY a.ARQ_DT_VIGENCIA 
                         ORDER BY a.ARQ_DT_VIGENCIA DESC 
                    """
    dfRetorno = pd.read_sql(sQryLimitesVigencia, cnxn)
    return dfRetorno

def obterProtocolo(sProtocolo: str, sGrau: str, sData:str, sPathDir:str=''):
    gravarPrtocolocos(sProtocolo, sGrau, sData, sPathDir)

def analisarProtocolos(arquivo: str):
    try:
        url       = "https://validador.stg.cloud.cnj.jus.br/v1/valida"
        payload   = {}
        headers   = {
                    'Authorization': 'Basic VEpQRTphOTMyNDc2YWVjOTlhNzgzY2IwNjZmNTc2NTk0M2UxOQ==',
                    'Cookie': 'INGRESSCOOKIE=cacd5413f7e3fa547a3097fbb762431c|4aeb3a373a6d189be43ae46633e65476'
                    }

        files         = [('arquivo',(arquivo.split('\\')[-1],open(arquivo,'rb'),'text/xml'))]
        response      = requests.request("POST", url, headers=headers, data=payload, files=files)
        sArquivo      = arquivo.split(sBarraDelimitadora)[-1]
        sGrau         = sArquivo.split('_')[0]
        sData         = sArquivo.split('_')[1]
        dMensagemErro = {"Protocolo" : sArquivo[12:],
                        "Grau"       : sGrau,
                        "Data"       : sData, 
                        "Mensagem"   : ' '.join(response.text[0:response.text.find('br.jus.cnj.selointegracao.resources')].split('lineNumber: 1;')[1:]).replace('"','').replace("'","").replace('{','').replace('}','')} #  response.text[0:response.text.find('\n\tat br.jus.cnj.selointegracao.resources.')].replace("'","").replace('"','')}
                        # "Mensagem"  : response.text[0:response.text.find('\n\tat br.jus.cnj.selointegracao.resources.')].replace("'","").replace('"','')}

        return dMensagemErro
    except requests.exceptions.ConnectionError:
        return "Erro de conexão"
    

def apagarProtocolos(sTempDir:str = ''):
    try:
        # Apagar os arquivos
        sPath = sTempDir if len(sTempDir)>0 else 'data'
        lArquivos = glob(sPath + sBarraDelimitadora +'*.xml')
        for arquivo in lArquivos:
            os.remove(arquivo)
    except OSError as e:
        print(f"Error:{ e.strerror}")

st.set_page_config(layout="wide")

imgLogoDataJud = Image.open("imagens"+sBarraDelimitadora+"DataJud.png")
imgLogoTJPE    = Image.open("imagens"+sBarraDelimitadora+"TJPE2.jfif")
imgLogoTJPE    = imgLogoTJPE.resize((100, 50))
with st.sidebar: 
    st.image(imgLogoDataJud)
    st.header("Ferramentas DATAJUD")
    st.caption("versão 1.1")
    with st.expander('SOBRE'): 
        st.markdown("Aplicativo que tem por finalidade disponibilizar algumas ferramentas para facilitar o tratamento das informações enviadas e recebidas pelo DATAJUD.")
        st.markdown('''
    | Versão |    Data    | Descrição                                                                                                                                                                  |
|--------|:----------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|   1.0  | 12/09/2023 | Versão inicial                                                                                                                                                             |
|   1.1  | 13/09/2023 | **IDENTIFICAR ERROS DE RECEBIMENTO:** Mudança nos parâmetros; Obtenção dos erros via protocolo de recebimento ao invés do período de envio. |'''
                    )
    st.subheader('Funcionalidades:')
    with st.expander("IDENTIFICAR ERROS NO RECEBIMENTO"):
        st.markdown("""
             O objetivo desta fucnionalidade é verificar os erros de recebimento dos arquivos enviados ao DATAJUD num determinado período de tempo.  Passo a passo:  
1. Consultar no DATAJUD o status dos arquivos enviados no período de tempo informado.  
2. Caso exista arquivos com erro:  
2.1. Obter o número do protocolo de envio no DATAJUD  
2.2. Consultar o protocolo no banco de dados e obter o conteúdo do arquivo XML enviado ao DATAJUD  
2.3. Submeter o conteúdo do arquivo ao VALIDADOR DO DATAJUD  
2.4. Analisar as críticas e apresentar o erro identificado.  
 Informar caso não exista arquivos enviados no período;  
 Informar resumo do status quando existirem arquivos enviados mas sem erro no recebimento.""")  
    # st.subheader('Próximas funcionalidades:')
    with st.expander("CARREGAR RETORNO PARA PAINEL"):
        st.markdown('Carregar os dados dos retornos para o painel do DATAJUD.')
    col1, col2 = st.columns(2)
    col2.image(imgLogoTJPE)

with st.status('DATAJUD - IDENTIFICAR ERROS DE RECEBIMENTO', expanded=True) as status1:
    dfCompetencias   = pd.read_sql(sQryVigencias, cnxnDW)
    col1, col2, col3 = st.columns(3)
    # dtInicio        = col1.date_input('Informe a data de início', format="DD/MM/YYYY")
    txProtocolo     = col1.text_input('Informe o protocolo de recebimento', placeholder='TJPE43952202308311693443695335')
    dtCompetencia   = col2.selectbox('Informe a competência', dfCompetencias['COMPLETO'])
    btnConsultar    = col3.button('Consultar erros')
    
    if not st.session_state.get('btnConsultar'):
        st.session_state['btnConsultar'] = btnConsultar

    if st.session_state['btnConsultar']:
        st.write("Consultando dados no DATAJUD ...") 
        # st.info("Consultando dados no DATAJUD ...", icon="ℹ️") 
        try:
            dfRetorno     = obterRetorno("", txProtocolo, dtCompetencia)
            if len(dfRetorno) > 0:
                nQtdAguardandoProcessamento = len(dfRetorno.query("descStatusProtocolo == 'Aguardando processamento'")) 
                nQtdProcessadoComSucesso    = len(dfRetorno.query("descStatusProtocolo == 'Processado com sucesso'"))
                nQtdEnviado                 = len(dfRetorno.query("descStatusProtocolo == 'Enviado'"))
                nQtdDuplicado               = len(dfRetorno.query("descStatusProtocolo == 'Duplicado'"))
                nQtdProcessadoComErro       = len(dfRetorno.query("descStatusProtocolo == 'Processado com erro'"))
                nQtdErroNoArquivo           = len(dfRetorno.query("descStatusProtocolo == 'Erro no arquivo'"))
                nQtdErroAoGravarNoAWS       = len(dfRetorno.query("descStatusProtocolo == 'Erro ao gravar no AWS'"))
                dfRetorno                   = dfRetorno.query("descStatusProtocolo == 'Erro no arquivo'")
                
                st.header('Resultado da consulta')
                st.subheader('Status gerais', divider='gray')
                colM1, colM2, colM3, colM4 = st.columns(4)    
                colM1.metric("Processado com sucesso", value=nQtdProcessadoComSucesso)
                colM2.metric("Aguard. processamento", value=nQtdAguardandoProcessamento)
                colM3.metric("Enviado", value=nQtdEnviado)
                colM4.metric("Duplicado", value=nQtdDuplicado)
                
                st.subheader('Status de erro', divider='gray')
                colEM1, colEM2, colEM3 = st.columns(3)    
                colEM1.metric("Erro no arquivo", value=nQtdErroNoArquivo)
                colEM2.metric("Erro ao gravar no AWS", value=nQtdErroAoGravarNoAWS)
                colEM3.metric("Processado com erro", value=nQtdProcessadoComErro)
                
                if nQtdErroNoArquivo>0:
                    btnObterDetalhes = st.button('Consultar detalhes dos erros')
                    if btnObterDetalhes:
                        st.write(f"Obtendo dado(s) do(s) {len(dfRetorno)} protocolo(s) com 'Erro no arquivo' ...")
                        ctnProgresso1 = st.empty()
                        nIndice = 1
                        print('obtendo dados dos protocolos')
                        try:
                            temp_dir = tempfile.TemporaryDirectory() 
                            for _, row in dfRetorno.iterrows():
                                sProtocolo = row['numProtocolo']
                                sGrau      = row['grau']
                                sData      = str(row['data'])[0:10].replace('-','')
                                obterProtocolo(sProtocolo, sGrau, sData, temp_dir.name)
                                ctnProgresso1.container().progress((nIndice * 100/len(dfRetorno))/100, f"Protocolo: {sProtocolo} [{nIndice}/{len(dfRetorno)} ({(nIndice * 100/len(dfRetorno)):.2f} %) ]")
                                # st.progress((nIndice * 100/len(dfRetorno))/100, f"Protocolo: {sProtocolo} [{nIndice}/{len(dfRetorno)} ({(nIndice * 100/len(dfRetorno)):.2f} %) ]")
                                nIndice += 1


                            # ctnMensagem.empty()
                            # ctnProgresso.empty()
                            # ctnMensagem.container().info(f"Analisando o(s) erro(s) do(s) {len(dfRetorno)} protocolo(s) com 'Erro no arquivo' ...")
                            st.write(f"Analisando o(s) erro(s) do(s) {len(dfRetorno)} protocolo(s) com 'Erro no arquivo' ...")
                            print('analisando erros dos protocolos')
                            lArquivos = glob(temp_dir.name + sBarraDelimitadora + '*.xml')
                            nIndice   = 1
                            lErros    = []
                            lTmp      = []
                            ctnProgresso2 = st.empty()
                            for arquivo in lArquivos:
                                sProtocolo = arquivo.replace('.xml','').replace(temp_dir.name,'')[13:]
                                while True:
                                    lTmp       = analisarProtocolos(arquivo)
                                    if "Erro de conexão" in lTmp:
                                        nTempo = randint(10, 60)
                                        # ctnMensagem.container().info(f"Quantidade máxima de requisições atingida! Hibernando análise por {nTempo} minutos.")
                                        st.write(f"Quantidade máxima de requisições atingida! Hibernando análise por {nTempo} minutos.")
                                        time.sleep(nTempo * 60)                        
                                    else:
                                        lErros.append(lTmp.copy())
                                        ctnProgresso2.container().progress((nIndice * 100/len(lArquivos))/100, f"Protocolo: {sProtocolo} [{nIndice}/{len(lArquivos)} ({(nIndice * 100/len(lArquivos)):.2f} %) ]")
                                        # st.progress((nIndice * 100/len(lArquivos))/100, f"Protocolo: {sProtocolo} [{nIndice}/{len(lArquivos)} ({(nIndice * 100/len(lArquivos)):.2f} %) ]")
                                        nIndice += 1
                                        break
                            print('termino da analise')
                        finally:
                            print('bloco finally')
                            apagarProtocolos(temp_dir.name)
                            os.removedirs(temp_dir.name)

                        if len(lErros)>0:
                            dfErros = pd.DataFrame(lErros)
                            # ctnMensagem.empty()
                            # ctnProgresso.empty()
                            st.bar_chart(dfRetorno['grau'].value_counts())
                            # st.dataframe(dfRetorno[['numProtocolo', 'grau', 'data']])
                            st.dataframe(dfErros)
                        else:
                            if nQtdAguardandoProcessamento + nQtdDuplicado + nQtdEnviado + nQtdProcessadoComSucesso>0:
                                sMensagem = "Não foi encontrado nenhum protocolo com 'Erro no arquivo' para o período informado!"
                                colM1, colM2, colM3, colM4 = st.columns(4)
                                colM1.metric("Enviado", value=nQtdEnviado)
                                colM2.metric("Processado com sucesso", value=nQtdProcessadoComSucesso)
                                colM3.metric("Duplicado", value=nQtdDuplicado)
                                colM4.metric("Aguard. processamento", value=nQtdAguardandoProcessamento)
            else:
                # ctnMensagem.empty()
                sMensagem = "Não foi encontrado nenhum dado no DATAJUD para o período informado!"
                st.write(sMensagem)
                # ctnMensagem.info(sMensagem, icon="ℹ️") 
        except Exception as e:
            st.write(f"ERRO: {str(e)}") 

with st.status('DATAJUD - CARREGAR RETORNO DATAJUD PARA O PAINEL DE MONITORAMENTO', expanded=True) as status2:
    st.write("O painel de monitoramento do datajud necessita dos arquivos de retorno, clique no botão ao lado para verificar pendências no recebimento desses arquivos")
    btnCarregarRetorno = st.button('Verificar pendência de retorno')
    
    if not st.session_state.get('btnCarregarRetorno'):
        st.session_state['btnCarregarRetorno'] = btnCarregarRetorno

    if st.session_state['btnCarregarRetorno']:
        st.write("Consultando dados dos arquivos ENVIADOS ...") 
        dfEnvios     = obterEnvios()        

        if len(dfEnvios)>0:
            # Verifica os arquivos de vigência que já foram baixados
            lArquivos = glob('data'+ sBarraDelimitadora +'*.csv')
            lArquivos = [item.split('\\')[-1].replace(' ','').replace('data/','') for item in lArquivos]
            # st.write(lArquivos)
            nQtdRetorno_ok          = 0
            nQtdRetorno_pendente    = 0 
            lPendentes              = []
            for _, row in dfEnvios.iterrows():
                sNomeCsv              = row['ARQ_DT_VIGENCIA'][0:10].replace('-','') + '.csv'
                nQtdRetorno_ok       += 1 if sNomeCsv in lArquivos else 0
                nQtdRetorno_pendente += 1 if sNomeCsv not in lArquivos else 0
                if sNomeCsv not in lArquivos:
                    lPendentes.append({'COMPETENCIA'    : row['ARQ_DT_VIGENCIA'][0:10]})

            print(len(lPendentes))    
            colEnvio1, colEnvio2 = st.columns(2)
            colEnvio1.metric("Recebido", value=nQtdRetorno_ok)
            colEnvio2.metric("Pendente", value=nQtdRetorno_pendente)
            if nQtdRetorno_pendente>0:
                st.write(f"Pesquisando o(s) {nQtdRetorno_pendente} arquivo(s) pendente(s)")
                ctnProgressoEnvio = st.empty()
                nIndice = 1                
                for item in lPendentes:
                    try:
                        sCompetencia = item.get('COMPETENCIA')
                        dftmp        = obterRetorno(sCompetencia = sCompetencia)
                        dftmp.to_csv(f"data"+ sBarraDelimitadora +"{sCompetencia.replace('-','')}.csv", sep=';', encoding='utf-8')
                        ctnProgressoEnvio.container().progress((nIndice * 100/len(lPendentes))/100, f"Competencia: {sCompetencia} [{nIndice}/{len(lPendentes)} ({(nIndice * 100/len(lPendentes)):.2f} %) ]")
                        nIndice += 1
                    except Exception as e:
                        st.write(f"ERRO ao obter dados competência {sCompetencia}, erro: {str(e)}")    
            else:
                st.write('Não existem arquivos pendentes de retorno do DATAJUD!')
        else:
            st.write('Não foram identificados envios para o DATAJUD nos últimos 24 meses!')

