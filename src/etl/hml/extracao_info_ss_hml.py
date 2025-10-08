import pandas as pd
import numpy as np
import os
import oracledb
import geopandas as gpd
import re
import pyodbc
import warnings
from datetime import datetime, timedelta
from shapely.geometry import Point
from cryptography.fernet import Fernet
from sqlalchemy import create_engine



def gerar_chave(path_arquivo_chave):
    # Função para gerar e armazenar uma chave
    chave = Fernet.generate_key()
    with open(path_arquivo_chave, 'wb') as chave_file:
        chave_file.write(chave)


def carregar_chave(path_arquivo_chave):
    # Função para carregar a chave existente
    return open(path_arquivo_chave, 'rb').read()


def codificar_senha(senha, path_arquivo_chave, path_arquivo_senha):
    # Verifica se a chave existe, caso contrário, gera uma nova
    if not os.path.exists(path_arquivo_chave):
        gerar_chave(path_arquivo_chave)

    # Carrega a chave criada
    chave = carregar_chave(path_arquivo_chave)
    fernet = Fernet(chave)

    # Codifica a senha
    senha_codificada = fernet.encrypt(senha.encode())
    
    # Salva a senha codificada
    with open(path_arquivo_senha, 'wb') as senha_file:
        senha_file.write(senha_codificada)


def decodificar_senha(arquivo_chave, arquivo_senha):
    # Carrega a chave criada
    chave = carregar_chave(arquivo_chave)
    fernet = Fernet(chave)

    # Lê a senha codificada
    with open(arquivo_senha, 'rb') as senha_file:
        senha_codificada = senha_file.read()

    # Decodifica a senha e retorna a mesma
    senha = fernet.decrypt(senha_codificada).decode()

    return senha


def obter_senha(path_folder_senha, senha_enc='senha.enc', chave_key='chave.key'):
    os.makedirs(path_folder_senha, exist_ok=True)

    # Caminho dos arquivos
    path_arquivo_senha = os.path.join(path_folder_senha, senha_enc)
    path_arquivo_chave = os.path.join(path_folder_senha, chave_key)

    # Verifica se a senha codificada já existe
    if not os.path.exists(path_arquivo_senha):
        # Se não existe será coletado a senha e codificada
        senha = input(f'Insira a senha para codificação: ')
        codificar_senha(senha, path_arquivo_chave, path_arquivo_senha)
    else:
        # Retorna a senha decodificada
        senha = decodificar_senha(path_arquivo_chave, path_arquivo_senha)

    return senha


def coletar_info_ss(senha_cisdprd):
    print('\nColetando informações das SS do CIS...')

    # Instanciando oracle para acessar via Python
    oracledb.init_oracle_client(lib_dir=r"C:\Programs\Oracle\instantclient_23_7", config_dir=r"C:\APL\Oracle12_32\12CR2\network\admin")
    uid = 'DG_C800984'
    pwd = senha_cisdprd
    db = 'cisdprd'
    engine = create_engine(f'oracle+oracledb://{uid}:{pwd}@{db}')

    query = f"""
        SELECT 
            cos.NUM_SEQ_OPER_COS numero_ss,
            cos.COD_UN_CONS_COS unidade_consumidora,
            cos.DTA_INC_COS data_criacao_ss,
            cos.NOM_USU_INC_COS usuario_inclusao,
            cos.COD_SUB_TIPO_OS_COS tipo_ss,
            cst.DES_SUB_TIPO_OS_STO descricao_tipo_ss,
            cos.DTA_SITU_COS data_situacao_ss,
            cos.COD_SITU_COS situacao_ss,
            tsi.DES_SITU_TSI descricao_situacao_ss,
            cos.NOM_USU_SITU_COS usuario_status,
            cur.DTA_SERV_CUR data_inicio_servico,
            (cur.QTD_KM_FIM_CUR - cur.QTD_KM_INIC_CUR) km_percorrido,
            tes.NOM_EQP_TES equipe,
            cos.DTA_CONCL_SERV_COS data_conclusao,
            ttc.DES_TIPO_CONCL_OS_CNC descricao_tipo_conclusao,
            cmi.DES_OBS_ATDE_CMI descricao_ss,
            cmi.DES_OBS_EXEC_SERV_CMI observacao_execucao,
            rfl.SIG_REGI_RLF regional,
            rfl.NOM_MUN_RLF municipio,
            rfl.SIG_DIST_RLF distrital,
            rfl.DES_DIST_RLF descricao_distrital,
            rfl.SIG_SECC_RLF sigla_seccional,
            rfl.DES_SECC_RLF descricao_seccional,
            cps.NUM_COORX_PSX coordx,
            cps.NUM_COORY_PSX coordy,
            juc.COD_CONJ_JUC num_cea,
            cju.DES_CONJ_CJU nome_cea
        FROM CAD_ORD_SERV cos
        LEFT JOIN TAB_SITUAC tsi 
            ON cos.COD_TIPO_SITU_COS = tsi.COD_TIPO_SITU_TSI
            AND cos.COD_SITU_COS = tsi.COD_SITU_TSI
        LEFT JOIN CAD_MISCELANEA_SS_OS cmi
            ON cos.NUM_SEQ_OPER_COS = cmi.NUM_SEQ_OPER_CMI
            AND cos.COD_CPU_COS = cmi.COD_CPU_CMI
            AND cos.NUM_SEQ_GER_COS = cmi.NUM_SEQ_GER_CMI
        LEFT JOIN CAD_RECURSOS_OS cur
            ON cos.NUM_SEQ_OPER_COS = cur.NUM_SEQ_OPER_OS_CUR
            AND cos.COD_CPU_COS = cur.COD_CPU_OS_CUR
            AND cos.NUM_SEQ_GER_COS = cur.NUM_SEQ_GER_OS_CUR
        LEFT JOIN TAB_EQUIPE_SERV tes
            ON cur.COD_EQP_CUR = tes.COD_EQP_TES
            AND cur.COD_TIPO_EQP_CUR = tes.COD_TIPO_EQP_TES
        LEFT JOIN REL_FILTRO rfl
            ON cos.COD_LOC_COS = rfl.COD_LOC_RLF
        LEFT JOIN TAB_TIPO_CONCL_ORD_SERV ttc
            ON cos.COD_TIPO_OS_COS = ttc.COD_TIPO_OS_CNC
            AND cos.COD_SUB_TIPO_OS_COS = ttc.COD_SUB_TIPO_OS_CNC
            AND cos.COD_TIPO_CONCL_COS = ttc.COD_TIPO_CONCL_OS_CNC
        LEFT JOIN CAD_SUB_TIPO_OS cst
            ON cos.COD_TIPO_OS_COS = cst.COD_TIPO_OS_STO
            AND cos.COD_SUB_TIPO_OS_COS = cst.COD_SUB_TIPO_OS_STO
        LEFT JOIN cad_uc_ee cuc
            ON cos.COD_UN_CONS_COS = cuc.COD_UN_CONS_UEE
        LEFT JOIN cad_pste_sist_extn cps
            ON cuc.NUM_PSTE_UEE = cps.NUM_PSTE_PSX
        LEFT JOIN REL_CONJ_UC juc
            ON cos.COD_UN_CONS_COS = juc.COD_UN_CONS_JUC
            AND juc.DTA_FIM_VAL_JUC IS NULL
        LEFT JOIN TAB_CONJ cju
            ON juc.COD_CONJ_JUC = cju.COD_CONJ_CJU
        WHERE cos.DTA_INC_COS >= DATE '2024-01-01'
            AND cos.COD_SUB_TIPO_OS_COS IN ('722', '724', '726', '674', '727', '669')
        ORDER BY cos.DTA_INC_COS DESC
    """

    # Executando a query
    info_ss = pd.read_sql(query, engine)

    # Fechando conexão
    engine.dispose()

    # Removendo valores duplicados
    info_ss.drop_duplicates(inplace=True)

    return info_ss


def selecionando_equipamento(info_ss):
    print('\nSelecionando o equipamento indicado na descrição da SS...')

    # Selecionando o equipamento indicado na descrição
    info_ss['equipamento_descricao'] = info_ss.descricao_ss.apply(lambda row: re.findall(r'(8\d{4}[A-Za-z0-9]{5})', row)[0] if isinstance(row, str) and re.search(r'(8\d{4}[A-Za-z0-9]{5})', row) else None)

    return info_ss


def selecionando_nome_usuario(info_ss, senha_denodo):
    print('\nSelecionando o nome do usuário e registro profissional...')

    # Omitindo warnings na leitura dos dados do Denodo
    warnings.filterwarnings("ignore", category=UserWarning, message='pandas only supports SQLAlchemy')

    # Informações de login
    username = 'c800984'
    password = senha_denodo
    server = 'vidgcpprd.copel.nt'
    port = '9996'
    database = 'admin'
    driver = 'DenodoODBC Unicode(x64)'

    # Criando conexão
    odbc_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"PORT={port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    conn = pyodbc.connect(odbc_str)

    # Query para coletar os funcionários Copel
    query = """
        SELECT 
            registro_profissional AS registro_profissional, 
            nome_profissional AS nome_profissional, 
            sigla_org_lotacao_profissional AS sigla_org_lotacao_profissional,
            situacao_profissional AS situacao_profissional
        FROM publico.profissional
    """
    funcionarios = pd.read_sql(query, conn) # type: ignore

    # Fechando conexão
    conn.close()

    print('\nAdicionando informações do usuário...')

    # Criando uma coluna com somente os números dos registros dos funcionários que criaram a SS
    mask_funcionario = info_ss.usuario_inclusao.str.upper().str.startswith(('C', 'T', 'E'))
    info_ss.loc[mask_funcionario, 'registro_profissional'] = info_ss.loc[mask_funcionario, 'usuario_inclusao'].apply(lambda row: re.findall(r'(\d+)', row)[0])
    info_ss.registro_profissional = pd.to_numeric(info_ss.registro_profissional, errors='coerce').astype('Int64')

    # Adicionando as informações do funcionário
    funcionarios.registro_profissional = pd.to_numeric(funcionarios.registro_profissional, errors='coerce').astype('Int64')
    info_ss = info_ss.merge(funcionarios, how='left', on='registro_profissional')

    # Corrigindo a informação da coluna usuario_inclusao
    mask_notna = info_ss.nome_profissional.notna()
    info_ss.loc[mask_notna, 'usuario_inclusao'] = info_ss.loc[mask_notna, 'usuario_inclusao'] + " - " + info_ss.loc[mask_notna, 'nome_profissional'] + " - " + info_ss.loc[mask_notna, 'sigla_org_lotacao_profissional'] + " - " + info_ss.loc[mask_notna, 'situacao_profissional']

    # Removendo colunas desnecessárias
    info_ss.drop(columns=['registro_profissional', 'nome_profissional', 'sigla_org_lotacao_profissional', 'situacao_profissional'], inplace=True)

    # Criando uma coluna com somente os números dos registros dos funcionários que atualizaram a SS
    mask_funcionario = info_ss.usuario_status.str.upper().str.startswith(('C', 'T', 'E'))
    info_ss.loc[mask_funcionario, 'registro_profissional'] = info_ss.loc[mask_funcionario, 'usuario_status'].apply(lambda row: re.findall(r'(\d+)', row)[0])
    info_ss.registro_profissional = pd.to_numeric(info_ss.registro_profissional, errors='coerce').astype('Int64')

    # Adicionando as informações do funcionário
    info_ss = info_ss.merge(funcionarios, how='left', on='registro_profissional')

    # Corrigindo a informação da coluna usuario_status
    mask_notna = info_ss.nome_profissional.notna()
    info_ss.loc[mask_notna, 'usuario_status'] = info_ss.loc[mask_notna, 'usuario_status'] + " - " + info_ss.loc[mask_notna, 'nome_profissional'] + " - " + info_ss.loc[mask_notna, 'sigla_org_lotacao_profissional']

    # Removendo colunas desnecessárias
    info_ss.drop(columns=['registro_profissional', 'nome_profissional', 'sigla_org_lotacao_profissional'], inplace=True)

    return info_ss


def cea_critico(info_ss, path_file_cea_critico):
    # Lendo arquivo de conjuntos críticos
    cea_critico = pd.read_excel(path_file_cea_critico, header=None, names=['nome_cea', 'num_cea'])

    # Adicionando indicador se o conjunto é critico
    info_ss.num_cea = pd.to_numeric(info_ss.num_cea, errors='coerce').astype('Int64')
    cea_critico.num_cea = pd.to_numeric(cea_critico.num_cea, errors='coerce').astype('Int64')
    info_ss['indicador_cea_critico'] = np.where(info_ss.num_cea.isin(cea_critico.num_cea), 'SIM', 'NÃO')

    # Corrigindo o nome do conjunto
    mask_num_cea_notna = info_ss.num_cea.notna()
    mask_cea_critico = info_ss.indicador_cea_critico == 'SIM'
    info_ss.loc[mask_num_cea_notna & mask_cea_critico, 'nome_num_cea'] = info_ss.loc[mask_num_cea_notna & mask_cea_critico, 'nome_cea'] + " (" + info_ss.loc[mask_num_cea_notna & mask_cea_critico, 'num_cea'].astype(str) + ") - CRÍTICO"
    info_ss.loc[mask_num_cea_notna & (~mask_cea_critico), 'nome_num_cea'] = info_ss.loc[mask_num_cea_notna & (~mask_cea_critico), 'nome_cea'] + " (" + info_ss.loc[mask_num_cea_notna & (~mask_cea_critico), 'num_cea'].astype(str) + ")"

    # Removendo colunas desnecessárias
    info_ss.drop(columns=['num_cea', 'nome_cea'], inplace=True)

    return info_ss


def classificar_ss(texto, mapeamento):
    for chave, valor in mapeamento.items():
        if chave in texto:
            return valor
    return "OUTROS"


def indicador_ss(info_ss, path_file_espacadores):
    # Mapeamento dos padrões exibidos nas SS
    mapeamento = {
        "#PLANODEC500": "#PLANODEC500ALIM",
        "#PLANDEC500": "#PLANODEC500ALIM",
        "#SENTINELA": "#SENTINELA",
        "#ESPACADORES": "#ESPACADORES",
        "#FUMICULTOR": "#FUMICULTOR",
        "#RECORR": "#RECORRENCIA",
        "#VCQSD": "#VCQSD",
        "#GDEC": "#GDEC"
    }

    # Adicionando um filtro para indicar o tipo de SS aberto
    mask_descricao_ss_notna = info_ss.descricao_ss.notna()
    info_ss.loc[mask_descricao_ss_notna, "filtro"] = info_ss.loc[mask_descricao_ss_notna, "descricao_ss"].apply(lambda descricao: classificar_ss(descricao, mapeamento))

    # Atribuindo OUTROS para as SS com descrição vazias
    mask_descricao_ss_isna = info_ss.descricao_ss.isna()
    info_ss.loc[mask_descricao_ss_isna, "filtro"] = 'OUTROS'

    # Lendo arquivo que contém as SS criadas para instalar espaçadores
    coluna_interesse = ['numero_ss']
    espacadores = pd.read_excel(path_file_espacadores, usecols=coluna_interesse)

    # Identificando se a SS foi gerada para instalar espaçadores
    espacadores.numero_ss = pd.to_numeric(espacadores.numero_ss, errors='coerce').astype('Int64')
    espacadores.drop_duplicates(inplace=True)
    mask_numero_ss_contido = info_ss.numero_ss.isin(espacadores.numero_ss)
    info_ss.loc[mask_numero_ss_contido, "filtro"] = '#ESPACADORES'

    return info_ss


def calculando_ci(info_ss, path_file_ci_liquido):
    # Lendo informações do ci líquido dos equipamentos
    ci = pd.read_parquet(path_file_ci_liquido)

    # Convertendo coluna data_referencia para datetime
    ci.data_referencia = pd.to_datetime(ci.data_referencia, yearfirst=True)

    # Selecionando somente os últimos 3 meses
    mask_data_referencia = ci.data_referencia >= (datetime.now() - timedelta(days=90))
    ci = ci[mask_data_referencia].copy()

    # Selecionando somente interrupções acidentais
    mask_acidental = ci.tipo_interrupcao == 'ACIDENTAL'
    ci = ci[mask_acidental].copy()

    # Selecionando somente interrupções na rede
    mask_rede = ci.area_eletrica_interrupcao == 'REDE'
    ci = ci[mask_rede].copy()

    # Removendo interrupções em jumper e unidade consumidora
    mask_jumper_uc = ci.tipo_equipamento.isin(['JP', 'BJ', 'UC'])
    ci = ci[~mask_jumper_uc].copy()

    # Simplificando a informação da coluna tipo_equipamento
    mask_posto = ci.tipo_equipamento == 'PT'
    ci.loc[mask_posto, 'tipo_equipamento'] = 'T'
    ci.loc[~mask_posto, 'tipo_equipamento'] = 'C'

    # Agrupando ci líquido por equipamento
    ci = ci.groupby(['equipamento', 'tipo_equipamento'])['ci_liquido'].sum().reset_index()

    # Adicionando o ci líquido dos equipamentos dos últimos 3 meses
    mask_equipamento_descricao_notna = info_ss.equipamento_descricao.notna()
    mask_chave = ci.tipo_equipamento != 'T'
    info_ss.loc[mask_equipamento_descricao_notna, 'ci_liquido'] = info_ss.loc[mask_equipamento_descricao_notna, 'equipamento_descricao'].map(ci[mask_chave].set_index('equipamento').ci_liquido)
    mask_ci_liquido_isna = info_ss.ci_liquido.isna()
    info_ss.loc[mask_equipamento_descricao_notna & mask_ci_liquido_isna, 'ci_liquido'] = info_ss.loc[mask_equipamento_descricao_notna & mask_ci_liquido_isna, 'equipamento_descricao'].map(ci[~mask_chave].set_index('equipamento').ci_liquido)
    mask_ci_liquido_isna = info_ss.ci_liquido.isna()
    info_ss.loc[mask_equipamento_descricao_notna & mask_ci_liquido_isna, 'ci_liquido'] = 0

    return info_ss


def descricao_duplicada(info_ss, path_file_ss_plan_dec_500):
    print('\nSelecionando as SS com descrição das solicitações repetidas...')

    # # removendo colunas desnecessárias
    # info_ss_descricao_duplicada = info_ss.drop(columns=['coordx', 'coordy', 'descricao_seccional', 'descricao_distrital', 'municipio', 'descricao_tipo_ss', 'data_situacao_ss', 'usuario_status', 'descricao_tipo_conclusao', 'regional'])

    # Criando DataFrame que vai receber as SS com descrição duplicadas
    info_ss_descricao_duplicada = info_ss.copy()

    # Removendo duplicadas
    info_ss_descricao_duplicada.drop_duplicates(inplace=True)

    # Removendo duplicadas com valores nulos
    mask_notna = info_ss_descricao_duplicada.descricao_ss.notna()
    info_ss_descricao_duplicada = info_ss_descricao_duplicada[mask_notna].copy()

    # Selecionando as descrições duplicadas
    mask_descricao_duplicada = info_ss_descricao_duplicada.descricao_ss.duplicated(keep=False)
    info_ss_descricao_duplicada = info_ss_descricao_duplicada[mask_descricao_duplicada].copy()

    # Convertendo coluna unidade_consumidora para Int64
    info_ss_descricao_duplicada.unidade_consumidora = pd.to_numeric(info_ss_descricao_duplicada.unidade_consumidora, errors='coerce').astype('Int64')

    # Lendo a planilha de SSs do plano dec 500
    colunas_interesse = ['SS', 'MOTIVO']
    ss_plan_dec_500 = pd.read_excel(path_file_ss_plan_dec_500, sheet_name='CHAVES', usecols=colunas_interesse)

    # Selecionando as SSs do plano dec 500
    ss_plan_dec_500 = ss_plan_dec_500[ss_plan_dec_500['MOTIVO'] == 'Plano 500'].copy()

    # Removendo coluna desnecessária
    ss_plan_dec_500.drop(columns=['MOTIVO'], inplace=True)

    # Removendo duplicadas
    ss_plan_dec_500.drop_duplicates(inplace=True)

    # Resetando index
    ss_plan_dec_500.reset_index(drop=True, inplace=True)

    # Verificando se a SS do plano dec 500 está na descrição da SS
    info_ss_descricao_duplicada.numero_ss = pd.to_numeric(info_ss_descricao_duplicada.numero_ss, errors='coerce').astype('Int64')
    ss_plan_dec_500.SS = pd.to_numeric(ss_plan_dec_500.SS, errors='coerce').astype('Int64')
    mask_ss_contido = info_ss_descricao_duplicada.numero_ss.isin(ss_plan_dec_500.SS)
    info_ss_descricao_duplicada['ss_dec_500'] = np.where(mask_ss_contido, True, False)

    return info_ss_descricao_duplicada


def convertendo_utm_lat_lon(info_ss):
    print('\nConvertendo as coordenadas UTM para Latitude e Longitude...')

    # Cria a geometria a partir das coordenadas UTM
    geometry = [Point(xy) for xy in zip(info_ss['coordx'], info_ss['coordy'])]

    # Define a zona UTM e o hemisfério
    zona_utm = 22
    hemisferio = 'south'
    crs_utm = f"+proj=utm +zone={zona_utm} +{hemisferio} +datum=WGS84 +units=m +no_defs"

    # Cria o GeoDataFrame
    ginfo_ss = gpd.GeoDataFrame(info_ss, geometry=geometry, crs=crs_utm)

    # Converte para latitude e longitude (CRS WGS84)
    ginfo_ss = ginfo_ss.to_crs(epsg=4326)

    # Extrai latitude e longitude
    ginfo_ss['latitude'] = ginfo_ss.geometry.y
    ginfo_ss['longitude'] = ginfo_ss.geometry.x

    # Removendo colunas desnecessárias
    ginfo_ss.drop(columns=['coordx', 'coordy', 'geometry'], inplace=True)

    # Removendo duplicadas
    ginfo_ss.drop_duplicates(inplace=True)

    return ginfo_ss


def exportando_output(ginfo_ss, info_ss_descricao_duplicada, path_output, path_output_descricao_duplicada, path_file_indicador_atualizacao_bi):
    print('\nExportando o resultado final...')

    # Salvando o resultado final
    ginfo_ss.to_parquet(path_output, index=False)
    info_ss_descricao_duplicada.to_parquet(path_output_descricao_duplicada, index=False)

    # Criando arquivo para indicar que deve ser atualizado o bi das informações das SS
    with open(path_file_indicador_atualizacao_bi, 'w') as f:
        f.write('')



if __name__ == "__main__":
    try:
        inicio = datetime.now()

        print(f'\n\nProcesso iniciado em {inicio.strftime('%d/%m/%Y às %H:%M')} para extraír informações das SS do CIS.')

        # Caminho da pasta onde será armazenada a senha
        path_folder_senha = f"C:\\Users\\{os.getlogin()}\\OneDrive - copel.com\\Senha Codificada"

        # Caminho onde será salvo as informações das SS
        path_output = r'\\km3rede2\grp4\VCQSD\Projetos\informacoes-ss\info_ss.parquet'

        # Caminho onde será salvo as informações das descrições duplicadas
        path_output_descricao_duplicada = r'\\km3rede2\grp4\VCQSD\Projetos\informacoes-ss\descricao_duplicada.parquet'

        # Caminho para salvar arquivo que será reconhecido pelo Power Automate para rodar o fluxo que atualiza o BI
        path_file_indicador_atualizacao_bi = f"C:\\Users\\{os.getlogin()}\\OneDrive - copel.com\\VCQSD - Atualizar BIs\\info-ss\\info-ss.txt"

        # Caminho para o arquivo com as SS do plano dec 500
        path_file_ss_plan_dec_500 = r'\\mgarede\grp\SDN_O&M\STDNRO\5-GSIM\SSs CADASTRO.xlsx'

        # Caminho para o arquivo com os conjuntos críticos
        path_file_cea_critico = r"\\km3rede2\grp4\VCQSD\Projetos\cea-critico\35_ceas_criticos.xlsx"

        # Caminho para o arquivo com o número das SS indicadas pela VCQSD para instalar espaçadores
        path_file_espacadores = r"\\km3rede2\grp4\VCQSD\Projetos\PlanoDEC500\Arquivos_BI\SS_espacadores.xlsx"

        # Caminho para o arquivo com o ci líquido dos equipamentos
        path_file_ci_liquido = r"\\km3rede2\grp4\VCQSD\Projetos\alimentadores-chi-ci\chi_ci_liquido.parquet"

        # Coletando a senha de acesso ao DB
        senha_cisdprd = obter_senha(path_folder_senha, 'senha_cisdprd.enc', 'chave_cisdprd.key')

        # Coletando a senha de acesso ao DB
        senha_denodo = obter_senha(path_folder_senha)

        # Coletando as informações das SSs geradas a partir de 2024 para MSC respectivas
        info_ss = coletar_info_ss(senha_cisdprd)

        # Selecionando os equipamentos indicados na descrição da SS
        info_ss = selecionando_equipamento(info_ss)

        # Adicionando o nome do usuário que criou e atualizou a SS
        info_ss = selecionando_nome_usuario(info_ss, senha_denodo)

        # Adicionando a informação se o CEA é crítico
        info_ss = cea_critico(info_ss, path_file_cea_critico)

        # Adicionando a informação da indicação do tipo de SS criada
        info_ss = indicador_ss(info_ss, path_file_espacadores)

        # Calculando o ci líquido dos equipamentos nos últimos 3 meses
        info_ss = calculando_ci(info_ss, path_file_ci_liquido)

        # Selecionando as SS com descrição das solicitações repetidas
        info_ss_descricao_duplicada = descricao_duplicada(info_ss, path_file_ss_plan_dec_500)

        # Convertendo utm para lat lon
        ginfo_ss = convertendo_utm_lat_lon(info_ss)

        # Exportando resultado final
        exportando_output(ginfo_ss, info_ss_descricao_duplicada, path_output, path_output_descricao_duplicada, path_file_indicador_atualizacao_bi)

        fim = datetime.now()

        print(f"\nProcesso finalizado com sucesso em {fim.strftime('%d/%m/%Y às %H:%M')}. Decorrido {fim - inicio}")
    except ValueError as e:
        print(f'\nOcorreu o seguinte erro ao rodar o script: {e}')