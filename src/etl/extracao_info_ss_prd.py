import pandas as pd
import numpy as np
import os
import oracledb
import geopandas as gpd
import re
from time import sleep
from datetime import datetime
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
                cos.DTA_CONCL_SERV_COS data_conclusao,
                ttc.DES_TIPO_CONCL_OS_CNC descricao_tipo_conclusao,
                cms.DES_OBS_ATDE_CMI descricao_ss,
                cms.DES_OBS_EXEC_SERV_CMI observacao_execucao,
                rfl.SIG_REGI_RLF regional,
                rfl.NOM_MUN_RLF municipio,
                rfl.SIG_DIST_RLF distrital,
                rfl.DES_DIST_RLF descricao_distrital,
                rfl.SIG_SECC_RLF sigla_seccional,
                rfl.DES_SECC_RLF descricao_seccional,
                cps.NUM_COORX_PSX coordx,
                cps.NUM_COORY_PSX coordy
            FROM CAD_ORD_SERV cos
            LEFT JOIN TAB_SITUAC tsi 
                ON cos.COD_TIPO_SITU_COS = tsi.COD_TIPO_SITU_TSI
                AND cos.COD_SITU_COS = tsi.COD_SITU_TSI
            LEFT JOIN CAD_MISCELANEA_SS_OS cms 
                ON cos.NUM_SEQ_OPER_COS = cms.NUM_SEQ_OPER_CMI
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


def descricao_duplicada(info_ss, path_file_ss_plan_dec_500):
    print('\nSelecionando as SS com descrição das solicitações repetidas...')

    # removendo colunas desnecessárias
    info_ss_descricao_duplicada = info_ss.drop(columns=['coordx', 'coordy', 'descricao_seccional', 'descricao_distrital', 'municipio', 'descricao_tipo_ss', 'data_situacao_ss', 'usuario_status', 'descricao_tipo_conclusao', 'regional'])

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
    print('\nIniciando a extração das informações das SS do CIS...\n')

    inicio = datetime.now()

    # Caminho da pasta onde será armazenada a senha
    path_folder_senha = f"C:\\Users\\{os.getlogin()}\\OneDrive - copel.com\\Senha Codificada"

    # Caminho onde será salvo as informações das SS
    path_output = r'\\km3rede2\grp4\VCQSD\Projetos\informacoes-ss\info_ss.parquet'

    # Caminho onde será salvo as informações das descrições duplicadas
    path_output_descricao_duplicada = r'\\km3rede2\grp4\VCQSD\Projetos\informacoes-ss\descricao_duplicada.parquet'

    path_file_indicador_atualizacao_bi = f"C:\\Users\\{os.getlogin()}\\OneDrive - copel.com\\VCQSD - Atualizar BIs\\info-ss\\info-ss.txt"

    path_file_ss_plan_dec_500 = r'\\mgarede\grp\SDN_O&M\STDNRO\5-GSIM\SSs CADASTRO.xlsx'

    # Coletando a senha de acesso ao DB
    senha_cisdprd = obter_senha(path_folder_senha, 'senha_cisdprd.enc', 'chave_cisdprd.key')

    # Coletando as informações das SSs geradas a partir de 2024 para MSC respectivas
    info_ss = coletar_info_ss(senha_cisdprd)

    # Selecionando os equipamentos indicados na descrição da SS
    info_ss = selecionando_equipamento(info_ss)

    # Selecionando as SS com descrição das solicitações repetidas
    info_ss_descricao_duplicada = descricao_duplicada(info_ss, path_file_ss_plan_dec_500)

    # Convertendo utm para lat lon
    ginfo_ss = convertendo_utm_lat_lon(info_ss)

    # Exportando resultado final
    exportando_output(ginfo_ss, info_ss_descricao_duplicada, path_output, path_output_descricao_duplicada, path_file_indicador_atualizacao_bi)

    fim = datetime.now()

    print(f'\nDecorrido {fim-inicio} para rodar o script.')

    sleep(43200)