import pandas as pd
import psycopg2
import re
from decimal import Decimal

"""
Script para Liquidar el Avalúo
RFC: https://docs.google.com/document/d/1q-2Y6DZJE-rvULJwwVGf-1Q1Hb4CI5DJ/edit?usp=sharing&ouid=117446754121510771521&rtpof=true&sd=true 

Utilice la función get_number_land_list_tuple_string
para consultar de manera personalizada
"""


towns = [
    #{"town": "25035", "town_name": "Anapoima", 'value_tipology_frecuncy': 760800},
    # {"town": "25040", "town_name": "Anolaima",'value_tipology_frecuncy': 677400},
    # {"town": "25769", "town_name": "Subachoque", "value_tipology_frecuncy": 769200},
    # {"town": "25807", "town_name": "Tibirita", "value_tipology_frecuncy": 645000},
    # {"town": "25269", "town_name": "Facatativá", "value_tipology_frecuncy": 639000},
    # {"town": "25805", "town_name": "Tibacuy", "value_tipology_frecuncy": 678000},
    # {"town": "25320", "town_name": "Guaduas", "value_tipology_frecuncy": 0},
    # {"town": "25862", "town_name": "Vergara", "value_tipology_frecuncy": 0},
    # {"town": "25486", "town_name": "Nemocón", "value_tipology_frecuncy": 0},
    # {"town": "25599", "town_name": "Apulo", "value_tipology_frecuncy": 312752},
    # {"town": "25326", "town_name": "Guatavita", "value_tipology_frecuncy": 699600},
     {"town": "25815", "town_name": "Tocaima", "value_tipology_frecuncy": 667800},
    # {
    #     "town": "25645",
    #     "town_name": "San Antonio del Tequendama",
    #     "value_tipology_frecuncy": 761000,
    # },
    # {"town": "25777", "town_name": "Supatá", "value_tipology_frecuncy": 0},
    # {"town": "25019", "town_name": "Albán", 'value_tipology_frecuncy': 0},
    # {"town": "25053", "town_name": "Arbeláez", "value_tipology_frecuncy": 0},
    # {"town": "25095", "town_name": "Bituima", 'value_tipology_frecuncy': 0},
    # {"town": "25123", "town_name": "Cachipay", 'value_tipology_frecuncy': 0},
    # {"town": "25151", "town_name": "Cáqueza", 'value_tipology_frecuncy': 0},
    # {"town": "25154", "town_name": "Carmen de Carupa", 'value_tipology_frecuncy': 0},
    # {"town": "25168", "town_name": "Chaguaní", "value_tipology_frecuncy": 0},
    # {"town": "25178", "town_name": "Chipaque", 'value_tipology_frecuncy': 0},
    # {"town": "25200", "town_name": "Cogua", 'value_tipology_frecuncy': 0},
    # {"town": "25224", "town_name": "Cucunubá", 'value_tipology_frecuncy': 0},
    # {"town": "25245", "town_name": "El Colegio", 'value_tipology_frecuncy': 0},
    # {"town": "25258", "town_name": "El Peñón", 'value_tipology_frecuncy': 0},
    # {"town": "25260", "town_name": "El Rosal", 'value_tipology_frecuncy': 0},
    # {"town": "25279", "town_name": "Fómeque", "value_tipology_frecuncy": 0},
    # {"town": "25281", "town_name": "Fosca", 'value_tipology_frecuncy': 0},
    # {"town": "25288", "town_name": "Fúquene", 'value_tipology_frecuncy': 0},
    # {"town": "25293", "town_name": "Gachalá", 'value_tipology_frecuncy': 0},
    # {"town": "25297", "town_name": "Gachetá", 'value_tipology_frecuncy': 0},
    # {"town": "25299", "town_name": "Gama", 'value_tipology_frecuncy': 0},
    # {"town": "25312", "town_name": "Granada", 'value_tipology_frecuncy': 0},
    # {"town": "25317", "town_name": "Guachetá", 'value_tipology_frecuncy': 0},
    # {"town": "25328", "town_name": "Guayabal de Síquima", 'value_tipology_frecuncy': 0},
    # {"town": "25335", "town_name": "Guayabetal", 'value_tipology_frecuncy': 0},
    # {"town": "25368", "town_name": "Jerusalén", 'value_tipology_frecuncy': 0},
    # {"town": "25372", "town_name": "Junín", 'value_tipology_frecuncy': 0},
    # {"town": "25386", "town_name": "La Mesa", 'value_tipology_frecuncy': 0},
    # {"town": "25394", "town_name": "La Palma", 'value_tipology_frecuncy': 0},
    # {"town": "25398", "town_name": "La Peña", 'value_tipology_frecuncy': 0},
    # {"town": "25407", "town_name": "Lenguazaque", 'value_tipology_frecuncy': 0},
    # {"town": "25436", "town_name": "Manta", 'value_tipology_frecuncy': 0},
    # {"town": "25438", "town_name": "Medina", 'value_tipology_frecuncy': 0},
    # {"town": "25483", "town_name": "Nariño", 'value_tipology_frecuncy': 0},
    # {"town": "25489", "town_name": "Nimaima", "value_tipology_frecuncy": 0},
    # {"town": "25491", "town_name": "Nocaima", "value_tipology_frecuncy": 0},
    # {"town": "25506", "town_name": "Venecia", 'value_tipology_frecuncy': 0},
    # {"town": "25518", "town_name": "Paime", 'value_tipology_frecuncy': 0},
    # {"town": "25524", "town_name": "Pandi", 'value_tipology_frecuncy': 0},
    # {"town": "25530", "town_name": "Paratebueno", 'value_tipology_frecuncy': 0},
    # {"town": "25535", "town_name": "Pasca", 'value_tipology_frecuncy': 0},
    # {"town": "25580", "town_name": "Pulí", 'value_tipology_frecuncy': 0},
    # {"town": "25592", "town_name": "Quebradanegra", "value_tipology_frecuncy": 0},
    # {"town": "25594", "town_name": "Quetame", 'value_tipology_frecuncy': 0},
    # {"town": "25596", "town_name": "Quipile", "value_tipology_frecuncy": 0},
    # {"town": "25653", "town_name": "San Cayetano", 'value_tipology_frecuncy': 0},
    # {"town": "25662", "town_name": "San Juan de Rioseco", 'value_tipology_frecuncy': 0},
    # {"town": "25718", "town_name": "Sasaima", 'value_tipology_frecuncy': 0},
    # {"town": "25743", "town_name": "Silvania", 'value_tipology_frecuncy': 0},
    # {"town": "25779", "town_name": "Susa", 'value_tipology_frecuncy': 0},
    # {"town": "25781", "town_name": "Sutatausa", "value_tipology_frecuncy": 0},
    # {"town": "25793", "town_name": "Tausa", "value_tipology_frecuncy": 0},
    # {"town": "25797", "town_name": "Tena", 'value_tipology_frecuncy': 0},
    # {"town": "25839", "town_name": "Ubalá", 'value_tipology_frecuncy': 0},
    # # {"town": "25841", "town_name": "Ubaque", 'value_tipology_frecuncy': 0},
    # {
    #     "town": "25843",
    #     "town_name": "Villa de San Diego de Ubaté",
    #     "value_tipology_frecuncy": 0,
    # },
    # {"town": "25845", "town_n/home/daniel/Downloadsame": "Une", 'value_tipology_frecuncy': 0},
    # {"town": "25867", "town_name": "Vianí", 'value_tipology_frecuncy': 0},
    # {"town": "25871", "town_name": "Villagómez", 'value_tipology_frecuncy': 0},
    # {"town": "25873", "town_name": "Villapinzón", 'value_tipology_frecuncy': 0},
    # {"town": "25875", "town_name": "Villeta", "value_tipology_frecuncy": 0},
    # {"town": "25878", "town_name": "Viotá", 'value_tipology_frecuncy': 0},
    # {"town": "25885", "town_name": "Yacopí", 'value_tipology_frecuncy': 0},
    # {"town": "25898", "town_name": "Zipacón", 'value_tipology_frecuncy': 0}
]



path = "d:/ACC/Ajustes Base Catastral/anapoima/Script/Tocaima/reportes20240821"
debug = False
year = 2024


# --------------
# Code DataBase
# -------------

# Database Current
origin_host = "localhost"
origin_database = "tocaima_nuevas_zh"
origin_user = "postgres"
origin_password = "carlos123"
origin_port = "5435"





def conection_postgres(host, port, db, user, passw):
    """Conectar a una base de datos de PostgreSQL.

    Args:
    host (str): Dirección IP o nombre de host del servidor PostgreSQL.
    puerto (str): Puerto en el que PostgreSQL está escuchando.
    nombre_bd (str): Nombre de la base de datos.
    usuario (str): Nombre de usuario para la conexión.
    contraseña (str): Contraseña para la conexión.

    Returns:
    connection: Objeto de conexión a la base de datos.
    """
    try:
        # Crear una conexión a la base de datos
        connection = psycopg2.connect(
            host=host, port=port, database=db, user=user, password=passw
        )
        print("Conexión exitosa a la base de datos PostgreSQL.")
        return connection

    except (Exception, psycopg2.Error) as error:
        print(f"Error al conectar a la base de datos PostgreSQL: {error}")
        return None


def exec_sql(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    resultado = cursor.fetchall()
    cursor.close()
    return resultado


def exec_fetchone(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    resultado = cursor.fetchone()
    cursor.close()
    return resultado


def exec_change_sql(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()


connection = conection_postgres(
    host=origin_host,
    port=origin_port,
    db=origin_database,
    user=origin_user,
    passw=origin_password,
)


# ------------
# Avaluo Terreno
# -------------


def get_number_land_list_tuple_string(town):
    try:
        cursor = connection.cursor()
        # Construir y ejecutar la consulta SQL
        query = f"""
            with number_lands as (
            	select 'data' as aux, numero_predial, nupre, matricula_inmobiliaria
            	from cun{town}.gc_predio p
            	left join (
            	select gc_predio_avaluo, avaluo_catastral from cun{town}.extavaluo 
            	where vigencia = '{year}-01-01'
            	) a on p.id = a.gc_predio_avaluo
            	where 
                 -- avaluo_catastral is null or avaluo_catastral = 0                 
                 -- and 
                 numero_predial in (
                    '258150002000000070073000000000',
'258150003000000030008000000000',
'258150003000000040167000000000',
'258150003000000030012000000000',
'258150003000000050020000000000',
'258150100000001890001000000000',
'258150003000000010321000000000',
'258150002000000020020000000000',
'258150002000000020065000000000',
'258150003000000040014000000000',
'258150003000000040013000000000',
'258150003000000040026000000000',
'258150001000000050566000000000',
'258150003000000040027000000000',
'258150003000000040029000000000',
'258150003000000040220000000000',
'258150003000000040223000000000',
'258150001000000050567000000000',
'258150001000000050101000000000',
'258150001000000050901000000000',
'258150001000000050900000000000',
'258150001000000050084000000000',
'258150001000000050102000000000',
'258150003000000050474000000000',
'258150001000000050217000000000',
'258150003000000040268000000000',
'258150001000000050095000000000',
'258150001000000050535000000000',
'258150001000000050098000000000',
'258150001000000050096000000000',
'258150001000000050210000000000',
'258150002000000070105000000000',
'258150002000000020019000000000',
'258150001000000050897000000000',
'258150003000000040016000000000',
'258150003000000040183000000000',
'258150100000002710023000000000',
'258150003000000040289000000000',
'258150003000000040219000000000',
'258150003000000040269000000000',
'258150003000000040240000000000',
'258150003000000040022000000000',
'258150003000000040293000000000',
'258150003000000040019000000000',
'258150003000000040239000000000',
'258150003000000040023000000000',
'258150002000000070039000000000',
'258150002000000020021000000000',
'258150003000000010216000000000',
'258150003000000040267000000000',
'258150002000000070114000000000',
'258150002000000070113000000000',
'258150002000000070036000000000',
'258150002000000020004000000000',
'258150003000000040190000000000',
'258150003000000020111000000000',
'258150003000000010217000000000',
'258150002000000080076000000000',
'258150100000002710801800000100',
'258150003000000030122000000000',
'258150003000000050494000000000',
'258150003000000030018000000000',
'258150003000000050427000000000',
'258150001000000050896000000000',
'258150003000000040801800000309',
'258150100000002710801800000099',
'258150100000002710801800000107',
'258150100000002710801800000019',
'258150003000000050421000000000',
'258150100000002710801800000013',
'258150001000000050802800000363',
'258150003000000050024000000000',
'258150100000002710801800000076',
'258150003000000040085000000000',
'258150002000000070038000000000',
'258150002000000070020000000000',
'258150002000000080081000000000',
'258150003000000040018000000000',
'258150003000000050139000000000',
'258150003000000040084000000000',
'258150100000002710801800000098',
'258150003000000050134000000000',
'258150100000002710801800000020',
'258150003000000010320000000000',
'258150001000000050114000000000',
'258150002000000020005000000000',
'258150003000000050120000000000',
'258150100000002710801800000101',
'258150002000000080080000000000',
'258150001000000050802800000367',
'258150002000000080082000000000',
'258150002000000080071000000000',
'258150003000000040010000000000',
'258150003000000010219000000000',
'258150003000000050013000000000',
'258150003000000040801800000313',
'258150003000000040801800000312',
'258150001000000050544000000000',
'258150003000000020112000000000',
'258150003000000010212000000000',
'258150003000000010213000000000',
'258150003000000040801800000308',
'258150100000002710801800000014',
'258150002000000080075000000000',
'258150003000000040801800000310',
'258150100000002710801800000021',
'258150002000000020006000000000',
'258150003000000010242000000000',
'258150003000000010215000000000',
'258150003000000040801800000311',
'258150002000000080102000000000',
'258150003000000030123000000000',
'258150001000000050802800000355',
'258150003000000030017000000000',
'258150400000000200016000000000',
'258150400000000200007000000000',
'258150400000000150006000000000',
'258150400000000220004000000000',
'258150400000000150010000000000',
'258150003000000050186000000000',
'258150400000000160015000000000',
'258150400000000160034000000000',
'258150400000000160023000000000',
'258150003000000030147000000000',
'258150400000000140008000000000',
'258150400000000150012000000000',
'258150400000000200003000000000',
'258150400000000160033000000000',
'258150400000000160024000000000',
'258150002000000080055000000000',
'258150003000000050128000000000',
'258150400000000150013000000000',
'258150400000000190007000000000',
'258150003000000030133000000000',
'258150400000000190015000000000',
'258150400000000220006000000000',
'258150400000000180001000000000',
'258150400000000170011000000000',
'258150400000000180011000000000',
'258150400000000180002000000000',
'258150400000000180009000000000',
'258150400000000170009000000000',
'258150400000000170010000000000',
'258150400000000180010000000000',
'258150400000000200019000000000',
'258150001000000050549000000000',
'258150003000000030168000000000',
'258150003000000010205000000000',
'258150003000000030151000000000',
'258150003000000040245000000000',
'258150003000000050185000000000',
'258150003000000030024000000000',
'258150003000000030166000000000',
'258150003000000040299000000000',
'258150003000000030144000000000',
'258150003000000030149000000000',
'258150003000000030014000000000',
'258150003000000050008000000000',
'258150003000000050451000000000',
'258150003000000050484000000000',
'258150003000000030146000000000',
'258150003000000030167000000000',
'258150003000000050374000000000',
'258150001000000050206000000000',
'258150003000000030042000000000',
'258150001000000050878000000000',
'258150001000000050266000000000',
'258150003000000030143000000000',
'258150003000000030139000000000',
'258150003000000030154000000000',
'258150003000000050183000000000',
'258150400000000030006000000000',
'258150003000000050274000000000',
'258150001000000050246000000000',
'258150003000000030111000000000',
'258150003000000030019000000000',
'258150003000000050420000000000',
'258150003000000050173000000000',
'258150003000000050448000000000',
'258150003000000050479000000000',
'258150400000000150001000000000',
'258150003000000050007000000000',
'258150003000000030103000000000',
'258150100000002310016000000000',
'258150003000000050032000000000',
'258150400000000140001000000000',
'258150003000000030148000000000',
'258150400000000140002000000000',
'258150400000000160016000000000',
'258150003000000050428000000000',
'258150003000000030025000000000',
'258150003000000030119000000000',
'258150003000000030142000000000',
'258150003000000050184000000000',
'258150003000000010218000000000',
'258150400000000160020000000000',
'258150003000000050442000000000',
'258150003000000030169000000000',
'258150003000000050480000000000',
'258150003000000030058000000000',
'258150003000000050018000000000',
'258150400000000190016000000000',
'258150003000000030098000000000',
'258150400000000140004000000000',
'258150003000000050172000000000',
'258150003000000030150000000000',
'258150003000000040033000000000',
'258150400000000170004000000000',
'258150003000000050444000000000',
'258150400000000200025000000000',
'258150400000000030008000000000',
'258150400000000200026000000000',
'258150001000000050514000000000',
'258150400000000200024000000000',
'258150400000000200022000000000',
'258150400000000200023000000000',
'258150400000000200021000000000',
'258150003000000040197000000000',
'258150003000000030092000000000',
'258150002000000080069000000000',
'258150003000000050169000000000',
'258150002000000080112000000000',
'258150001000000050550000000000',
'258150003000000050290000000000',
'258150003000000050418000000000',
'258150003000000050414000000000',
'258150003000000030051000000000',
'258150001000000050555000000000',
'258150003000000050486000000000',
'258150003000000050031000000000',
'258150400000000160018000000000',
'258150001000000050552000000000',
'258150001000000050545000000000',
'258150003000000030089000000000',
'258150003000000050508000000000',
'258150002000000080070000000000',
'258150001000000050553000000000',
'258150003000000050167000000000',
'258150400000000160022000000000',
'258150003000000030022000000000',
'258150003000000040186000000000',
'258150400000000190012000000000',
'258150001000000050554000000000',
'258150400000000030007000000000',
'258150003000000050181000000000',
'258150003000000050178000000000',
'258150003000000030165000000000',
'258150003000000030044000000000',
'258150003000000020236000000000',
'258150003000000040002000000000',
'258150003000000030043000000000',
'258150400000000160017000000000',
'258150003000000030152000000000',
'258150003000000040020000000000',
'258150003000000030062000000000',
'258150001000000050120000000000',
'258150003000000030100000000000',
'258150003000000050465000000000',
'258150002000000020116000000000',
'258150003000000030052000000000',
'258150003000000050407000000000',
'258150400000000190009000000000',
'258150003000000050296000000000',
'258150400000000190014000000000',
'258150003000000050505000000000',
'258150400000000170007000000000',
'258150003000000020037000000000',
'258150003000000050501000000000',
'258150400000000160006000000000',
'258150400000000160003000000000',
'258150400000000160027000000000',
'258150003000000050379000000000',
'258150400000000170001000000000',
'258150400000000160026000000000',
'258150400000000170002000000000',
'258150400000000160007000000000',
'258150400000000160002000000000',
'258150400000000160001000000000',
'258150400000000160012000000000',
'258150400000000160029000000000',
'258150400000000170003000000000',
'258150400000000160004000000000',
'258150003000000040004000000000',
'258150400000000200018000000000',
'258150003000000030096000000000',
'258150400000000160019000000000',
'258150001000000050899000000000',
'258150400000000200013000000000',
'258150003000000040094000000000',
'258150003000000050500000000000',
'258150003000000030097000000000',
'258150003000000040305000000000',
'258150003000000040079000000000',
'258150400000000140005000000000',
'258150003000000030090000000000',
'258150400000000140009000000000',
'258150003000000040034000000000',
'258150400000000160025000000000',
'258150003000000030120000000000',
'258150003000000040035000000000',
'258150003000000050336000000000',
'258150003000000010333000000000',
'258150003000000030105000000000',
'258150003000000030106000000000',
'258150003000000050287000000000',
'258150400000000160009000000000',
'258150400000000200020000000000',
'258150400000000160010000000000',
'258150400000000160005000000000',
'258150400000000160008000000000',
'258150400000000160021000000000',
'258150003000000040192000000000',
'258150400000000200005000000000',
'258150400000000060003000000000',
'258150003000000030109000000000',
'258150003000000030113000000000',
'258150003000000030153000000000',
'258150003000000030104000000000',
'258150400000000160013000000000',
'258150003000000050337000000000',
'258150400000000160028000000000',
'258150003000000030114000000000',
'258150400000000200011000000000',
'258150400000000160014000000000',
'258150003000000030110000000000',
'258150003000000050177000000000',
'258150003000000030091000000000',
'258150400000000020002000000000',
'258150002000000020023000000000',
'258150400000000200001000000000',
'258150003000000050344000000000',
'258150003000000030082000000000',
'258150003000000050341000000000',
'258150400000000200004000000000',
'258150003000000050174000000000',
'258150003000000040096000000000',
'258150003000000030053000000000',
'258150003000000040194000000000',
'258150003000000030099000000000',
'258150003000000050272000000000',
'258150003000000040081000000000',
'258150003000000050165000000000',
'258150003000000050141000000000',
'258150003000000050485000000000',
'258150400000000060004000000000',
'258150001000000050118000000000',
'258150002000000020022000000000',
'258150003000000040036000000000',
'258150003000000030037000000000',
'258150400000000200015000000000',
'258150400000000200017000000000',
'258150100000002710801800000016',
'258150100000002710801800000022',
'258150003000000040801800000025',
'258150003000000050147000000000',
'258150003000000040097000000000',
'258150003000000050132000000000',
'258150003000000030207000000000',
'258150003000000050345000000000',
'258150002000000020115000000000',
'258150003000000050523000000000'

  
                    )
            )
            
            select STRING_AGG(concat('''',numero_predial, ''''), ',')  
            	from number_lands
            group by aux
        """
        if debug == True:
            print(query)
        cursor.execute(query)
        # Obtener el resultado
        resultado = cursor.fetchone()
        cursor.close()
        if resultado:
            return "(" + resultado[0] + ")"
        else:
            return None
    except Exception as err:
        pass


def get_query_value_terrain(town, land_number_tuple):
    query = f"""
            with
                land as (
                    select
                        p.id,
                        numero_predial,
                        t.geometria as geom_land,
                        p.area,
                        ST_Area(t.geometria) as area_geom
                    from
                        cun{town}.gc_predio p
                        left join 
                        (select * from cun{town}.col_uebaunit ue where ue.ue_gc_terreno is not null )
								 ue on ue.unidad = p.id
                        left join cun{town}.gc_terreno t on ue.ue_gc_terreno = t.id
                    where
                         numero_predial in {land_number_tuple}
                ),
                zhg as (
                    select
                        codigo_zona_geoeconomica as code_zhg,
                        (valor_hectarea_agropecuario / 10000) as valor_m2_catastral,
                        geometria as geom_zhg
                    from
                        cun{town}.zhg_rural
                    union
                    select
                        codigo_zona_geoeconomica,
                        valor_m2_catastral,
                        geometria
                    from
                        cun{town}.zhg_urbana
                ),
                land_zhg as (
                    select
                        zhg.code_zhg,
                    	CASE WHEN  zhg.code_zhg is null THEN null 
                    	ELSE
                                            concat (
                                                '[ zona: ',
                                                zhg.code_zhg,
                                                ' - valor_m2_zhg: ',
                                                round(zhg.valor_m2_catastral, 0),
                                                '- area: ',
                                                cast(
                                                    ST_Area (ST_Intersection (geom_land, geom_zhg)) as DECIMAL(10, 2)
                                                ),
                                                ']'
                                            ) 
                    	END
                    	as zhg_value,
                        zhg.valor_m2_catastral as valor_m2_zhg,
                        (
                            cast(
                                ST_Area (ST_Intersection (geom_land, geom_zhg)) as DECIMAL(10, 2)
                            ) / ST_Area (geom_land)
                        ) * zhg.valor_m2_catastral as valor_m2_ponderado,
                        land.numero_predial,
                        land.area,
                        land.area_geom

						from
					    land
					left join
					    zhg
					on
					    ST_Intersects (geom_land, geom_zhg)
                ),
                report as (
                    select
                        numero_predial,
                        count(*) as cantidad_zonas,
                        STRING_AGG (code_zhg, '-') as zonas,
                        STRING_AGG (zhg_value, '-') as descripcion_zhg,
                        sum(valor_m2_ponderado) as valor_m2_ponderado,
                         area,
                         area_geom,
                        sum(valor_m2_ponderado) * area as avaluo_terreno
                    from
                        land_zhg
                    group by
                        numero_predial,
                        area, area_geom
                )
            select
              numero_predial,
              cantidad_zonas,
              zonas,
              descripcion_zhg,
              valor_m2_ponderado,
              area,
              area_geom,
              CASE
                  when avaluo_terreno is null then 0
                  else avaluo_terreno
                END as avaluo_terreno_final
            
            from
                report
        """

    if debug:
        print("query avaluo terrain")
        print(query)
    data = exec_sql(connection, query)
    data_df = pd.DataFrame(
        data,
        columns=[
            "numero_predial",
            "cantidad_zonas",
            "zhg",
            "descripcion_zhg",
            "valor_m2_terreno",
            "area_terreno",
            "area_terreno_geometria",
            "avaluo_terreno",
        ],
    )
    return data_df


def get_query_value_build(town, land_number_tuple):
    query = f"""
    with
    unids_value as (
        select
            p.numero_predial,
            p.area_construida,
            uc.id as id,
            cuc.identificador,
            cuc.anio_construccion,
            uc.area,
            uct.dispname as uso,
            case 
            when cuc.anio_construccion is not null then
            {year} - cuc.anio_construccion 
            else 0
            end as edad,
            case
                when tct.id is null then tv2.vida_util
                when tv.vida_util is not null then tv.vida_util
                else 0
            end as vida_util,
            case
                when tct.id is null then 1
                else 0
            end as es_anexo,
            case
                when tct.id is null then act.text_code
                else tct.text_code
            end as tipologia,
            case
                when tct.id is null then tv2.valor_salvamento
                else tv.valor_salvamento
            end as valor_salvamento,
            case
                when tct.id is null then tv2.valor_m2_catastral
                else tv.valor_m2_catastral
            end as valor_m2_catastral
        from
            cun{town}.gc_predio p
            left join (
                select
                    *
                from
                    cun{town}.col_uebaunit ue
                where
                    ue.ue_gc_unidadconstruccion is not null
            ) ue on ue.unidad = p.id
            left join cun{town}.gc_unidadconstruccion uc on ue.ue_gc_unidadconstruccion = uc.id
            left join cun{town}.gc_caracteristicasunidadconstruccion cuc on uc.gc_caracteristicasunidadconstruccion = cuc.id
            left join cun{town}.gc_usouconstipo uct on cuc.uso = uct.id
            left join cun{town}.cuc_tipologiaconstruccion tc on tc.gc_caracteristicasunidadconstruccion = cuc.id
            left join cun{town}.cuc_tipologiatipo tct on tct.id = tc.tipo_tipologia
            left join cun{town}.cuc_calificacionnoconvencional cnc on cnc.gc_caracteristicasunidadconstruccion = cuc.id
            left join cun{town}.cuc_anexotipo act on act.id = cnc.tipo_anexo
            left join cun{town}.tb_valortipologia tv on tv.tipo_tipologia = tc.tipo_tipologia
            left join cun{town}.tb_valortipologia tv2 on tv2.tipo_anexo = cnc.tipo_anexo
        where
            p.numero_predial in {land_number_tuple}
    ),
    depreciation as (
        -- Anexo: Heidecken
        select
            unids_value.*,
            CASE
                WHEN es_anexo = 1 THEN 'tb_heidecke'
                ELSE 'fitto_corvini'
            END as metodo_dep,
            CASE
                WHEN es_anexo = 1 THEN h.clase
                ELSE fc.clase
            END as clase,
            CASE
                WHEN h.clase = 1 and es_anexo = 1 THEN 0
                WHEN h.clase = 1.5 and es_anexo = 1 THEN 0.00032
                WHEN h.clase = 2 and es_anexo = 1 THEN 0.0252
                WHEN h.clase = 2.5 and es_anexo = 1 THEN 0.0809
                WHEN h.clase = 3 and es_anexo = 1 THEN 0.181
                WHEN h.clase = 3.5 and es_anexo = 1 THEN 0.332
                WHEN h.clase = 4 and es_anexo = 1 THEN 0.526
                WHEN h.clase = 4.5 and es_anexo = 1 THEN 0.752
                WHEN edad > 500 and es_anexo = 1 THEN 0.752
                --- Fito Corvini
                WHEN fc.clase = 1 and es_anexo = 0 and vida_util != 0
                	THEN (0.0050*((edad/vida_util)^2)+0.5001*((edad/vida_util))-0.0071)/100
                WHEN fc.clase = 1.5 and es_anexo = 0 and vida_util != 0
                	THEN (0.0050*((edad/vida_util)^2)+0.4998*((edad/vida_util))+0.0262)/100
                WHEN fc.clase = 2 and es_anexo = 0 and vida_util != 0
                	THEN  (0.0049*((edad/vida_util)^2)+0.4861*((edad/vida_util))+2.5407)/100
                WHEN fc.clase = 2.5  and es_anexo = 0  and vida_util != 0
                	THEN ( 0.0046*((edad/vida_util)^2)+0.4581*((edad/vida_util))+8.1068)/100
                WHEN fc.clase = 3 and es_anexo = 0 and vida_util != 0
                	THEN  (0.0041*((edad/vida_util)^2)+0.4092*((edad/vida_util))+18.1041)/100
                WHEN fc.clase = 3.5 and es_anexo = 0 	and vida_util != 0
                	THEN  (0.0033*((edad/vida_util)^2)+0.3341*((edad/vida_util))+33.1990)/100
                WHEN fc.clase = 4 and es_anexo = 0 	and vida_util != 0
                	THEN  (0.0023*((edad/vida_util)^2)+0.2400*((edad/vida_util))+52.5274)/100
                WHEN fc.clase = 4.5 and es_anexo = 0 	and vida_util != 0
                	THEN (0.0012*((edad/vida_util)^2)+0.1275*((edad/vida_util))+75.1530)/100
                ELSE 0
            END as depreciation
        FROM
            unids_value
            LEFT JOIN cun{town}.tb_heidecke h ON unids_value.edad BETWEEN h.edad_inferior AND h.edad_superior
            LEFT JOIN cun{town}.tb_fittocorvini fc ON unids_value.edad BETWEEN fc.edad_inferior AND fc.edad_superior
    ),
    m2_dep as (
        select
            *,
            valor_m2_catastral - (depreciation * valor_m2_catastral) as valor_m2_dep
        from
            depreciation
    ),
    valor_m2_final as (
        select
            *,
            CASE
                WHEN valor_salvamento > valor_m2_dep THEN valor_salvamento
                ELSE valor_m2_dep
            END as valor_m2_final
        from
            m2_dep
    ),
    avaluo_unid_build as (
        select
            CASE
                when id is not null THEN 1
                ELSE 0
            END as unid_exist,
            *,
            valor_m2_final * area as avaluo_construccion
        from
            valor_m2_final
    ),
    avaluo_unid_build_report as (
        select
            numero_predial,
            area_construida,
            unid_exist,
            CASE
                WHEN unid_exist = 1 THEN concat (
                    'Id: ',
                    id,
                    ' - Identificador: ',
                    identificador,
                    ' - año_construccion: ',
                    anio_construccion,
                    ' - area: ',
                    area,
                    ' - uso: ',
                    uso,
                    ' - edad: ',
                    edad,
                    ' - vida util: ',
                    vida_util,
                    ' - anexo: ',
                    es_anexo,
                    ' - Tipologia: ',
                    tipologia,
                    ' - Valor Tipologia m2: ',
                    valor_m2_catastral,
                    ' - Valor_salvamento: ',
                    valor_salvamento,
                    ' - Metodo Depreciacion: ',
                    metodo_dep,
                    ' - Clase Depreciacion: ',
                    clase,
                    ' - Depreciacion: ',
                    depreciation,
                    ' - Valor m2 Catastral: ',
                    valor_m2_final,
                    ' - Avaluo construccion: ',
                    avaluo_construccion
                )
                ELSE null
            END as unidad,
            avaluo_construccion
        from
            avaluo_unid_build
    ),
    report as (
        select
            numero_predial,
            sum(unid_exist) as cantidad_unidades,
            STRING_AGG (unidad, ';') as unidades,
            case 
                when area_construida is not null and area_construida != 0 then
                    sum(avaluo_construccion) / area_construida 
                else 0 
            end as valor_m2_construccion,
            area_construida,
            sum(avaluo_construccion) as avaluo_construccion
        from
            avaluo_unid_build_report
        group by
            numero_predial,
            area_construida
    )
select
    numero_predial,
    cantidad_unidades,
    unidades,
    valor_m2_construccion,
    area_construida,
    CASE 
        when avaluo_construccion is null then 0
        else avaluo_construccion
    END
from
    report
        """
    if debug:
        print("query avaluo construccion")
        print(query)
    data = exec_sql(connection, query)
    data_df = pd.DataFrame(
        data,
        columns=[
            "numero_predial",
            "cantidad_unidades",
            "descripcion_unidades",
            "valor_m2_construccion",
            "area_construida",
            "avaluo_construccion",
        ],
    )
    return data_df


def get_predio_matriz(row):
    try:
        land_number = row["numero_predial"]
        cursor = connection.cursor()
        # Construir y ejecutar la consulta SQL
        query = f"""
            with 
            predios_matriz as (
            	select pc.*, p.numero_predial as numero_predial_matriz, p.nupre as nupre_matriz
            	from cun{town}.gc_prediocopropiedad pc
            	inner join cun{town}.gc_predio p on p.id = pc.matriz
            ),
            predios_copropiedad as (
            	select 
            		p.numero_predial , pc.*
            	from cun{town}.gc_predio p 
            	inner join predios_matriz pc on pc.unidad_predial = p.id
            
            )
            select  
            	numero_predial_matriz
            from predios_copropiedad 
            WHERE numero_predial = '{land_number}'
        """
        
        cursor.execute(query)
        # Obtener el resultado
        resultado = cursor.fetchone()
        cursor.close()
        if resultado:
            return resultado[0]
        else:
            return None
    except Exception as err:
        pass


def get_predio_coeficiente(row):
    try:
        land_number = row["numero_predial"]
        cursor = connection.cursor()
        query = f"""
            with 
            predios_matriz as (
            	select pc.*, p.numero_predial as numero_predial_matriz, p.nupre as nupre_matriz
            	from cun{town}.gc_prediocopropiedad pc
            	inner join cun{town}.gc_predio p on p.id = pc.matriz
            ),
            predios_copropiedad as (
            	select 
            		p.numero_predial , pc.*
            	from cun{town}.gc_predio p 
            	inner join predios_matriz pc on pc.unidad_predial = p.id
            
            )
            select  
            	coeficiente
            from predios_copropiedad 
            WHERE numero_predial = '{land_number}'
        """
        cursor.execute(query)
        # Obtener el resultado
        resultado = cursor.fetchone()
        cursor.close()
        if resultado:
            return resultado[0]
        else:
            return None
    except Exception as err:
        pass


def get_avaluo_predio_matriz(row, report):
    try:
        predio_matriz = row["predio_matriz"]
        valor_avaluo = report.loc[
            report["numero_predial"] == predio_matriz, "avaluo_catastral"
        ].values[0]
        return valor_avaluo
    except Exception as err:
        return None


def get_derecho(row):
    land_number = row["numero_predial"]
    cursor = connection.cursor()
    # Construir y ejecutar la consulta SQL
    try:
        query = f"""
            with interesados_group as (
            select ai.id, concat('Participacion: ', cm.participacion, '- Interesado: ', i.nombre) as interesado
            from cun{town}.gc_agrupacioninteresados ai
            inner join cun{town}.col_miembros cm ON cm.agrupacion = ai.id
            inner join cun{town}.gc_interesado i on cm.interesado_gc_interesado = i.id
        ),
        group_int as (
            select id, STRING_AGG(interesado, ' ; ') as interesados from interesados_group
            group by id
        )

        select p.numero_predial, concat(ai.id, i.nombre, ai.interesados) as interesado
            from cun{town}.gc_derecho d
            inner join cun{town}.gc_predio p on d.baunit = p.id
        left join cun{town}.gc_interesado i on d.interesado_gc_interesado = i.id
        left join group_int ai on d.interesado_gc_agrupacioninteresados = ai.id 
        WHERE numero_predial = '{land_number}'
        """
        cursor.execute(query)
        # Obtener el resultado
        resultado = cursor.fetchone()
        cursor.close()
        if resultado:
            return resultado[1]
        else:
            return None
    except Exception as err:
        pass


def get_data_history_value(connection, town, land_number_tuple, debug):
    query = f"""
   with count_geoms AS ( 
    	select 
    		p.numero_predial,
	 		count(ue.ue_gc_terreno) as cantidad_terreno,
	 		STRING_AGG(cast(ue.ue_gc_terreno as TEXT), ' - ') as ids_terreno,
    		count(ue.ue_gc_construccion) as construccion,
	 	 	STRING_AGG(cast(ue.ue_gc_construccion as TEXT), ' - ') as ids_construccion,
    		count(ue.ue_gc_unidadconstruccion) as unidad_construccion,
	 	 	STRING_AGG(cast(ue.ue_gc_unidadconstruccion as TEXT), ' - ') as ids_unidades_const,
    		count(ue.ue_gc_servidumbretransito) as servidumbre
    	FROM cun{town}.gc_predio p
    	LEFT JOIN cun{town}.col_uebaunit ue
    		on p.id = ue.unidad
	 	LEFT JOIN cun{town}.gc_terreno t
	 		on t.id = ue.ue_gc_terreno
		LEFT JOIN cun{town}.gc_unidadconstruccion uc on ue.ue_gc_unidadconstruccion = uc.id 
    	where p.numero_predial in {land_number_tuple}
    	group by p.numero_predial
	 )
	 
	, cases AS (
    select *,
    	CASE 
    		WHEN cantidad_terreno = 0 and construccion = 0 and unidad_construccion = 0 THEN 8
    		WHEN cantidad_terreno > 0 and construccion > 0 and unidad_construccion > 0 THEN 1
            WHEN cantidad_terreno > 0 and construccion > 0 and unidad_construccion = 0 THEN 2
            WHEN cantidad_terreno > 0 and construccion = 0 and unidad_construccion > 0 THEN 3
            WHEN cantidad_terreno > 0 and construccion = 0 and unidad_construccion = 0 THEN 4
            WHEN cantidad_terreno = 0 and construccion > 0 and unidad_construccion > 0 THEN 5
            WHEN cantidad_terreno = 0 and construccion > 0 and unidad_construccion = 0 THEN 6
            WHEN cantidad_terreno = 0 and construccion = 0 and unidad_construccion > 0 THEN 7
    		ELSE 10
    	END as caso
    	from count_geoms
    ),
	values_history  AS (
		SELECT
			p.numero_predial,
            SUM(CASE WHEN ea.vigencia = '2022-01-01' THEN ea.avaluo_catastral ELSE 0 END) AS vigencia2022,
			SUM(CASE WHEN ea.vigencia = '2023-01-01' THEN ea.avaluo_catastral ELSE 0 END) AS vigencia2023,
			SUM(CASE WHEN ea.vigencia = '2024-01-01' THEN ea.avaluo_catastral ELSE 0 END) AS vigencia2024
			FROM cun{town}.gc_predio p
			LEFT join cun{town}.extavaluo ea on ea.gc_predio_avaluo = p.id
			where p.numero_predial in 
            {land_number_tuple}
		GROUP BY
			p.numero_predial, p.nupre
    )
    	
    	SELECT 
    		cases.*,
            vh.vigencia2022,
			vh.vigencia2023,
			vh.vigencia2024
			FROM cases
			left join values_history vh on vh.numero_predial = cases.numero_predial
    """
    if debug:
        print(query)
    data = exec_sql(connection, query)
    result = pd.DataFrame(
        data,
        columns=[
            "numero_predial",
            "terrenos",
            "id_terrenos",
            "construcciones",
            "id_construccion",
            "unidades",
            "id_unidades",
            "servidumbres",
            "caso",
            "vigencia2022",
            "vigencia2023",
            "vigencia2024",
        ],
    )
    return result


def get_nupre(row):
    try:
        land_number = row["numero_predial"]
        cursor = connection.cursor()
        # Construir y ejecutar la consulta SQL
        query = f"""
            SELECT nupre
            FROM cun{town}.gc_predio
            WHERE numero_predial = '{land_number}'
        """
        cursor.execute(query)
        # Obtener el resultado
        resultado = cursor.fetchone()
        cursor.close()
        if resultado:
            return resultado[0]
        else:
            return None
    except Exception as err:
        pass


def get_matricula(row):
    try:
        land_number = row["numero_predial"]
        cursor = connection.cursor()
        # Construir y ejecutar la consulta SQL
        query = f"""
            SELECT matricula_inmobiliaria
            FROM cun{town}.gc_predio
            WHERE numero_predial = '{land_number}'
        """
        cursor.execute(query)
        # Obtener el resultado
        resultado = cursor.fetchone()
        cursor.close()
        if resultado:
            return resultado[0]
        else:
            return None
    except Exception as err:
        pass


def get_tipoderecho(row):
    try:
        land_number = row["numero_predial"]
        cursor = connection.cursor()
        # Construir y ejecutar la consulta SQL
        query = f"""

        SELECT  dt.text_code  as tipoderecho
        FROM cun{town}.gc_predio
            inner join cun{town}.gc_derecho d ON d.baunit = gc_predio.id
            inner join cun{town}.gc_derechotipo dt ON dt.id = d.tipo
            WHERE numero_predial = '{land_number}'
        """
        cursor.execute(query)
        # Obtener el resultado
        resultado = cursor.fetchone()
        cursor.close()
        if resultado:
            return resultado[0]
        else:
            return None
    except Exception as err:
        pass


def get_destinacion(row):
    try:
        land_number = row["numero_predial"]
        cursor = connection.cursor()
        # Construir y ejecutar la consulta SQL
        query = f"""
            select de.dispname from cun{town}.gc_predio p
            inner join cun{town}.gc_destinacioneconomicatipo de ON de.id = p.destinacion_economica
            where p.numero_predial = '{land_number}'
        """
        cursor.execute(query)
        # Obtener el resultado
        resultado = cursor.fetchone()
        cursor.close()
        if resultado:
            return resultado[0]
        else:
            return None
    except Exception as err:
        pass


def main(town, number_land_list_tuple):
    print(town)
    print("Get value build...")
    value_build = get_query_value_build(town, number_land_list_tuple)

    print("Get value Terrain...")
    value_terrain = get_query_value_terrain(town, number_land_list_tuple)

    print("Get value Land...")
    value_land = value_terrain.merge(value_build, on="numero_predial")
    value_land["avaluo_catastral"] = round(
        value_land.avaluo_construccion + value_land.avaluo_terreno, -3
    )

    print("Get Right...")
    value_land["interesados"] = value_land.apply(get_derecho, axis=1)

    print("Get Cases PH Matriz...")
    value_land["predio_matriz"] = value_land.apply(get_predio_matriz, axis=1)
    value_land["coeficiente"] = value_land.apply(get_predio_coeficiente, axis=1)
    value_land["avaluo_predio_matriz"] = value_land.apply(
        get_avaluo_predio_matriz, axis=1, args=(value_land,)
    )
    value_land["valor_aumentar"] = value_land["avaluo_predio_matriz"].astype(
        float
    ) * value_land["coeficiente"].astype(float)
    
    value_land["avaluo_final"] = value_land["avaluo_catastral"] + value_land["valor_aumentar"]

    print("Get History Values...")
    values_current = get_data_history_value(
        connection, town, number_land_list_tuple, debug
    )
    value_land = value_land.merge(values_current, on="numero_predial", how="left")

    value_land["nupre"] = value_land.apply(get_nupre, axis=1)
    value_land["matricula"] = value_land.apply(get_matricula, axis=1)
    value_land["destinacion"] = value_land.apply(get_destinacion, axis=1)
    value_land["derecho"] = value_land.apply(get_tipoderecho, axis=1)

    value_land["municipio"] = value_land.apply(get_tipoderecho, axis=1)

    print("Build Report...")
    if debug == True:
        pass

    #Organizar data frame
    columnas_originales = value_land.columns.tolist()
    columnas_principio = ['numero_predial', 'nupre', 'matricula']
    columnas_restantes = [col for col in columnas_originales if col not in columnas_principio]
    columnas_ordenadas = columnas_principio + columnas_restantes
    value_land_organizado = value_land.reindex(columns=columnas_ordenadas)
    print(value_land_organizado.head())





    print("Save file in " + "{}/{}_reporte_liquidacion.csv".format(path, town))
    
    value_land.to_excel("{}/{}_reporte_liquidacion.xlsx".format(path, town), index=False)

    value_land_organizado.to_csv("{}/{}_reporte_liquidacion.csv".format(path, town), index=False)


for town_data in towns:
    town = town_data["town"]
    number_land_list_tuple = get_number_land_list_tuple_string(town)
    if number_land_list_tuple != None:
        print("Cantidad de Predios a liquidar: ", len(number_land_list_tuple))
        main(town, number_land_list_tuple)
    else:
        print("No hay predios para liquidar ", town)

print("END")
