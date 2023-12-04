import json
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import OperationalError

def conectar_a_base_datos():
    try:
        return psycopg2.connect(
            dbname="IndividualRuteo",
            user="postgres",
            password="123",
            host="localhost"
        )
    except OperationalError as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def crear_tabla_si_no_existe(conexion):
    cursor = conexion.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS infraestructura_chilena (
            index serial PRIMARY KEY,
            id VARCHAR(255),
            id_2 VARCHAR(255),
            id_3 VARCHAR(255),
            id_2_2 VARCHAR(255),
            id_4 VARCHAR(255),
            geometria GEOMETRY(LineString, 4326),
            source integer,
            target integer,
            probabilidad_falla FLOAT DEFAULT 0.0
        );
    """)
    conexion.commit()
    cursor.close()

def leer_datos_json(archivo):
    try:
        with open(archivo, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"No se pudo encontrar el archivo {archivo}")
        return None

def procesar_datos(data):
    datos_para_insertar = []
    for feature in data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        tipo_geometria = geometry['type']
        coordenadas = geometry['coordinates']

        if tipo_geometria in ['LineString', 'MultiLineString']:
            if tipo_geometria == 'LineString':
                coordenadas = [coordenadas]

            for line in coordenadas:
                line_string = f"SRID=4326;LINESTRING({', '.join([' '.join(map(str, coord)) for coord in line])})"
                datos_para_insertar.append((
                    properties.get('id'), 
                    properties.get('id_2'),
                    properties.get('id_3'),
                    properties.get('id_2_2'),
                    properties.get('id_4'),
                    line_string
                ))

    return datos_para_insertar

def insertar_datos(conexion, datos):
    query = """
    INSERT INTO infraestructura_chilena (id, id_2, id_3, id_2_2, id_4, geometria)
    VALUES (%s, %s, %s, %s, %s, ST_GeomFromEWKT(%s))
    """
    cursor = conexion.cursor()
    execute_batch(cursor, query, datos)
    conexion.commit()
    cursor.close()

def main():
    conexion = conectar_a_base_datos()
    if conexion:
        crear_tabla_si_no_existe(conexion)
        data = leer_datos_json('Fibrapticadetectada.json')
        if data:
            datos_para_insertar = procesar_datos(data)
            insertar_datos(conexion, datos_para_insertar)
        conexion.close()

if __name__ == "__main__":
    main()
