import psycopg2
import psycopg2.extras
import math
import json

def conectar_a_base_datos():
    try:
        return psycopg2.connect(
            dbname="IndividualRuteo",
            user="postgres",
            password="123",
            host="localhost"
        )
    except psycopg2.OperationalError as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def cargar_datos_terremoto(archivo_json):
    with open(archivo_json, 'r') as file:
        data = json.load(file)
        feature = data['features'][0]
        magnitud = feature['properties']['mag']
        epicentro = tuple(feature['geometry']['coordinates'][:2])
        return magnitud, epicentro

def calcular_distancia(conexion, geometria, epicentro):
    cursor = conexion.cursor()
    # Convertir el epicentro en un objeto POINT de PostGIS
    query_epicentro = "SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography;"
    cursor.execute(query_epicentro, epicentro)
    epicentro_geom = cursor.fetchone()[0]

    query_distancia = "SELECT ST_Distance(ST_GeomFromText(%s, 4326)::geography, %s) as distancia;"
    cursor.execute(query_distancia, (geometria, epicentro_geom))
    distancia = cursor.fetchone()[0]

    cursor.close()
    return distancia

def calcular_probabilidad_falla(magnitud, distancia):
    if distancia <= 0:
        distancia = 0.001  # Para evitar división por cero

    factor_atenuacion = math.exp(-distancia / 35000) 

    Y = math.exp(1 * magnitud) * factor_atenuacion

    return min(Y, 1.0)

def actualizar_probabilidad_falla(conexion, epicentro, magnitud):
    cursor = conexion.cursor()
    cursor.execute("SELECT index, ST_AsText(geometria) FROM infraestructura_chilena;")
    enlaces = cursor.fetchall()

    for enlace in enlaces:
        index, geometria = enlace
        distancia = calcular_distancia(conexion, geometria, epicentro)
        probabilidad_falla = calcular_probabilidad_falla(magnitud, distancia)
        cursor.execute("UPDATE infraestructura_chilena SET probabilidad_falla = %s WHERE index = %s;", (probabilidad_falla, index))

    conexion.commit()
    cursor.close()

def main():
    conexion = conectar_a_base_datos()
    if conexion:
        magnitud, epicentro = cargar_datos_terremoto("terremoto_8-8.json")
        actualizar_probabilidad_falla(conexion, epicentro, magnitud)
        conexion.close()
    else:
        print("No se pudo establecer una conexión con la base de datos.")

if __name__ == "__main__":
    main()
