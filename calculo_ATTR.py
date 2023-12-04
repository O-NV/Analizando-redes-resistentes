import psycopg2

def conectar_base_datos():
    return psycopg2.connect(
        dbname="IndividualRuteo",
        user="postgres",
        password="123",
        host="localhost"
    )

def obtener_nodos(cursor, limite=50):
    cursor.execute(f"SELECT id FROM infraestructura_chilena_vertices_pgr LIMIT {limite};")
    return cursor.fetchall()

def calcular_confiabilidad_camino(cursor, nodo_origen, nodo_destino):
    cursor.execute(f"""
        SELECT infra.index, (1 - probabilidad_falla) as confiabilidad_enlace
        FROM pgr_dijkstra(
            'SELECT index as id, source, target, ST_Length(geometria) as cost FROM infraestructura_chilena',
            {nodo_origen}, {nodo_destino}, directed := false
        ) as rutas
        JOIN infraestructura_chilena as infra ON rutas.edge = infra.index;
    """)
    confiabilidad_camino = 1
    for _, confiabilidad_enlace in cursor.fetchall():
        confiabilidad_camino *= confiabilidad_enlace
    return confiabilidad_camino

def calcular_attr_promedio(conexion):
    cursor = conexion.cursor()
    nodos = obtener_nodos(cursor)
    confiabilidad_caminos = []

    for nodo_origen in nodos:
        for nodo_destino in nodos:
            if nodo_origen != nodo_destino:
                confiabilidad_camino = calcular_confiabilidad_camino(cursor, nodo_origen[0], nodo_destino[0])
                confiabilidad_caminos.append(confiabilidad_camino)

    cursor.close()
    return sum(confiabilidad_caminos) / len(confiabilidad_caminos) if confiabilidad_caminos else 0

def main():
    conexion = conectar_base_datos()
    attr_promedio = calcular_attr_promedio(conexion)
    print(f"ATTR Promedio de la Red: {attr_promedio}")
    conexion.close()

if __name__ == "__main__":
    main()
