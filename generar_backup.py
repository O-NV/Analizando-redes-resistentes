import psycopg2

def generar_backup_sql(nombre_archivo, conexion):
    cursor = conexion.cursor()
    cursor.execute("SELECT index, probabilidad_falla FROM infraestructura_chilena;")
    enlaces = cursor.fetchall()

    with open(nombre_archivo, 'w') as file:
        for enlace in enlaces:
            index, probabilidad = enlace
            line = f"UPDATE infraestructura_chilena SET probabilidad_falla = {probabilidad} WHERE index = {index};\n"
            file.write(line)

    cursor.close()

def main():
    conexion = psycopg2.connect(
        dbname="IndividualRuteo",
        user="postgres",
        password="123",
        host="localhost"
    )
    generar_backup_sql("backup.sql", conexion)
    conexion.close()

if __name__ == "__main__":
    main()
