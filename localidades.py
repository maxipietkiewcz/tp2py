import csv
import sys
import MySQLdb



def crear_tabla(connection):
    try:
        #crear cursor
        cursor = connection.cursor()

        #borrar tabla si ya existe
        borrar_tabla = """DROP TABLE IF EXISTS localidades"""
        #crear tabla
        crear_tabla= """ 
        CREATE TABLE localidades (
            provincia VARCHAR(255),
            id INT,
            localidad VARCHAR(255),
            cp VARCHAR(255),
            id_prov_mstr INT 
        )"""

        #ejecutar consulta
        cursor.execute(borrar_tabla)
        cursor.execute(crear_tabla)

        #confirmar cambios
        connection.commit()
        print("Base de datos creada con exito")
    except MySQLdb.Error as error:
        print("Error: {}".format(error))

def insetar_datos(connection):
    try:
        #crear cursor
        cursor = connection.cursor()
        #abrir archivo csv
        with open("localidades.csv", mode='r', newline='', encoding='utf-8') as archivo_csv:
            lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
            for fila in lector_csv:
                #extraer datos
                provincia = fila[0]
                id = fila[1]
                localidad = fila[2]
                cp = fila[3]
                id_prov_mstr = fila[4]
                #consulta para insertar datos
                sqsl_insert = """
                INSERT INTO localidades(provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)
                """

                #ejecutar consulta con los datos extraidos
                cursor.execute(sqsl_insert, (provincia,id, localidad, cp, id_prov_mstr))

                #confirmar cambios
                connection.commit()
        

        print("Registros insertados con exito")

    except MySQLdb.Error as error:
        print("Error: {}".format(error))

def exportar_localidades_por_provincia(connection):
    try:
        #crear cursor
        cursor = connection.cursor()
        # Consulta SQL para agrupar las localidades por provincia
        consulta_sql = """
        SELECT provincia, GROUP_CONCAT(localidad) AS localidades, COUNT(*) AS cantidad
        FROM localidades
        GROUP BY provincia
        """

        # Ejecutar la consulta
        cursor.execute(consulta_sql)

        # Obtener los resultados
        resultados = cursor.fetchall()

        #exportar resultados a un archivo csv separados
        for resultado in resultados:
            provincia = resultado[0]
            localidades = resultado[1].split(",")
            cantidad_localidades = resultado[2]

            # Crear un archivo CSV para cada resultado
            nombre_archivo = f"{provincia}.csv"

            #escribir los resultados en el archivo CSV
            with open(nombre_archivo, mode="w", newline="", encoding="utf-8") as archivo_csv:
                escritor_csv = csv.writer(archivo_csv)
            
            #escribir las localidades en el archivo CSV
                for localidad in localidades:
                    escritor_csv.writerow([localidad])

                #escribir la cantidad de localidades al final del archivo CSV
                escritor_csv.writerow([f"Cantidad de localidades: {cantidad_localidades}"])
                print(f"Exportando las localidades de {provincia} a {nombre_archivo}")


    except MySQLdb.Error as error:
        print("Error: {}".format(error))


def main():
    try:
        # Conectarse a la base de datos
        connection = MySQLdb.connect(host='localhost',
                                     user='root',
                                     password='',
                                     database='localidades')
        
        crear_tabla(connection) #llamar a la función para crear la tabla en la base de datos.   
        insetar_datos(connection) #llamar a la función para insertar datos del archivo csv a la base de datos.
        exportar_localidades_por_provincia(connection) #llamar a la función para exportar datos agrupados por provincia a archivos csv separados. 

    except MySQLdb.Error as error:
        print("Error: {}".format(error))

    finally:
        # cerrar la base de datos
        if connection:
            connection.close()
            print("Conexión cerrada")

main()
