import pandas as pd
import os
import sqlite3
import traceback
from datetime import datetime

def insert_data(df_file, db_name, db_path):
    try:
        if os.path.exists(os.path.join(db_path, db_name)):
            # Check if the tables of the data warehouse are not empty
            conn = sqlite3.connect(os.path.join(db_path, db_name))
            cursor = conn.cursor()

            if pd.read_sql("""SELECT * FROM tbCliente""", conn).empty:
                
                # Tabla cliente
                df_num_documento = df_file.drop_duplicates(subset=['num_documento_cliente']).copy()
                
                for index, row in df_num_documento.iterrows():
                    cursor.execute("INSERT INTO tbCliente (num_documento_cliente, tipo_documento_cliente) VALUES (?, ?);", (str(row['num_documento_cliente']), str(row['tipo_documento_cliente'])))

                # Tabla Barrio
                df_id_barrio = df_file.drop_duplicates(subset=['id_barrio']).copy()

                for index, row in df_id_barrio.iterrows():
                    cursor.execute("INSERT INTO tbBarrio (id_barrio, nombre_barrio) VALUES (?, ?);", (str(row['id_barrio']), str(row['nombre_barrio'])))

                # Tabla Tipo Tienda
                df_id_tienda =  df_file[['tipo_tienda']].drop_duplicates()

                for index, row in df_id_tienda.iterrows():
                    cursor.execute("INSERT INTO tbTipoTienda (tipo_tienda) VALUES (?);", (str(row['tipo_tienda']),))

                # Tabla Tienda
                df_tienda = df_file.drop_duplicates(subset=['codigo_tienda']).copy()


                for index, row in df_tienda.iterrows():

                    if not isinstance(row['total_compra'], datetime):
                        cursor.execute("SELECT id_TipoTienda FROM tbTipoTienda WHERE tipo_tienda = ?;", (str(row['tipo_tienda']),))
                        tipo_tienda= cursor.fetchone()[0]

                        cursor.execute("SELECT id_barrio FROM tbBarrio WHERE nombre_barrio = ?;", (str(row['nombre_barrio']),))
                        codigo_barrio= cursor.fetchone()[0]

                        # Convertir valores de tipo datetime a números
                        total_compra = pd.to_numeric(row['total_compra'], errors='coerce')

                        if codigo_barrio is not None and tipo_tienda is not None:
                            cursor.execute("INSERT INTO tbTienda (codigo_tienda, total_compra, tbTipoTienda_id_TipoTienda, tbBarrio_id_barrio) VALUES (?, ?, ?, ?);", (int(row['codigo_tienda']), total_compra, int(tipo_tienda), str(codigo_barrio)))
                        else:
                            print("Tipo tienda o Codigo de barrio incorrectos o nulos")

                # Tabla tbCliente_has_tbTienda
                df_cliente_tienda = df_file

                

                for index, row in df_cliente_tienda.iterrows():

                    cursor.execute("SELECT num_documento_cliente FROM tbCliente WHERE num_documento_cliente = ?;", (str(row['num_documento_cliente']),))
                    codigo_cliente= cursor.fetchone()

                    cursor.execute("SELECT codigo_tienda FROM tbTienda WHERE codigo_tienda = ?;", (int(row['codigo_tienda']),))
                    codigo_tienda= cursor.fetchone()

                    if codigo_tienda is not None and codigo_cliente is not None:
                        cursor.execute("SELECT COUNT(*) FROM tbCliente_has_tbTienda WHERE tbCliente_num_documento_cliente = ? AND tbTienda_codigo_tienda=?", (str(codigo_cliente[0]), int(codigo_tienda[0])))
                        count = cursor.fetchone()[0]

                        if count == 0:
                            cursor.execute("INSERT INTO tbCliente_has_tbTienda (tbCliente_num_documento_cliente, tbTienda_codigo_tienda) VALUES (?, ?);", (str(codigo_cliente[0]), int(codigo_tienda[0])))
                    
                
                # Close connection
                conn.commit()
                conn.close()

    except Exception as e:
        print('Error: ', e)
        print(traceback.format_exc())
    return None
