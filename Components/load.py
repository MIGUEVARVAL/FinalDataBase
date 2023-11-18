import pandas as pd
import os
import sqlite3
import traceback

def insert_data(df_file,db_name,db_path):
    try:
        if os.path.exists(os.path.join(db_path, db_name)):
            # Check if the tables of the data warehouse are not empty
            conn = sqlite3.connect(os.path.join(db_path, db_name))
            cursor = conn.cursor()

            if pd.read_sql("""SELECT * FROM tbCliente""", conn).empty:

                
                #Tabla cliente
                df_num_documento = df_file[['num_documento_cliente']].drop_duplicates()

                for index, row in df_num_documento.iterrows():
                  cursor.execute("INSERT INTO tbCliente (num_documento_cliente, tipo_documento_cliente) VALUES (?, ?);", (row['num_documento_cliente'], row['tipo_documento_cliente']))


                #Tabla Barrio
                df_id_barrio = df_file[['id_barrio']].drop_duplicates()

                for index, row in df_id_barrio.iterrows():
                  cursor.execute("INSERT INTO tbBarrio (id_barrio, nombre_barrio) VALUES (?, ?);", (row['id_barrio'], row['nombre_barrio']))


                #Tabla Tipo Tienda
                df_id_tienda = df_file[['tipo_tienda']].drop_duplicates()

                for index, row in df_id_tienda.iterrows():
                  cursor.execute("INSERT INTO tbTipoTienda (tipo_tienda) VALUES (?);", (row['tipo_tienda']))


                #Tabla Tienda
                df_tienda = df_file[['codigo_tienda']].drop_duplicates()

                for index, row in df_tienda.iterrows():
                  
                  cursor.execute("SELECT id_TipoTienda FROM tbTipoTienda WHERE tipo_tienda = ?;", (row['tipo_tienda']))
                  tipo_tienda= cursor.fetchone()

                  cursor.execute("SELECT id_barrio FROM tbBarrio WHERE nombre_barrio = ?;", (row['nombre_barrio']))
                  codigo_barrio= cursor.fetchone()

                  if codigo_barrio is not None and tipo_tienda is not None:

                    cursor.execute("INSERT INTO tbTienda (codigo_tienda, total_compra, tbTipoTienda_id_TipoTienda, tbBarrio_id_barrio) VALUES (?, ?, ?, ?);", (row['codigo_tienda'],row['total_compra'],tipo_tienda,codigo_barrio))
                  else:
                    print("Tipo tienda o Codigo de barrio incorrectos o nulos")


                #Tabla tbCliente_has_tbTienda
                df_cliente_tienda = df_file[['num_documento_cliente']]

                for index, row in df_cliente_tienda.iterrows():
                  
                  cursor.execute("SELECT num_documento_cliente FROM tbCliente WHERE num_documento_cliente = ?;", (row['num_documento_cliente']))
                  codigo_cliente= cursor.fetchone()

                  cursor.execute("SELECT codigo_tienda FROM tbTienda WHERE codigo_tienda = ?;", (row['codigo_tienda']))
                  codigo_tienda= cursor.fetchone()

                  if codigo_barrio is not None and tipo_tienda is not None:

                    cursor.execute("INSERT INTO tbCliente_has_tbTienda (tbCliente_num_documento_cliente, tbTienda_codigo_tienda) VALUES (?, ?);", (codigo_cliente,codigo_tienda))
                  else:
                    print("Codigo cliente o Codigo tienda incorrectos o nulos")
                
                # Close connection
                conn.commit()
                conn.close()

    except Exception as e:
        print('Error: ',e)
        print(traceback.format_exc())
    return None