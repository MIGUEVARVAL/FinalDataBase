import os
import sqlite3
import traceback
import datetime as dt


def create_db(db_name="tiendas.db",db_path='DB'):

  try:
        # Create connection to sqlite3
        conn = sqlite3.connect(os.path.join(db_path,db_name))
        cursor = conn.cursor()
        # Creation of tables

        cursor.execute("""CREATE TABLE IF NOT EXISTS `tbCliente` (
            `num_documento_cliente` TEXT NOT NULL,
            `tipo_documento_cliente` TEXT NOT NULL,
            PRIMARY KEY (`num_documento_cliente`)
            );""")
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS `tbBarrio` (
            `id_barrio` TEXT NOT NULL,
            `nombre_barrio` TEXT NOT NULL,
            PRIMARY KEY (`id_barrio`)
            );""")
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS `tbTipoTienda` (
            `id_TipoTienda` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            `tipo_tienda` TEXT NOT NULL
            ); """)

        cursor.execute("""CREATE TABLE IF NOT EXISTS `tbTienda` (
            `codigo_tienda` INTEGER NOT NULL,
            `total_compra` REAL NOT NULL,
            `tbTipoTienda_id_TipoTienda` INTEGER NOT NULL,
            `tbBarrio_id_barrio` TEXT NOT NULL,
            PRIMARY KEY (`codigo_tienda`, `tbTipoTienda_id_TipoTienda`, `tbBarrio_id_barrio`),
            FOREIGN KEY (`tbTipoTienda_id_TipoTienda`) REFERENCES `tbTipoTienda` (`id_TipoTienda`),
            FOREIGN KEY (`tbBarrio_id_barrio`) REFERENCES `tbBarrio` (`id_barrio`)
            );""")
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS `tbCliente_has_tbTienda` (
            `tbCliente_num_documento_cliente` TEXT NOT NULL,
            `tbTienda_codigo_tienda` INTEGER NOT NULL,
            PRIMARY KEY (`tbCliente_num_documento_cliente`, `tbTienda_codigo_tienda`),
            FOREIGN KEY (`tbCliente_num_documento_cliente`) REFERENCES `tbCliente` (`num_documento_cliente`),
            FOREIGN KEY (`tbTienda_codigo_tienda`) REFERENCES `tbTienda` (`codigo_tienda`)
            );""")

        # Close connection
        conn.commit()
        conn.close()
  except Exception as e:
      print('Error: ',e )
      print(traceback.format_exc())
  return None
