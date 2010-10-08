import MySQLdb

SERVER = 'sugarcrm.amaipu.com.ar'
USER = 'rcastro'
PASS = 'hometrix'
DATABASE = 'sugarcrm'

def conectar(server=None, user=None, p=None, db=None):
    global SERVER, USER, DATABASE, PASS
    print 'Conectando a la base %s@%s  - DB: %s ...'%(USER, SERVER,DATABASE)
    db = MySQLdb.connect(SERVER,USER,PASS,DATABASE)
    #print 'Conexion exitosa!'
    # Preparo el objeto cursor
    return db, db.cursor()

def procesar():

    # Abro archivo TXT
    file = open('TXT-GERENT-SERV-2010-09-14.TXT', 'r')

    # Me conecto
    db, cursor = conectar()
    
    for line in file.readlines():
        
        datos = line.split(';')
        
        # Integer
        ID = int( datos[0].strip() )
        
        # Varchars
        CHASIS = datos[1].strip()
        MODELO = datos[2].strip()
        RAZON_SOCIAL = datos[3].strip()
        CALLE = datos[4].strip()
        NRO  = datos[5].strip()
        LOCALIDAD = datos[6].strip()
        CODIGO_POSTAL = datos[7].strip()
        TELEFONO = datos[8].strip()
        FECHA_ENTREGA = datos[9].strip()
        FECHA_ENVIO = datos[10].strip()
        FECHA_RECEPCION = datos[11].strip()
        TIPO_ENVIO = datos[12].strip()
        # INTEGERS
        if int( datos[13].strip() ) == 0:
            ID_TURNO1 = 'null'
        else:
            ID_TURNO1 = int( datos[13].strip() )
        
        if int( datos[14].strip() ) == 0:
            ID_TURNO2 = 'null'
        else:
            ID_TURNO2 = int( datos[14].strip() )

        if int( datos[15].strip() ) == 0:
            ID_TURNO3 = 'null'
        else:
            ID_TURNO3 = int( datos[15].strip() )
        
        if int( datos[16].strip() ) == 0:
            ID_TURNO4 = 'null'
        else:
            ID_TURNO4 = int( datos[16].strip() )
        
        if int( datos[17].strip() ) == 0:
            ID_TURNO5 = 'NULL'
        else:
            ID_TURNO5 = int( datos[17].strip() )
        
        # Prepare SQL query to INSERT a record into the database.
        sql = 'INSERT INTO listado_gerentes_servicio( id_listado, chasis, modelo, razon_social,\
            calle, numero, localidad, codigo_postal, telefono, fecha_entrega, fecha_envio,\
            fecha_recepcion, tipo_envio, id_turno1, id_turno2, id_turno3, id_turno4, id_turno5, fecha_creacion)\
         VALUES ( %s, \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\',\
            \'%s\', \'%s\', \'%s\', %s, %s, %s, %s, %s, "2010-09-14")'%(ID, CHASIS, MODELO , RAZON_SOCIAL, CALLE, NRO, LOCALIDAD, CODIGO_POSTAL,\
 TELEFONO, FECHA_ENTREGA, FECHA_ENVIO, FECHA_RECEPCION, TIPO_ENVIO, ID_TURNO1, ID_TURNO2, ID_TURNO3, ID_TURNO4, ID_TURNO5)

        #print sql
        #raw_input('enter ')
        #continue
        try:
            # Execute the SQL command
            cursor.execute(sql)
            # Commit your changes in the database
            db.commit()
        except:
            # Rollback in case there is any error
            db.rollback()
    # disconnect from server
    db.close()

if __name__ == '__main__':
    procesar()