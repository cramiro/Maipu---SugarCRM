#!/usr/bin/python
import os
import MySQLdb
from time import *
import datetime

SERVER = 'sugarcrm.amaipu.com.ar'
USER = 'rcastro'
PASS = 'hometrix'
DATABASE = 'sugarcrm'


def ensure_dir(f):
    # Obtengo la ruta de directorios
    dirs = os.path.dirname(f)
    
    # Chequeo que la ruta exista
    if not os.path.exists(dirs):
        # Si no existe, la creo
        print 'Creando path: ',dirs
        os.makedirs(dirs)
    return f

def conectar(server=None, user=None, p=None, db=None):
    global SERVER, USER, DATABASE, PASS
    print 'Conectando a la base %s@%s  - Tabla: %s ...'%(SERVER,USER,DATABASE)
    db = MySQLdb.connect(SERVER,USER,PASS,DATABASE,use_unicode=True)
    db.set_character_set('utf8')
    #print 'Conexion exitosa!'
    # Preparo el objeto cursor
    return db, db.cursor()

def procesar(pathname):
    
    db, cursor = conectar()

    # Abro el archivo de datos.
    arch_datos = open(pathname)
    lineas = arch_datos.readlines()
    
    # Archivos de log
    fecha = datetime.datetime.today().strftime("%Y%m%d-%H-%M")
    file_error = open(ensure_dir('./error/clientes/log-error-clientes-especiales-%s.txt'%(fecha)),'w')
    file_exito = open(ensure_dir('./exito/clientes/log-exito-clientes-especiales-%s.txt'%(fecha) ),'w')
        
    clientes_existentes = 0
    clientes_inexistentes = 0
    clientes_duplicados = 0
    
    iter = 0
    for linea in lineas:
        
        datos = linea.split(';')
        id_maipu = int(datos[0])
        tipo = datos[1]
        
        # Obtengo el ID de encuesta, ID de turno y si el turno es o no garantia
        sql= 'select contacts_cstm.id_c from contacts_cstm where id_maipu_c = %s'%(id_maipu)

        try:
            cursor.execute(sql)
        except:
            print 'Error al ejecutar la sentencia SQL!'
            print sql
        
        resultado=cursor.fetchall()
        ERROR = True
        if len(resultado) == 1 :
            ERROR = False
            cliente= resultado[0]
            id_cliente = cliente[0]
            clientes_existentes += 1 
            sql = 'UPDATE contacts_cstm SET tipo_cliente_c = \'%s\' WHERE id_c = \'%s\''%(tipo, id_cliente )
            msg = 'OK: Existe cliente con ID = %s - tipo = %s'%(id_maipu, tipo )
            file_exito.write(msg)
            print msg

        elif len(resultado) == 0 :
            clientes_inexistentes += 1
            msg = 'ERROR: No existe cliente con ID = %s - tipo = %s'%(id_maipu, tipo )
            file_error.write(msg)
            print msg
            continue
            
        elif len(resultado) >1 :
            clientes_duplicados += 1
            msg = 'ERROR: Existen %s clientes con ID = %s - tipo = %s'%(len(resultado), id_maipu, tipo )
            file_error.write(msg)
            print msg
            continue
        
        # debug
        print sql
        continue
        
        try:
            cursor.execute(sql)
            # Hago un commit del cambio en la base
            db.commit()
        except:
            msg = 'ERROR: Ejecutando %s'%(sql)
            file_error.write(msg)
            print msg
            db.rollback()
            return
        
        msg = 'ACTUALIZADO: cliente ID = %s - tipo = %s'%(id_maipu, tipo )
        file_exito.write(msg)
        print msg
            
        
    print 'Cliente existentes = ', clientes_existentes
    print 'Cliente inexistentes = ', clientes_inexistentes
    print 'Cliente duplicados = ', clientes_duplicados
    
    # Cierro archivos de log
    file_exito.close()
    file_error.close()
    
    # disconnect from server
    db.close()

    
    
if __name__ == '__main__':
    import sys

    if len(sys.argv) >= 2:
        procesar( sys.argv[1] )
    else:
        "ERROR: El programa necesita un parametro!"
