#!/usr/bin/python
import os
import MySQLdb
from time import *
import datetime

SERVER = 'sugarcrm.amaipu.com.ar'
USER = 'rcastro'
PASS = 'hometrix'
DATABASE = 'sugarcrm'

def conectar(server=None, user=None, p=None, db=None):
    global SERVER, USER, DATABASE, PASS
    print 'Conectando a la base %s@%s  - Tabla: %s ...'%(SERVER,USER,DATABASE)
    db = MySQLdb.connect(SERVER,USER,PASS,DATABASE,use_unicode=True)
    db.set_character_set('utf8')
    
    # Preparo el objeto cursor
    return db, db.cursor()

def procesar(pathname):
    
    # Abro el archivo de datos.
    arch_datos = open(pathname)
    lineas = arch_datos.readlines()
    
    db, cursor = conectar()
    
    # Archivos de log
    fecha = datetime.datetime.today().strftime("%Y%m%d-%H-%M")
    error_file_name = open('./error/grupo-orden-'+fecha+'.txt', 'w')
    exito_file_name = open('./exito/grupo-orden-'+fecha+'.txt', 'w')
    
    ventas_actualizadas = 0
    ventas_inexistentes = 0
    ventas_duplicadas = 0
    for linea in lineas:
        datos = linea.split(';')
        id_venta = int(datos[0])
        grupo = int(datos[1])
        orden = int(datos[2])
        
        # Obtengo el ID de encuesta, ID de turno y si el turno es o no garantia
        sql= 'SELECT id FROM mm002_ventas WHERE deleted=0 AND mm002_ventas.operacion_id =%s'%(id_venta)
        
        try:
            cursor.execute(sql)
        except:
            print 'Error al ejecutar la sentencia SQL!'
            print sql
        
        resultado=cursor.fetchall()
        
        if len(resultado) == 1 :
            
            id_sugar = resultado[0][0]
            ventas_actualizadas += 1 
            sql = 'UPDATE mm002_ventas SET plan_grupo = %s, plan_orden = %s WHERE id = \'%s\''%(grupo, orden, id_sugar )
            exito_file_name.write("Actualizado: Operacion ID = %s - Grupo = %s - Orden = %s - ID VENTA SUGAR=%s \n"%(id_venta, grupo, orden, id_sugar) )
            print "OK: Operacion ID = %s - Grupo = %s - Orden = %s \n"%(id_venta, grupo, orden) 
            
        elif len(resultado) == 0 :
            
            ventas_inexistentes += 1
            error_file_name.write("Inexistente: Operacion ID = %s \n"%(id_venta) )
            print 'ERROR: No existe operacion ID = %s \n'%(id_venta)
           
        elif len(resultado) >1 :
            
            ventas_duplicadas += 1
            error_file_name.write("Duplicado: Operacion ID = %s \n"%(id_venta) )
            print 'ERROR: Duplicada operacion ID = %s \n'%(id_venta)
        
        # debug
        #continue
        
        try:
            cursor.execute(sql)
            # Hago un commit del cambio en la base
            db.commit()
        except:
            print 'Error ejecutando %s'%sql
            return
            db.rollback()
        print sql
        
            
    print 'Ventas actualizadas = ', ventas_actualizadas
    print 'Ventas inexistentes = ', ventas_inexistentes
    print 'Ventas duplicados = ', ventas_duplicadas
    
    error_file_name.close()
    exito_file_name.close()
    db.close()
    
    
if __name__ == '__main__':
    import sys

    procesar(sys.argv[1])

