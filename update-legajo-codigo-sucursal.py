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
    error_file_name = open('./error/vendedor-sucursal-'+fecha+'.txt', 'w')
    exito_file_name = open('./exito/vendedor-sucursal-'+fecha+'.txt', 'w')
    
    vendedor_actualizados = 0
    vendedor_inexistentes = 0
    vendedor_duplicados = 0
    
    for linea in lineas:
        datos = linea.split(';')
        legajo = int(datos[0])
        id_sucursal = int(datos[1])
        
        # Obtengo el ID de encuesta, ID de turno y si el turno es o no garantia
        sql= 'SELECT id FROM users WHERE deleted = 0 and users.description= \'%s\''%(legajo)
        
        try:
            cursor.execute(sql)
        except:
            print 'Error al ejecutar la sentencia SQL!'
            print sql
        
        resultado=cursor.fetchall()
        
        if len(resultado) == 1 :
            
            id_sugar = resultado[0][0]
            vendedor_actualizados += 1 
            sql = 'UPDATE users SET department = \'%s\' WHERE id = \'%s\''%(id_sucursal, id_sugar )
            exito_file_name.write("Actualizado: Legajo = %s - Sucursal = %s - ID USUARIO SUGAR=%s \n"%(legajo, id_sucursal, id_sugar) )
            print "Actualizado: Legajo = %s - Sucursal = %s - ID USUARIO SUGAR=%s \n"%(legajo, id_sucursal, id_sugar) 
            
        elif len(resultado) == 0 :
            
            vendedor_inexistentes += 1
            error_file_name.write("Inexistente: Legajo = %s \n"%(legajo) )
            print 'ERROR: No existe usuario con legajo = %s \n'%(legajo)
           
        elif len(resultado) >1 :
            
            vendedor_duplicados += 1
            error_file_name.write("Duplicado: Legajo = %s \n"%(legajo) )
            print 'ERROR: Duplicada Legajo = %s \n'%(legajo)
        
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
        
            
    print 'Usuarios actualizadas = ', vendedor_actualizados
    print 'Usuarios inexistentes = ', vendedor_inexistentes
    print 'Usuarios duplicados = ', vendedor_duplicados 
    
    error_file_name.close()
    exito_file_name.close()
    db.close()
    
    
if __name__ == '__main__':
    import sys

    procesar(sys.argv[1])

