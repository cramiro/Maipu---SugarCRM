#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
from geopy import geocoders
import datetime
import time
import random
import os

SERVER = 'sugarcrm.amaipu.com.ar'
USER = 'rcastro'
PASS = 'hometrix'
DATABASE = 'sugarcrm'

COLON_4045 = (-31.3983531 , -64.2331642)

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
    print 'Conectando a la base %s@%s  - DB: %s ...'%(USER, SERVER,DATABASE)
    db = MySQLdb.connect(SERVER,USER,PASS,DATABASE,use_unicode=True)
    db.set_character_set('utf8')
    #print 'Conexion exitosa!'
    # Preparo el objeto cursor
    return db, db.cursor()


def procesar():

    g = geocoders.Google('ABQIAAAAtGLFHYz6bfKeWA7GGQ8fzRSfYWwldeQTn-MMsG6oDuo7Kf7ifBSD9Yv-SCgMoxscszNjCTLqX9vU2g')
    # Me conecto
    db, cursor = conectar()
    
    fecha = datetime.datetime.today().strftime("%Y%m%d-%H-%M")
    file_error = open(ensure_dir('./error/geocoders/log-error-geocoder-%s.txt'%(fecha)),'w')
    file_exito = open(ensure_dir('./exito/geocoders/log-exito-geocoder-%s.txt'%(fecha) ),'w')
    file_etapa = open('log-etapa-geocoder.txt', 'a')
    
    etapa = '2010-%'
    sql = "\
        SELECT \
            DISTINCT mm002_ventas_contacts_c.mm002_vente4f9ontacts_ida \
        FROM \
            mm002_ventas, mm002_ventas_contacts_c \
        WHERE \
            mm002_ventas.id=mm002_ventas_contacts_c.mm002_vent6709_ventas_idb AND\
            mm002_ventas.deleted =0 AND mm002_ventas.fecha_venta like '%s'"%(etapa)
    
    # Guardo el periodo que se filtro para las ventas
    file_etapa.write("%s\n"%(etapa))
    
    try:
        cursor.execute(sql)
    except:
        file_error.write(sql+'\n')
    
    # Guardo consulta hecha
    file_exito.write (sql+'\n')
    
    # Obtengo el resultado de la consulta
    datos = cursor.fetchall()
    
    count_ok = 0
    count_default = 0
    count_not_cordoba = 0
    count_mas1 = 0
    count_error = 0
    count_done = 0
    print "Cantidad de ventas = %s"%len(datos)

    iter = 0
    for dato in datos:
        iter += 1
        id_contacto = dato[0]
        
        # Si 'estado_localizacion_c' != 'OK' armo direccion
        # y trato de geolocalizar
        sql = "\
        SELECT \
            contacts.alt_address_street , IFNULL(IF(contacts_cstm.domicilio_dos_numero_c REGEXP '^[0-9]+' = 0, -1, contacts_cstm.domicilio_uno_numero_c), -1) AS altura,\
             IFNULL(contacts.alt_address_city , 'NO-CITY' )AS ciudad, contacts_cstm.estado_localizacion_c AS estado \
        FROM contacts, contacts_cstm \
        WHERE \
            contacts.deleted=0 AND contacts.id = '"+str(id_contacto)+"' AND contacts.id=contacts_cstm.id_c \
            AND  contacts_cstm.estado_localizacion_c = 'DEFAULT'"
        try:
            cursor.execute(sql)
        except:
            file_error.write(sql+'\n')
        
        # Obtengo datos con la direccion
        direcciones = cursor.fetchall()
        
        if len(direcciones) > 1:
            count_mas1 += 1
            file_error.write("Se encontraron %s direcciones para %s\n"%(len(direcciones), id_contacto))
            continue
        elif len(direcciones) == 1:
            direccion = direcciones[0]
            
            # Chequeo que la ciudad sea CORDOBA
            ciudad = direccion[2].split('(')[0].strip()
            
            if (ciudad.upper() == 'NO-CITY' or ciudad.upper() != 'CORDOBA'):
                count_not_cordoba += 1
                continue
            
            # Chequeo que el estado sea distinto de OK o DEFAULT
            estado = direccion[-1]
            if (estado in ['OK'] ):
                file_exito.write("Contacto: %s - Estado Localizacion: %s\n"%(id_contacto, estado))
                count_done += 1
                continue
            
            # Obtengo calle, altura, provincia y pais
            if (direccion[0] == None):
                count_error += 1
                continue
            calle = direccion[0].replace(u'\xd1', 'N').replace(u'\xf1','n')
            
            if (int(direccion[1]) != -1):
                altura = int(direccion[1])
            else:
                altura = ""
                
            provincia = ciudad
            pais = "Argentina"
            
            # Genero un tiempo de espera aleatorio
            sleep_time = random.randint(0,5)
            print "Sleeping %s seconds before geocoding this: %s...\n"%(sleep_time, ', '.join([calle,str(altura),ciudad,pais]))
            print "ANALIZADOS %s REGISTROS DE %s."%(iter, len(datos))
            
            time.sleep(sleep_time)
            
            
            # Intento obtener localizacion de la direccion en (lat, long)
            try:
                place, (lat, long) = g.geocode( "%s, %s, %s, %s"%(calle, altura, ciudad, pais) )
                estado_localizacion = 'OK'
                count_ok += 1
            except Exception, e:
                print "Error: geocode -> %s"%(e)
                
                # Si el error es por encontrar mas de un lugar, marco por defecto Colon 4045
                #if "Didn't find exactly one placemark" in str(e):
                place, (lat, long) = "Av. Colon 4045", COLON_4045
                estado_localizacion = 'DEFAULT'
                count_default +=1
                file_error.write('Error: geocode: %s for ID=%s - Dir: %s\n - Estado: %s'%(e, id_contacto, ', '.join([calle.encode('ascii', 'replace'),str(altura),ciudad,pais]), estado_localizacion ))
                
            # Si encontro Cordoba o un lugar fuera de Cordoba, seteo COLON 4045
            if place == u'C\xf3rdoba, Cordoba, Argentina' or u'C\xf3rdoba, Cordoba, Argentina' not in place:
                print 'Place not found!...searching new address'
                place, (lat, long) = "Av. Colon 4045", COLON_4045
                estado_localizacion = 'DEFAULT'
                count_default += 1
                print 'New Place = %s - (%s,%s)'%(place, lat, long)
            
            # Logging y UPDATE a la base de datos
            file_exito.write ('Contacto: %s - Dir: %s - (%s,%s)\n'%(id_contacto, place.encode('ascii', 'replace'), lat, long))
            print "%s: Dir.: %s - (%s,%s)\n\n"%(estado_localizacion, place, lat, long)
            sql = "UPDATE \
            contacts_cstm SET latitud_c = '%s', longitud_c='%s', estado_localizacion_c = '%s' \
            WHERE \
            contacts_cstm.id_c='%s' "%(lat, long, estado_localizacion, id_contacto)
            #print sql
            #continue
            try:
                # Execute the SQL command
                cursor.execute(sql)
                # Commit your changes in the database
                db.commit()
            except:
                # Rollback in case there is any error
                db.rollback()
        else:
            file_error.write("Error: ID contacto= %s - Direcciones= %s\n"%(id_contacto, str(direcciones)))
            #print "Se encontraron %s direcciones para %s\n"%(len(direcciones), id_contacto)
            count_error += 1
            #print sql
            continue
        
    # Imprimo resultados
    print "Direcciones marcadas OK: %s"%(count_ok)
    print "Direcciones marcadas DEFAULT: %s"%(count_default)
    print "Direcciones previamente actualizadas: %s"%(count_done )
    print "Direcciones Fuera de CORDOBA: %s"%(count_not_cordoba )
    print "Contactos con mas de 1 direccion: %s"%(count_mas1)
    print "Direcciones con error geocoder: %s"%(count_error)
    # disconnect from server
    db.close()
    file_exito.write ('--'*5+' FIN '+'--'*5+'\n')
    
    file_error.close()
    file_exito.close()
    file_etapa.close()
    

if __name__ == '__main__':
    procesar()