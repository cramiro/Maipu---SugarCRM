#!/usr/bin/env python
# -*- coding: utf-8 -*-
from geopy import geocoders
import datetime
import time
import random
import util

COLON_4045 = (-31.3983531 , -64.2331642)

def procesar():

    g = geocoders.Google('ABQIAAAAtGLFHYz6bfKeWA7GGQ8fzRSfYWwldeQTn-MMsG6oDuo7Kf7ifBSD9Yv-SCgMoxscszNjCTLqX9vU2g')
    # Me conecto
    db, cursor = util.conectar()
    
    fecha = datetime.datetime.today().strftime("%Y%m%d-%H-%M")
    file_error = open(util.ensure_dir('./error/geocoders/log-error-geocoder-%s.txt'%(fecha)),'w')
    file_exito = open(util.ensure_dir('./exito/geocoders/log-exito-geocoder-%s.txt'%(fecha) ),'w')
    file_etapa = open('log-etapa-geocoder.txt', 'a')
    
    etapa = '2010-06%'
    #sql = "\
    #    SELECT \
    #        DISTINCT mm002_ventas_contacts_c.mm002_vente4f9ontacts_ida \
    #    FROM \
    #        mm002_ventas, mm002_ventas_contacts_c \
    #    WHERE \
    #        mm002_ventas.id=mm002_ventas_contacts_c.mm002_vent6709_ventas_idb AND\
    #        mm002_ventas.deleted =0 AND mm002_ventas.fecha_venta like '%s'"%(etapa)
    
    sql = "SELECT contacts_cstm.id_c FROM contacts_cstm WHERE contacts_cstm.estado_localizacion_c = \'\' or contacts_cstm.estado_localizacion_c is null"
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
    
    # Inicializo contadores
    count_ok = 0
    count_default = 0
    count_not_cordoba = 0
    count_mas1 = 0
    count_error = 0
    count_done = 0
    print "Cantidad de datos = %s"%len(datos)

    iter = 0
    for dato in datos:
        iter += 1
        id_contacto = dato[0]
        
        sql = "\
        SELECT \
            contacts.primary_address_street , IFNULL(IF(contacts_cstm.domicilio_uno_numero_c REGEXP '^[0-9]+' = 0, -1, contacts_cstm.domicilio_uno_numero_c), -1) AS altura,\
             IFNULL(contacts.primary_address_city , 'NO-CITY' ) AS ciudad, contacts_cstm.estado_localizacion_c AS estado, \
            IFNULL(contacts.primary_address_state  , 'NO-STATE' ) AS provincia\
        FROM contacts, contacts_cstm \
        WHERE \
            contacts.deleted=0 AND contacts.id = '"+str(id_contacto)+"' AND contacts.id=contacts_cstm.id_c "
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
            ciudad = (direccion[2].split('(')[0].strip()).replace(u'\xd1', 'N').replace(u'\xf1','n')
            provincia = (direccion[4].strip()).replace(u'\xd1', 'N').replace(u'\xf1','n')

            if (ciudad.upper() == 'NO-CITY' ) or (ciudad.upper() != 'CORDOBA'):
                count_not_cordoba += 1
                continue
            
            if (provincia.upper() == 'NO-STATE' ):
                #count_not_cordoba += 1
                provincia = ""
                #continue
            
            # Chequeo que el estado sea distinto de OK o DEFAULT
            estado = direccion[-1]
            if (estado in ['OK', 'DEFAULT'] ):
                file_exito.write("Contacto: %s - Estado Localizacion: %s\n"%(id_contacto, estado))
                count_done += 1
                continue
            
            # Obtengo calle, altura, provincia y pais
            if (direccion[0] != None):
                calle = direccion[0].replace(u'\xd1', 'N').replace(u'\xf1','n')
                if (int(direccion[1]) != -1):
                    altura = int(direccion[1])
                else:
                    altura = ""
            else:
                calle = "Av. Colon"
                altura = 4045
            
            pais = "Argentina"
            
            # Genero un tiempo de espera aleatorio
            sleep_time = random.randint(0,30)
            print "BUSCO: %s %s, %s, %s, %s"%(calle, altura, provincia, ciudad, pais) 
            print "ANALIZADOS %s REGISTROS DE %s. SLEEPING %s seconds."%(iter, len(datos), sleep_time)
            
            time.sleep(sleep_time)
            
            # Intento obtener localizacion de la direccion en (lat, long)
            try:
                place, (lat, long) = g.geocode( "%s %s, %s, %s, %s"%(calle, altura, provincia, ciudad, pais) )
                estado_localizacion = 'OK'
                count_ok += 1
            except Exception, e:
                print "Error: geocode -> %s"%(e)
                
                # Si el error es por encontrar mas de un lugar, marco por defecto Colon 4045
                #if "Didn't find exactly one placemark" in str(e):
                place, (lat, long) = "Av. Colon 4045", COLON_4045
                estado_localizacion = 'DEFAULT'
                count_default +=1
                file_error.write('Error: geocode: %s for ID=%s - Dir: %s\n - Estado: %s'%(e, id_contacto, ', '.join([calle.encode('ascii', 'replace'),str(altura),ciudad, provincia, pais]), estado_localizacion ))
                
            # Si encontro Cordoba o un lugar fuera de Cordoba, seteo COLON 4045
            
            if 'CORDOBA' == ciudad and place == u'C\xf3rdoba, Cordoba, Argentina' or u'C\xf3rdoba, Cordoba, Argentina' not in place:
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
            print "Se encontraron %s direcciones para %s\n"%(len(direcciones), id_contacto)
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