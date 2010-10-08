import sugar
import crm_config
import monitor_config
import logging
import datetime
import calendar
from time import *
import os.path

######################
# Con este script se busca turnos sin encuesta y se eliminan.
######################

LOG_FILENAME = 'example.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

END = False
CANTIDAD = 500
OFFSET = 0

def procesar(instancia):
    
    global END, OFFSET, CANTIDAD
    
    fecha = strftime("%Y%m%d%H%M%S",gmtime())
    con_encuestas = './turnos_con_encuesta_'+fecha+'.txt'
    sin_encuestas = './turnos_sin_encuesta_'+fecha+'.txt'
    
    
    log_sin_encuesta = open( sin_encuestas , 'w' )
    log_con_encuesta = open( con_encuestas , 'w' )
    
    while not END:
        
        if (raw_input('Salir? s/n ')[0] =='s'):
            return False
        print "Buscando turnos en rango (%s, %s)..."%(OFFSET, OFFSET+CANTIDAD)
        
        # Busco un lote nuevo de datos de turnos
        busq = instancia.modulos['mm002_Turnos'].buscar(inicio = OFFSET, cantidad = CANTIDAD)
        
        # Veo si tengo que terminar el bucle de busqueda
        if len(busq) < CANTIDAD:
            print "Ultima busqueda.\n"
            END = True
            
        # Actualizo el OFFSET
        OFFSET += CANTIDAD
        
        # Para cada turno del lote
        for objeto in busq:
            
            # Recupero estado del turno
            estado_turno = objeto.obtener_campo('estado_turno').a_sugar()
            
            # Si no es 'auto listo' o 'auto ratirado', continuo
            if (estado_turno not in ['1372','1373']):
                print "Turo con estado != Auto Listo, Auto Retirado\n"
                continue
            
            # Recupero el ID del turno (a_sugar() devuelve string)
            operacion_id = objeto.obtener_campo('turno_id').a_sugar()
            
            print "Buscando encuestas para turno %s "%(operacion_id)
            # Busco encuestas con el id del turno
            search = instancia.modulos['mm002_Encuestas'].buscar(turno_id=str(operacion_id))
            if (len( search ) == 0):
                # Si no hay encuestas, elimino el turno
                #objeto.modificar_campo('deleted', 1)
                # Guardo datos en log correspondiente
                log_sin_encuesta.write('%s\n'%operacion_id)
                print "Eliminando Turno con ID = %s"%operacion_id
                print "Guandando ID de turno en archivo %s"%log_sin_encuesta.name
                #objeto.grabar()
            else:
                # Guardo datos en log correspondiente
                log_con_encuesta.write('%s\n'%operacion_id)
                print "Guandando ID de turno en archivo %s"%log_con_encuesta.name
            
    log_sin_encuesta.close() 
    log_con_encuesta.close()
    return True


def obtener_instancia():
    # Me conecto a la instancia de SugarCRM.
    instancia = sugar.InstanciaSugar(crm_config.WSDL_URL, crm_config.USUARIO,
                    crm_config.CLAVE, ['mm002_Turnos', 'mm002_Marcas',
                                        'mm002_Modelo', 'mm002_Encuestas',
                                        'Contacts', 'Calls'],
                    crm_config.LDAP_KEY, crm_config.LDAP_IV)
    return instancia

if __name__ == '__main__':
    import sys

    instancia = obtener_instancia()
    
    procesar( instancia )
    
