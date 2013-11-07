#!/usr/bin/python
import os.path
import MySQLdb
from time import *
import datetime
import sets

TEST_AMOUNT = 50
DEBUG = True

# testing
SERVER = 'sugarcrm.amaipu.com.ar'
USER = 'rcastro'
PASS = 'hometrix'
DATABASE = 'sugarcrm'
TABLE = 'mm002_encuestas'

def conectar(server=None, user=None, p=None, db=None):
    global SERVER, USER, DATABASE, PASS
    print 'Conectando a la base %s@%s  - DB: %s ...'%(USER, SERVER,DATABASE)
    db = MySQLdb.connect(SERVER,USER,PASS,DATABASE)
    # Preparo el objeto cursor
    return db, db.cursor()

def update():

    global TABLE, SUCURSALES, DEBUG
    
    # Obtengo la fecha actual en formato ISO 8601, 'YYYY-MM-DD'
    today = datetime.date.today().isoformat()
    
    fecha = datetime.datetime.today().strftime("%Y%m%d-%H-%M")
    error_file_name = './error/calculo-RFM-'+fecha+'.txt'
    exito_file_name = './exito/calculo-RFM-'+fecha+'.txt'
    
    # Me conecto
    db, cursor = conectar()

    campo_cliente_id = {'ventas':'id_maipu_cliente'}
    campo_fecha_factura = {'ventas': 'fecha_venta'}
    tablas = {'ventas': 'mm002_ventas', 'servicios': 'mm002_turnos', 
    'accesorios': 'mm002_accesorios', 'repuestos': 'mm002_repuestos', 
    'tarjetas': 'mm002_tarjetas', 'seguros': 'mm002_seguros'}
    
    
    # Diccionarios para guardar la recencia y monto de clientes
    clientes_recencia = {}
    clientes_monto = {}
    
    # Ciclo para calculo de recencia por cada modulo
    for modulo in tablas.keys():
    
        # Campo id de cliente
        cliente = campo_cliente_id.get(modulo, 'cliente_id')
        # Campo fecha de factura
        fecha = campo_fecha_factura.get(modulo, 'fecha_facturacion')
        # Tabla de la DB
        tabla = tablas.get(modulo)
        format = "%Y%m"
        if modulo in ('tarjetas', 'seguros'):
            sql = "select %s as id_cliente, period_diff(date_format(curdate(), \"%s\" ), date_format(max(%s), \"%s\") ) as recencia,%s from %s where deleted=0 and %s != 0 group by %s"%(cliente, format, fecha, format, fecha, tabla, cliente, cliente )
        elif modulo == 'servicios':
            sql = 'select %s as id_cliente, period_diff(date_format(curdate(), \"%s\" ), date_format(max(%s), \"%s\") ) as recencia, sum(importe) as monto , id , %s from %s where deleted=0 and estado_turno in (1372, 1373) and %s != 0 group by %s'%(cliente, format, fecha, format, fecha, tabla, cliente, cliente)
        else:
            # Calculo el monto
            sql = 'select %s as id_cliente, period_diff(date_format(curdate(), \"%s\" ), date_format(max(%s), \"%s\") ) as recencia, sum(importe) as monto , id , %s from %s where deleted=0 and %s != 0 group by %s'%(cliente, format, fecha, format, fecha, tabla, cliente, cliente)
        try:
            cursor.execute(sql)
        except:
            if not (os.path.exists(error_file_name)):
                try:
                    file_error = open(error_file_name , 'w')
                except IOError:
                    os.makedirs(os.path.dirname(exito_file_name))
                    print 'Creando carpeta %s....\n'%os.path.dirname(exito_file_name)
                    file_error = open(error_file_name , 'w')
                
            file_error.write(sql+'\n')
            return
        try:
            file_exito = open(exito_file_name , 'w')
        except IOError:
            print 'Creando carpeta Exito....\n'
            os.makedirs(os.path.dirname(exito_file_name))
            file_exito = open(exito_file_name , 'w')
        file_exito.write (sql+'\n')
        datos = cursor.fetchall()
    
        for dato in datos:
            id_cliente = dato[0]
            
            if dato[1] == None:
                file_exito.write('Cliente %s: Imposible calcular recencia en modulo %s\n'%(id_cliente, modulo))
                recencia = 24
            else:
                recencia = int(dato[1])
                
            if modulo in ('tarjetas', 'seguros') or dato[2] == None:
                file_exito.write('Cliente %s: Imposible calcular monto en modulo %s\n'%(id_cliente, modulo))
                monto = 0.00
            else:
                monto = float(dato[2])
            
            file_exito.write ('\nUltima compra (%s) de %s hace meses = %s\n Monto (%s) de %s = %s'%(modulo, id_cliente, recencia, modulo, id_cliente, monto))
            
            # Actualizo el monto del cliente
            if not clientes_monto.has_key(id_cliente):
                clientes_monto[id_cliente] = monto
            else:
                clientes_monto[id_cliente] += monto
            
            # Asigno el scoring de Recencia
            if (recencia <= 6):
                if not clientes_recencia.has_key(id_cliente):
                    clientes_recencia[id_cliente] = []
                value = 5
            elif (12 >= recencia > 6):
                if not clientes_recencia.has_key(id_cliente):
                    clientes_recencia[id_cliente] = []
                value = 4
            elif (18 >= recencia > 12):
                if not clientes_recencia.has_key(id_cliente):
                    clientes_recencia[id_cliente] = []
                value = 3
            elif (24 >= recencia > 18):
                if not clientes_recencia.has_key(id_cliente):
                    clientes_recencia[id_cliente] = []
                value = 2
            elif (recencia > 24):
                if not clientes_recencia.has_key(id_cliente):
                    clientes_recencia[id_cliente] = []
                value = 1
            clientes_recencia[id_cliente].append(value)
            file_exito.write('Recencia de %s: %s\n'%(id_cliente, value))

    for key in clientes_recencia.keys():
        # Actualizo la recencia con el maximo valor obtenido
        clientes_recencia[key] = max(clientes_recencia[key])
        
        if clientes_monto[key] > 200000:
            value = 5
        elif 200000 > clientes_monto[key] >= 150000:
            value = 4
        elif 150000 > clientes_monto[key] >= 100000:
            value = 3
        elif 100000 > clientes_monto[key] >= 50000:
            value = 2
        elif 50000 > clientes_monto[key]:
            value = 1
        clientes_monto[key] = value
        # Debugg
        if clientes_recencia[key] <= 2:
            continue
        print '\nCliente %s, monto total = %s (Score = %s)'%(key, clientes_monto[key], value)
        print 'Recencia de %s = %s - Maximo = %s\n'%(key, clientes_recencia[key], clientes_recencia[key])
        if DEBUG:
            out = raw_input('Stop DEBUG? (s/n) ')
            if out != '' and out[0]=='s':
                DEBUG = False
        
        
    # -----------------  Calculo de Frecuencia  -----------------  
    # Reseteo DEBUG
    DEBUG = True
    # Diccionario para guardar la frecuencia de clientes
    clientes_frecuencia = {}
    
    # Ciclo para calculo de frecuencia por cada modulo
    for modulo in tablas.keys():
    
        # Campo id de cliente
        cliente = campo_cliente_id.get(modulo, 'cliente_id')
        # Campo fecha de factura
        fecha = campo_fecha_factura.get(modulo, 'fecha_facturacion')
        # Tabla de la DB
        tabla = tablas.get(modulo)

        sql = "select %s as id_cliente, count(*) as frecuencia from %s where period_diff(date_format(curdate(), \"%s\" ), date_format(%s, \"%s\") ) <= 24 and deleted=0 and %s is not null and %s != 0 group by %s"%(cliente, tabla, format, fecha, format, fecha, cliente, cliente)

        try:
            cursor.execute(sql)
        except:
            if not (os.path.exists(error_file_name)):
                try:
                    file_error = open(error_file_name , 'w')
                except IOError:
                    print 'Creando carpeta Error....\n'
                    os.makedirs(os.path.dirname(error_file_name))
                    file_error = open(error_file_name , 'w')
            file_error.write(sql+'\n')
            return
        #file_exito.write('Frecuencia (%s): %s\n'%(sql, modulo))
        datos = cursor.fetchall()
    
        for dato in datos:
            id_cliente = dato[0]
            
            if dato[1] == None:
                # Si es null asigno 0 a frecuencia
                frecuencia = 0
            else:
                frecuencia = int(dato[1])
                
            if not clientes_frecuencia.has_key(id_cliente):
                clientes_frecuencia[id_cliente] = frecuencia
            else:
                clientes_frecuencia[id_cliente] += frecuencia
                
            file_exito.write ('Frecuencia (%s) de %s = %s\n'%(modulo, id_cliente, frecuencia))
    
    for key in clientes_frecuencia.keys():
        
        # Asigno scoring de Frecuencia
        if (clientes_frecuencia[key]> 6):
            valor = 5
        elif (6 >= clientes_frecuencia[key] > 3 ):
            valor = 4
        elif (3 >= clientes_frecuencia[key] > 1):
            valor = 3
        elif (1 == clientes_frecuencia[key] ):
            valor = 2
        elif (clientes_frecuencia[key] < 1):
            valor = 1
        
        clientes_frecuencia[key] = valor
        if valor > 2:
            print 'Frecuencia de %s = %s - Score = %s'%(key,clientes_frecuencia[key], valor )
            if DEBUG:
                out = raw_input('Stop DEBUG? (s/n) ')
                if out != '' and out[0]=='s':
                    DEBUG = False
                
        
    
    
    # -------------- Actualizacion de tipo de clientes --------------
    
    # Armo un conjunto con todos los id de maipu de los clientes
    id_maipu_actualizar = sets.Set(clientes_frecuencia.keys()+clientes_recencia.keys())

    # Obtengo todos los id de maipu de los clientes en la tabla contacts_cstm
    sql = 'select id_maipu_c as id_cliente, id from contacts, contacts_cstm where contacts.id = contacts_cstm.id_c and contacts.deleted=0'
    
    try:
        cursor.execute(sql)
    except:
        if not (os.path.exists(error_file_name)):
            try:
                file_error = open(error_file_name , 'w')
            except IOError:
                os.makedirs(os.path.dirname(exito_file_name))
                print 'Creando carpeta %s....\n'%os.path.dirname(exito_file_name)
                file_error = open(error_file_name , 'w')
            
        file_error.write(sql+'\n')
        return
    file_exito.write('UPDATE: %s\n'%sql)
    contactos = cursor.fetchall()
    count = 0
    id_null = 0
    print "Cantidad de contactos seleccionados = ",len(contactos)
    
    for contacto in contactos:
        
        id = contacto[0]
        if None == id:
            id_null += 1
            continue
        id_sugar = contacto[1]
        msg = ''
        msg = 'ID: %s '%id
        #if id not in id_maipu_actualizar :
        
        if id not in (clientes_frecuencia.keys() + clientes_recencia.keys()):
            # Actualizar cliente con tipo D
            tipo = 'D'
            msg += '- tipo: %s - motivo: no se encontraron datos del cliente'%tipo
        else:
            # Seteo valores de R, F y M
            if id in clientes_recencia.keys():
                R = clientes_recencia[id]
            else:
                R = 1
            
            if id in clientes_frecuencia.keys():
                F = clientes_frecuencia[id]
            else:
                F = 1
            
            if id in clientes_monto.keys():
                M = clientes_monto[id]
            else:
                M = 1
                
            # Aplico formula RFM
            score = R*0.45 + F*0.2 + M*0.35
            
            if 5 >= score  >= 4:
                tipo= 'A'
            elif 4 >  score  >= 3:
                tipo= 'B'
            elif 3 >  score  >= 2:
                tipo= 'C'
            elif 2 >  score  >= 1:
                tipo= 'D'
            
            
            print '\nR = %s, F = %s, M = %s  - Score = %s - Tipo Cliente = %s'%(R, F, M, score, tipo)
            msg += '- tipo: %s - motivo: Score = %s (R = %s, F = %s, M = %s) '%(tipo, score, R, F, M)
        
        file_exito.write(msg+'\n')
        # Hago update o insert segun corresponda
        sql = 'select id_c from contacts_cstm where id_c = \'%s\' '%id_sugar
        try:
            cursor.execute(sql)
        except:
            if not (os.path.exists(error_file_name)):
                try:
                    file_error = open(error_file_name , 'w')
                except IOError:
                    os.makedirs(os.path.dirname(exito_file_name))
                    print 'Creando carpeta %s....\n'%os.path.dirname(exito_file_name)
                    file_error = open(error_file_name , 'w')
            file_error.write(sql+'\n')
            return
        
        cant=cursor.fetchall()
        
        if len(cant) == 0:
            # No tengo un registro que relacione al contacto con los datos cstm
            print 'INSERT: CANT = 0 '
            #file_exito.write('INSERT: CANT = %s \n'%len(cant) )
            sql = 'INSERT contacts_cstm (id_c, tipo_cliente, fecha_tipo_cliente_c) VALUES (\'%s\', \'%s\', \'%s\') '%(id_sugar, tipo, today)
        elif len(cant) == 1:
            print 'UPDATE: CANT = 1 '
            #file_exito.write('UPDATE: CANT = %s \n'%len(cant))
            # Actualizo tipo de cliente en la base
            sql = 'update contacts_cstm set tipo_cliente_c =\'%s\', fecha_tipo_cliente_c = \'%s\' where id_maipu_c = %s'%(tipo, today, id)
        else:
            print 'Error: CANT = %s'%len(cant)
            file_exito.write('ERROR: CANT = %s \n'%len(cant))
            continue
        file_exito.write(sql+'\n')

        
        # logeo la actualizacion
        print 'Actualizando contacto ',count
        #if count > TEST_AMOUNT:
        #    print 'Test end....Bye!'
            #return
            #sys.exit(0)
        #continue
        count += 1
        try:
            cursor.execute(sql)
            # Hago un commit del cambio en la base
            db.commit()
        except:
            if not (os.path.exists(error_file_name)):
                try:
                    file_error = open(error_file_name , 'w')
                except IOError:
                    os.makedirs(os.path.dirname(error_file_name))
                    print 'Creando carpeta %s....\n'%os.path.dirname(error_file_name)
                    file_error = open(error_file_name , 'w')
                
            file_error.write(sql+'\n')
            #return
            db.rollback()
        file_exito.write(sql+'\n')
        

    file_exito.write('Total: %s\n'%count)
    if (os.path.exists(error_file_name)):
        file_error.close()
    if (os.path.exists(exito_file_name)):
        file_exito.close()
    print 'Total de updates = ',count
    print 'Total de id nulls = ', id_null
    # disconnect from server
    db.close()

if __name__ == '__main__':
    update()
    
