import MySQLdb
import random
import string
import sys

SERVER = 'sugarcrm.amaipu.com.ar'
USER = 'rcastro'
PASS = 'hometrix'
DATABASE = 'sugarcrm'

TABLE = 'mm002_encuesta'

SUCURSALES = ['\'COLON AUDI\'','\'VENTAS ESPECIALES VW\'','\'VENTAS ESPECIALES FORD\'','\'COLOR CHEVROLET\'',\
	'\'COLON FORD\'', '\'MUNDO MAIPU\'', '\'USADOS\'', '\'NUNIES CHERY\'', '\'GERENCIA\'','\'SARMIENTO FORD\'',\
	'\'COLON VW\'', '\'SABATTINI VW\'', '\'LAND ROVER\'', '\'SABATTINI\'', '\'COLON SEAT\'']

cantidad = 200

MARCAS = [ '\'Audi\'', '\'Chery\'','\'Chevrolet\'', '\'Ford\'', '\'Land \
		Rover\'','\'MundoMaipu\'','\'Seat\'','\'Usados\'','\'Volkswagen\'']

CAMPOS_SERV = """(id,tipo_encuesta, date_modified,serv_gral_grado_satisfaccion, serv_turno_horario_turno_bueno, 
	serv_turno_facil_contactarse, serv_turno_horario_respetado, serv_asesor_grado_satisfaccion, serv_asesor_educado_amable, 
	serv_asesor_entiende_necesidad, serv_asesor_con_conocimiento, serv_asesor_mantuvo_informado, serv_entrega_cumple_plazo, 
	serv_entrega_factura_clara, serv_entrega_monto_acorde, serv_taller_buena_reparacion, serv_taller_ultima_visita, 
	serv_gral_observaciones, serv_gral_nos_recomendaria, serv_grntia_vendrasin_garantia, serv_grntia_auto_en_garantia, 
	serv_grntia_por_que_no_vendra)"""


grado_sat = ['\'5\'','\'4\'','\'3\'','\'2\'','\'1\'']
si_no = ['\'SI\'', '\'NO\'', '\'NO_INFORMA\'']


def main(tipo, cantidad):
	print 'Conectando a la base %s@%s  - Tabla: %s ...'%(USER,SERVER,DATABASE)
	# Me conecto a la base
	db, cursor = conectar()
	
	# Sentencia INSERT sql
	if tipo == 0:
		campos = CAMPOS_SERV
	elif tipo == 1:
		campos = CAMPOS_VENTA
		
	table = TABLE
	campos_list = campos.strip(' ()')
	campos_list = get_campos(campos_list)

	for i in range(cantidad):
		val = ''
		for c in campos_list:
			if (c.find('satis') != -1):
				if (random.random() > 0.75):
					val += random.choice(grado_sat)
				else:
					val += random.choice(['\'5\'','\'4\'','\'3\''])
			elif (c == 'id'):
				val += '\''+gen_id()+'\''
			elif (c == 'date_modified'):
				val += '\''+gen_date()+'\''
			elif (c == 'tipo_encuesta'):
				if tipo == 1:
					val += '\'1\''
				else:
					val += '\'0\''
			else:
				val += random.choice(si_no)
			val += ', '
	
		val = val.rstrip(', ')

		values = '( '+ val + ')'

		sql = 'insert into ' + table +' '+campos+' values '+values 
		print 'Sentencia INSERT armada'


		try:
			print 'Ejecutando sentencia SQL...'
			# Ejecuto comando sql
			cursor.execute(sql)
		
			# Hago un commit del cambio en la base
			db.commit()
	
		except:
			print 'Error al ejecutar la sentencia SQL!'
			print sql
			db.rollback()
	


	print 'Cerrando conexion con la base'
	# Cierro conexion
	db.close()



def conectar(server=None, user=None, p=None, db=None):

	print "Default: %s@%s:%s"%(USER,SERVER,DATABASE)
	default = raw_input('Usar valores por defecto para conexion? (s/n) ')[0] == 's'
	if default:
		print 'Conectando a la base %s@%s  - Tabla: %s ...'%(SERVER,USER,DATABASE)
		db = MySQLdb.connect(SERVER,USER,PASS,DATABASE)
		print 'Conexion exitosa!'
		# Preparo el objeto cursor
		return db, db.cursor()
		            
	if server == None:
		server = raw_input('Ingrese Servidor: ')
	if user == None:
		user = raw_input('Ingrese Usuario: ')
		use_pass = raw_input('Usa password? (s/n) ')[0] == 's'
	if use_pass and p == None:
		import getpass
		p = getpass.getpass('Password: ')
	elif not use_pass:
		p = ''
	if db == None:
		db = raw_input('Ingrese el nombre de la base de datos: ')
		
	print 'Conectando a la base %s@%s  - Tabla: %s ...'%(user,server,db)
	
	# Me conecto a la base
	db = MySQLdb.connect(server, user, p, db)
	print 'Conexion exitosa!'
	# Preparo el objeto cursor
	
	return db, db.cursor()

def get_users_id(cant, offset):
	db, cursor = conectar()
	sql = 'select id from contacts limit '+cant+' offset '+offset
	try:
		print 'Ejecutando sentencia SQL...'
		# Ejecuto comando sql
		cursor.execute(sql)
	except:
		print 'Error al ejecutar la sentencia SQL!'
		print sql
		return -1
	resultado=cursor.fetchall()
	return resultado

def update():
    global TABLE, SUCURSALES
    
    # Me conecto
    db, cursor = conectar()
        
    tabla = 'mm002_encuestas'
    campo = 'grupo_orden'
        
    # Obtengo el ID de encuesta, ID de turno y si el turno es o no garantia
    sql= 'select mm002_encuestas_cstm.id_c, mm002_encuestas_cstm.sucursal_descripcion_c from mm002_encuestas_cstm '

    try:
        cursor.execute(sql)
    except:
        print 'Error al ejecutar la sentencia SQL!'
        print sql
        
    resultado=cursor.fetchall()
    
    count=0
    no_null= 0
    for registro in resultado:
        
        id = registro[0]
        sucursal = registro[1]
        
        if (sucursal != '' and sucursal != None):
            sql = 'UPDATE mm002_encuestas SET sucursal_descripcion=\'%s\' WHERE id = \'%s\' '%(sucursal, id)
            no_null +=1
        else:
            sql = 'UPDATE mm002_encuestas SET sucursal_descripcion=null WHERE id = \'%s\' '%(id)
        #print sql+'\n'
        count += 1

        #print sql+'\n'
        #continue
        
        try:
            print 'Ejecutando sentencia SQL...'
            # Ejecuto comando sql
            cursor.execute(sql)
            # Hago un commit del cambio en la base
            db.commit()
        except:
            print 'Error al ejecutar la sentencia SQL!'
            print sql
        
        db.rollback()
    
    print '\nActualizados %s registros= %s No null + %s NULL'%(count,no_null, count-no_null )
        
def get_campos(campos):
	campos = campos.split(',')
	aux = []
	
	for c in campos:
		aux.append(c.strip('\n '))
	
	return  aux


def gen_id():

    id = ''.join(random.Random().sample(string.letters+string.digits,8))
    id += '-'
    id += ''.join(random.Random().sample(string.letters+string.digits,4))
    id +='-'
    id += ''.join(random.Random().sample(string.letters+string.digits,4))
    id +='-'
    id += ''.join(random.Random().sample(string.letters+string.digits,4))
    id +='-'
    id += ''.join(random.Random().sample(string.letters+string.digits,12))
    
    return id
                                        
def gen_date(y0=None,y1=None,m0=None,m1=None,d0=None,d1=None):
	if y0==None:
		y0=1960
	if y1==None:
		y1=2010
	if m0==None:
		m0=01
	if m1==None:
		m1=13
	if d0==None:
		d0=01
	if d1==None:
		d1=30
	years= range(y0,y1,1)
	months = range(m0,m1,1)
	days = range(d0,d1,1)
	ret = ''
	ret += str(random.choice(years))
	ret += '-'
	ret += str(random.choice(months))
	ret += '-'
	ret += str(random.choice(days))
	return ret

def update_ventas(file_name=None):
    global TABLE, SUCURSALES
    # Me conecto
    db, cursor = conectar()
    
    if file_name != None:
        try:
            file = open(file_name,'r')
        except:
            print 'Error: Imposible abrir %s'%file_name
            return
    else:
        print 'Error: no especifica el nombre de archivo'
        return
    
    tabla = 'mm002_ventas'
    campo = 'operacion_id'
    campo_update = 'importe'
    
    # Bucle para actualizar datos
    for line in file.readlines():
        datos = line.strip().rstrip(';').split(';')
        
        operacion_id = datos[0]
        importe = datos[-1].strip()
        #print '\nMarca: %s  - Modelo: %s  -  Contacto: %s'%(datos[4], datos[6], datos[2])
        sql = 'UPDATE %s SET %s = %s WHERE %s = \'%s\' AND deleted=0'%(tabla, campo_update, importe, campo, operacion_id)
        
        #print sql
        #continue
        
        try:
            print 'Ejecutando sentencia SQL...'
            # Ejecuto comando sql
            cursor.execute(sql)
            # Hago un commit del cambio en la base
            db.commit()
        except:
            print 'Error al ejecutar la sentencia SQL!'
            print sql
        
        db.rollback()
        
    
if __name__ == '__main__':
    #print sys.argv
    #sys.exit(0)

    if len(sys.argv) > 1:
        x= raw_input('Update VENTAS FROM FILE \'%s\'? (s/n)'%sys.argv[-1])[0]=='s'
    
        if x:
            update_ventas(sys.argv[-1])
            sys.exit(0)
        
    x= raw_input('Update? (s/n) ')[0]=='s'
    
    if x:
        update()
        sys.exit(0)


