import os
import MySQLdb

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