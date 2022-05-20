#PRECONDICIONES DE INSTALACION EN EL SO
#$ pip install numpy
#$ pip install geos
#Shapely pide instalar geos
#$ pip install shapely
#FIN PRECONDICIONES

import psycopg2, psycopg2.extras
# import urllib2  # Solo python 2
import urllib.request
import json
import re
import os
# import timer
#import numpy as np
import geojson
from shapely.geometry import shape
#from json import dumps
# from main import *
import ConfigParser

from jsonwsp.client import ServiceConnection
from reg import reg

thisfolder = os.path.dirname(os.path.abspath(__file__))
initfile = os.path.join(thisfolder, 'config.ini')

config = ConfigParser.ConfigParser()
config.read(initfile)


## EliminaDatos()
## Selecciona()
class base(object):
    cursor = False
    conn = False
    codprov = 0
    host = ''
    port = 8049
    ipserver = ''
    vpnuse = False
    def __init__(self, logging):
        self.codprov = config.get('Publicacion', 'codprov')
        self.ipserver = str(config.get('Publicacion', 'ipserv'))
        self.vpnuse = config.getboolean('Publicacion', 'vpn')
        #DATABASE
        nameDB = config.get('DB', 'nameDB')
        userDB = config.get('DB', 'userDB')
        passDB = config.get('DB', 'passDB')
        hostDB = config.get('DB', 'hostDB')
        print ("CONECTANDO DB--....")
        self.conn = psycopg2.connect(dbname=nameDB, user=userDB, password=passDB, host=hostDB)
        try:
            self.cursor = self.conn.cursor()
        except psycopg2.DatabaseError as e:
            logging.info("Error de conexion con la BD local")
            print ((e.pgerror))
        if self.cursor:
            logging.info("Conexion correcta con la BD local: " + nameDB)
            print (("Conexion correcta con la BD local: " + nameDB))
        else:
            print ("Error de conexion con la BD local")
        #return self

    def _desconectar(self, cnx):
        cnx.close()

    def insert_multipolig(self, wkt, mutipoligon):
        print (("HOLA: "+str(wkt)))
        #self._conectar()
        try:
            self.cursor.execute("""insert into MultiPolygon_Sample(geom, polig_text) VALUES ( ST_GeogFromText('%s') , '%s')""" % (str(wkt), mutipoligon))
        except psycopg2.Error as e:
            pass
            print ((e.pgerror))
        self.conn.commit()
        self.conn.close()

    """"
    def getDataWebService(self, url_ws):
        req = urllib2.Request(url_ws)
        opener = urllib2.build_opener()
        f = opener.open(req)
        json_local = json.loads(f.read())
        return json_local
    """

    #Convierte la informacion de Multipoligono mediante Json en formato WKT,
    # en un tipo Geografico apto para ser guardado en PostGis (Tipo geographics)
    def _convMultiGeog(self, cordenadas_multipo):
        #NUEVA LIBRERIA
        # https://pypi.org/project/Shapely/1.4.0/
        s = dumps(cordenadas_multipo)
        # Convert to geojson.geometry.Polygon
        #https://pypi.org/project/geojson/
        g1 = geojson.loads(s)
        # Feed to shape() to convert to shapely.geometry.polygon.Polygon
        # This will invoke its __geo_interface__ (https://gist.github.com/sgillies/2217756)
        #http://toblerity.org/shapely/manual.html#polygons
        g2 = shape(g1)
        # Now it's very easy to get a WKT/WKB representation
        return g2.wkt
        #g2.wkb
        #FIN NUEVA LIBRERIA

    # Elimina datos de la tabla Cateos de BackUp
    def EliminaDatos(self, tabla, cod_prov):
        try:
            #queryDelete = """ DELETE FROM  """ + tabla + """ WHERE codprov = %s """
            queryDelete = """ DELETE FROM  """ + tabla
            self.cursor.execute(queryDelete)
            self.conn.commit()
            rows_deleted = self.cursor.rowcount
            print (("Se eliminaron " + rows_deleted + " registros de la Base de la tabla: " + tabla))
        except Exception as e:
            raise

# Inserta los registros traidos del WebService de la provincia de Jujuy
    def InsertCateos(self, tabla, json_local):
        #print(("INSERTANDO DATOS DE CANTERAS"))
        # Recorre el JSON segun el totalFeatures almacena en variables e inserta en tabla Cateos
        for i in range(0,int(json_local['totalFeatures'])):
            expediente = json_local['features'][i]['properties']['expediente']
            nombre = json_local['features'][i]['properties']['nombre']
            titular = json_local['features'][i]['properties']['titular_ac']
            mineral = json_local['features'][i]['properties']['tipo_miner']
            #A continuacion obtener la parte del Json que contiene el poligono
            #tipo de poligono y puntos en formato WKT
            cordenadas_multipo = json_local['features'][i]['geometry']
            #Solicitar conversion
            poligGeo = self._convMultiGeog(cordenadas_multipo)
            codProv = "1"
            query = """ INSERT INTO public."""+tabla+""" ("geom", "expediente", "nombre", "titular", "mineral", "codprov")
                    VALUES(ST_GeogFromText(%s),%s,%s,%s,%s,%s); """
            data = (str(poligGeo), expediente,nombre,titular,mineral,codProv)
            self.cursor.execute(query,data)
            self.conn.commit()
        query1 = """ SELECT "expediente", "nombre", "titular", "mineral", "codprov" FROM public."""+tabla+"""; """
        self.cursor.execute(query1)
        row = self.cursor.rowcount
        print (("Se Insertaron" + row + " registros nuevos en la tabla " + tabla + " ..."))
        return True

        ########CLIENTE WEB SERVICE -- CASO "B" ################
#####################################################
    def insertDataClientB(self, tabla, wkt, expte, nombre, titular, mineral, codprov):
        try:
            self.cursor.execute("""INSERT INTO %s (geom, expediente, nombre, titular, mineral, codprov) VALUES ( ST_GeogFromText('%s') , '%s', '%s', '%s', '%s', '%s')""" % (tabla, str(wkt), expte, nombre, titular, mineral, codprov))
        except psycopg2.Error as e:
            pass
            print ((e.pgerror))
        self.conn.commit()
        #self.conn.close()

    def recorreDataWebServiceB(self, json_local, tabla, cod_prov):
        # Recorre el JSON segun el totalFeatures almacena en variables e inserta en tabla Cateos
        #pu = base()
        for dato in json_local:
            #print (( 'Insert Expte.:' + str(dato['expediente'])))
            resp = self.insertDataClientB( tabla, dato['geom'], dato['expediente'],  dato['nombre'], dato['titular'], dato['mineral'], cod_prov)
            #expediente = json_local['features'][i]['properties']['expediente']
        #self.desconectar()

    # Selecciona de la Base de Datos de Nacion e Inserta  la Base de Datos de BackUp
    def Save(self):
        x = self.Selecciona()
        for value in x:
            query = """ INSERT INTO public.vacantes("expediente", "nombre", "titular", "mineral", "codprov") VALUES(%s,%s,%s,%s,%s); """
            data = (value['expediente'],value['nombre'],value['titular'],value['mineral'],value['codprov'])
            self.cursor.execute(query,data)
            pass
        pass
    # Inserta y Desconecta de la Base de Datos de BackUp

    def Selecciona(self):
        query1 = """ SELECT "expediente","nombre","titular","mineral","codprov" FROM public.vacantes; """
        x = self.cursor.execute(query1)
        c = self.cursor.rowcount
        print (("Numero de Row Seleccionados: " + c))
        row = self.cursor.fetchone()
        columns = [column[0] for column in cur.description]
        results = []
        for row in self.cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results

    def Desconect(self):
        self.conn.commit()
        self.conn.close()
        pass

    def cargaTextoGeog(self, tabla, codprov):
        query1 = """ UPDATE public.%s_gis
                        SET geogtext = ST_AsText(geom);"""
        self.cursor.execute(query1 % tabla)
        return True


    def seleccionaPublicacion(self, tabla, codprov):
        #res = self.cargaTextoGeog(tabla, codprov)
        #if not res:
            #print (("FALLO LA FUNCION DE TRANSFORMACION"))
            #return True
        #Modificacion 11/10/2018
        #Se modifico la tabla de lectura agregandole el postfijo '_gis',
        # de acuerdo con el nuevo modelo de concatenacion de fajas
        query1 = """ SELECT "expediente","nombre","titular","mineral",
                    20, ST_AsText("geog") AS "geom", "estado_legal",
                    to_char("create_date", 'YYYY-MM-DD HH24:MI:SS') AS create_date,
                    to_char("write_date", 'YYYY-MM-DD HH24:MI:SS') AS write_date
                    FROM public.%s_gis;"""
        self.cursor.execute(query1 % tabla)
        c = self.cursor.rowcount
        print (("Numero de registros seleccionados en " + tabla + ": ", c))
        results = []
        rows = self.cursor.fetchall()
        if c is 0:
            return False
        for row in rows:
            r = reg(self.cursor, row)
            results.append({'expediente' : r.expediente, 'nombre' : r.nombre,
                            'titular' : r.titular, 'mineral' : r.mineral,
                            'codprov' : codprov, 'geom' : r.geom, 'estado_legal' : r.estado_legal,
                            'create_date' : r.create_date, 'write_date' : r.write_date})
        jsresult = json.dumps(results)
        #print (("LINEA PARA ENVIAR"))
        #print ((str(jsresult)))
        return jsresult

    def sendDataWebServiceC(self, url_ws, table, codprov):
        print (("dentro del SEND DATA - TABLA: "+ table))
        print (("COD PROV: "+ str(codprov)))
        jsonData = self.seleccionaPublicacion(table, codprov)
        if jsonData is False:
            return False
        serv_ip = self.ipserver
        print (("CONECTANDO AL SERVIDOR: " + serv_ip))
        try:
            connection = ServiceConnection(serv_ip, 8049, '/gisminero')
            connection.initialize()
        except ValueError:
            print (("No se Puede establecer conexion con el servidor o inicielizar el servicio..." + serv_ip))
        servicio = connection.get_method('reception')
        r = servicio(codprov, table, str(jsonData))
        #r = servicio(jsonData)
        #print str(json.loads(r))
        return True

    def control_caso_c(self, link, tablaActualiza, cod_prov):
        #Arguments:
        #link: Se utiliza en caso de conexion del servidor de nacion hacia las provincias
        #tablaActualiza: Nombre del procedimiento del cual se enviaran los datos
        #cod_prov: COdigo de provincia
        resp = self.sendDataWebServiceC(link, tablaActualiza, cod_prov)
        return resp

    def deltable(self, tablename):
        query = """DROP TABLE IF EXISTS %s_gis"""
        print (("BORRANDO:" + query))
        try:
            provincia = self.cursor.execute(query % tablename)
            res = True
        except (Exception, psycopg2.DatabaseError) as error:
            print (("ERROR AL BORRAR LA TABLA TEMPORTAL: " + error))
            res = False
        return res

    def delTempTables(self, codigoprov):
        provincia = self.cursor.execute(""" SELECT DISTINCT
            codprov_conex.link, codprov_conex.codigoprov,
            codprocedimientos.nombre
            FROM public.codprov_conex
            INNER JOIN public.codprocedimientos ON (codprov_conex.codigoproced = codprocedimientos.codigoproced)
            WHERE codprov_conex.codigoprov=%s""" % codigoprov)
        prov_conex = self.cursor.fetchall()
        for info_conex in prov_conex:
            self.deltable(info_conex[2])
        return True



# base()






#EJEMPLO:
#http://www.codedirection.com/convert-multipolygon-geometry/
# multipolig_string : Es una variable que sirve para cargar el multipoligono en formato WKT, ver:
#http://www.postgis.net/docs/PostGIS_Special_Functions_Index.html
