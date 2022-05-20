# -*- coding: utf-8 -*-
from base import base
import logging
import datetime
import os
import psycopg2, psycopg2.extras

da = datetime.date.today().strftime("%Y-%m-%d")
thisfolder = os.path.dirname(os.path.abspath(__file__))
logfile = thisfolder + '/log/log' + da + '.log'
print (('LOG FILE:' + logfile))

logging.basicConfig(filename = logfile, level=logging.INFO)
dati = datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S")
#logging.info(dati + ' - Inicio de envio  ')

class concat(object):
    #codprov = 0
    def __init__(self):
        super(concat, self).__init__()
        #self.codprov = prov

    def newtable(self, objbase, proced):
        print(("CREANDO LA NUEVA TABLA "+proced+"_gis"))
        #provincia = objbase.cursor.execute("""DROP TABLE IF EXISTS minas_gis""")
        #geogtext     varchar(700),
        #geom        geometry,
        #srid        integer,
        try:
            query_sec = """CREATE SEQUENCE IF NOT EXISTS
                            public.%s_gis_gid_seq minvalue 1 increment 1;"""
            provincia = objbase.cursor.execute(query_sec % proced)
            query_sec_init = """SELECT setval('%s_gis_gid_seq', 1)"""
            provincia = objbase.cursor.execute(query_sec_init % proced)
            query_create_table = """CREATE TABLE IF NOT EXISTS %s_gis (
                        gid         integer CONSTRAINT %s_firstkey PRIMARY KEY DEFAULT nextval('%s_gis_gid_seq'),
                        expediente  varchar(254) NOT NULL,
                        nombre      varchar(254),
                        titular     varchar(254),
                        mineral     varchar(254),
                        estado_legal     varchar(254),
                        geog        geography(geometry),
                        write_date timestamp without time zone,
                        create_date timestamp without time zone)"""
            provincia = objbase.cursor.execute(query_create_table % (proced, proced, proced))
        except (Exception, psycopg2.DatabaseError) as error :
            print (("CREACION DE TABLA " + proced+"_gis " + "Error PostgreSQL ", error))
        return True

    def deltable(self, objbase, tablename):
        query = """DROP TABLE IF EXISTS %s_gis"""
        try:
            provincia = objbase.cursor.execute(query % tablename)
            res = True
        except (Exception, psycopg2.DatabaseError) as error :
            res = False
        return res

    def existtable(self, objbase, tablename):
        query = """SELECT exists(SELECT 1 FROM %s )"""
        try:
            provincia = objbase.cursor.execute(query % tablename)
            res = True
        except (Exception, psycopg2.DatabaseError) as error :
            res = False
            #print ("Error PostgreSQL", error)
        #if res:
            #print ("EXISTE LA TABLA")
        #else:
            #print("NO EXISTE LA TABLA")
        return res


    def datatable(objbase, tablename):
        query = """SELECT exists(SELECT 1 FROM %s )"""
        try:
            provincia = objbase.cursor.execute(query % tablename)
            res = True
        except (Exception, psycopg2.DatabaseError) as error :
            res = False
            print ("Error PostgreSQL", error)
        if res:
            c = objbase.cursor.rowcount
            print (("Numero de registros seleccionados en " + tabla + ": ", c))
        if c > 0:
            return True
        else:
            return False

    def disableTable(self, objbase, tablename, codprov, logging, motivo):
        print (("DESACTIVANDO TABLA"))
        query = """UPDATE public.codprov_conex SET activo=False
                    WHERE codigoprov=%s AND tabla_local='%s';"""
        try:
            res = objbase.cursor.execute(query % (codprov, tablename))
            res = True
            logging.info("La tabla "+tablename+" fue desactivada de la configuracion: " + motivo)
        except (Exception, psycopg2.DatabaseError) as error :
            res = False
            print ("Error PostgreSQL", error)
        return res

    def validColumnNames(self, objbase, tablename):
        obligatory = ["geom", "expediente", "nombre", "titular", "mineral",
                        "write_date", "create_date", "estado_legal"]
        for field in obligatory:
            try:
                query = """SELECT * FROM %s WHERE %s IS NULL"""
                res = objbase.cursor.execute(query % (tablename, field))
                res = True
            except (Exception, psycopg2.DatabaseError) as error :
                res = False
                break
                #print ("Error PostgreSQL", error)
        return res

    def nullexped(self, objbase, tablename):
        query = """SELECT * FROM %s WHERE expediente IS NULL"""
        try:
            provincia = objbase.cursor.execute(query % tablename)
            res = True
        except (Exception, psycopg2.DatabaseError) as error :
            res = False
            print ("Error PostgreSQL", error)
        if res:
            c = objbase.cursor.rowcount
            #print (("Numero de registros con faltante de EXPEDIENTE EN: " + tablename + " ES: ", c))
        return c


    def insertDataFromSelectSINGEOG(self, objbase, tablename, metodo, srid):
        query = """INSERT INTO %s_temp (expediente, nombre, titular, mineral, geom, srid)
                                    SELECT expediente, nombre, titular, mineral, geom, %s FROM %s
                                    ;"""
        try:
            provincia = objbase.cursor.execute(query % (metodo, srid, tablename, tablename))
            r = True
        except (Exception, psycopg2.DatabaseError) as error :
            print ("Error PostgreSQL", error)
            r = False
        return r

    def insertDataFromSelect(self, objbase, tablename, metodo, srid):
        #La funcion ST_Transform requiere como segundo parametro el valor 4326 para transformar a
        # coordenadas geograficas
        #query = """INSERT INTO %s_temp (expediente, nombre, titular, mineral, geom, srid, geog)
                                    #SELECT expediente, nombre, titular, mineral, geom, %s, ST_Transform(ST_SetSRID(geom, %s), 4326) FROM %s
                                    #WHERE %s.expediente <> '';"""
        query = """INSERT INTO %s_gis (expediente, nombre, titular, mineral, estado_legal, geog)
                                    SELECT expediente, nombre, titular, mineral, estado_legal, ST_Transform(ST_SetSRID(geom, %s), 4326) FROM %s
                                    WHERE %s.expediente <> '';"""
        try:
            #provincia = objbase.cursor.execute(query % (metodo, srid, srid, tablename, tablename))
            provincia = objbase.cursor.execute(query % (metodo, srid, tablename, tablename))
            r = True
        except (Exception, psycopg2.DatabaseError) as error :
            print ("Error PostgreSQL", error)
            r = False
        return r

    def obtainProcedV0(self, objbase):
        provincia = objbase.cursor.execute(""" SELECT * FROM public.codprocedimientos""")
        proced = objbase.cursor.fetchall()
        return proced

    def obtainProced(self, objbase, codigoprov):
        provincia = objbase.cursor.execute(""" SELECT DISTINCT
            codprocedimientos.codigoproced, codprocedimientos.nombre
            FROM public.codprov_conex
            INNER JOIN public.codprocedimientos
            ON (codprov_conex.codigoproced = codprocedimientos.codigoproced)
            WHERE codprov_conex.activo = true
            AND codprov_conex.codigoprov=%s""" % codigoprov)
        proced = objbase.cursor.fetchall()
        return proced
        #for info_conex in prov_conex:
            #self.deltable(info_conex[2])
        #return True

    def obtainLocalTables(self, objbase, codigoprov, proced):
        provincia = objbase.cursor.execute(""" SELECT * FROM public.codprov_conex
                INNER JOIN public.codfajas ON (codprov_conex.codigofaja = codfajas.codigofaja)
                WHERE
                codprov_conex.activo=true AND codprov_conex.codigoprov=%s AND codprov_conex.codigoproced=%s """ % (codigoprov, proced))
        tables = objbase.cursor.fetchall()
        return tables

    def geomToGeog():
        query = """ALTER TABLE minas_f1
            ALTER COLUMN geom TYPE geometry(Polygon, 4326)
            USING ST_Transform(ST_SetSRID(geom, 4258), 4326);"""
        try:
            provincia = objbase.cursor.execute(query % tablename)
            res = True
        except (Exception, psycopg2.DatabaseError) as error:
            res = False
        return res

    def loop(self):
        logging.basicConfig(filename = logfile, level=logging.INFO)
        dati = datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S")
        logging.info(dati + ' - Inicio de Concatenacion  ')
        control = base(logging)
        codigoprov = control.codprov
        print (("PROVINCIA: ", codigoprov))
        proced = self.obtainProced(control, codigoprov)
        cuenta = 1
        for pro in proced:
            print ((str(cuenta) + " - PROCEDIMIENTO: ", pro[0], pro[1]))
            cuenta = cuenta + 1
            cod_proced = pro[0]
            nombre_proced = pro[1]
            self.deltable(control, nombre_proced)
            self.newtable(control, nombre_proced)
            prov_conex = self.obtainLocalTables(control, codigoprov, cod_proced)
            for info_conex in prov_conex:
                print (("OBTENIENDO DATOS PARA ENVIAR: ", info_conex[2], info_conex[3], info_conex[4], info_conex[10]))
                sridPosgar94 = info_conex[10]
                if not self.existtable(control, info_conex[4]):
                    print (("NO EXISTE LA TABLA " + info_conex[4] + " Y SERA DESACTIVADA DE LA CONFIGURACION."))
                    self.disableTable(control, info_conex[4], codigoprov, logging, "-No existe la tabla-")
                else:
                    validcol = self.validColumnNames(control, info_conex[4])
                    if not validcol:
                        print (("LA TABLA " + info_conex[4] + " NO TIENE LOS CAMPOS SOLICITADOS Y SERA DESACTIVADA"))
                        self.disableTable(control, info_conex[4], codigoprov, logging, "-No tiene las columnas correctas-")
                    nullexped = self.nullexped(control, info_conex[4])
                    if nullexped > 0:
                        print (("Numero de registros con faltante de EXPEDIENTE EN: " + info_conex[4] + " ES: ", nullexped))
                        logging.info('La tabla ' + info_conex[4] + " tiene " + str(nullexped) + " registros con faltante del dato EXPEDIENTE.")
                    resp = self.insertDataFromSelect(control, info_conex[4], nombre_proced, sridPosgar94)
                    #resp = False
                    if resp:
                        print (("TRUE: SE INSERTARON CORRECTAMENTE"))
                    else:
                        print (("FALSE: NO SE INSERTARON DATOS"))
                        logging.info(dati + ' - No se insertaron datos de la tabla: '+ str(info_conex[4]))
        control.Desconect()
        del control
        dati = datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S")
        logging.info(dati + ' - Fin de envio')


thisfolder = os.path.dirname(os.path.abspath(__file__))
initfile = os.path.join(thisfolder, 'config.ini')



#SOLO PARA DESARROLLO
#control = concat(20)
#control.loop()



#A CONTINUACION UN SELECT TOTAL PARA PROBAR
"""
SELECT *
FROM salta_minas_p2
INNER JOIN salta_minas_p3
ON salta_minas_p2.expediente = salta_minas_p3.expediente
INNER JOIN salta_minas_p4
ON salta_minas_p3.expediente = salta_minas_p4.expediente;
"""
