# -*- coding: utf-8 -*-
from base import base
import logging
import datetime
import os
import psycopg2, psycopg2.extras

da = datetime.date.today().strftime("%Y-%m-%d")
thisfolder = os.path.dirname(os.path.abspath(__file__))
logfile = thisfolder + '/log' + da + '.log'
print (('LOG FILE:' + logfile))

logging.basicConfig(filename = logfile, level=logging.INFO)
dati = datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S")
logging.info(dati + ' - Inicio de envio  ')

control = base(logging)
    #control = base()
codigoprov = control.codprov
print (("PROVINCIA: ", codigoprov))

#("salta_minas_union.gid", "geom", "area", "expediente", "expediente2", "descripcio")
def insertData(tabla, wkt, expte, expte2, descripcio):
    try:
        control.cursor.execute("""INSERT INTO salta_minas_union
            ("gid", "geom", "expediente", "expediente2", "descripcio")
            VALUES (DEFAULT, '%s' , '%s', '%s', '%s')""" % (wkt , str(expte) , str(expte2), str(descripcio)))
    except psycopg2.Error as e:
        pass
        print ((e.pgerror))
    control.conn.commit()
        #self.conn.close()

##################33CONSULTA NORMAL DE UNION CON IGUAL#################
#provincia = control.cursor.execute(""" SET search_path TO sml, public;
                                    #SELECT salta_minas_p2.expediente,
                                    #ST_Multi(ST_Union(ST_Transform(salta_minas_p2.geom, 4326), ST_Transform(salta_minas_p3.geom, 4326)))
                                    #AS geom_union
                                    #FROM salta_minas_p2
                                    #INNER JOIN salta_minas_p3
                                    #ON (salta_minas_p2.expediente = salta_minas_p3.expediente)
                                    #INNER JOIN salta_minas_p4
                                    #ON (salta_minas_p3.expediente = salta_minas_p4.expediente)""")
#############################################################################

##################UNION CON EXPTE TRUNCADO BASE : saltaTresFunion #################
provincia = control.cursor.execute(""" SET search_path TO sml, public;
                                    SELECT salta_minas_p2.area AS area, salta_minas_p2.descripcio AS descripcio,
                                    salta_minas_p2.expediente AS expediente,
                                    ST_Multi(ST_Union(ST_Transform(salta_minas_p2.geom, 4326), ST_Transform(salta_minas_p3.geom, 4326)))
                                    AS geom, salta_minas_p3.expediente AS expediente2
                                    FROM salta_minas_p2
                                    INNER JOIN salta_minas_p3
                                    ON substring(salta_minas_p2.expediente from 1 for 8) = substring(salta_minas_p3.expediente from 1 for 8)
                                    """)
#############################################################################


union_tablas = control.cursor.fetchall()
for info_conex in union_tablas:
    print (("EXPEDIENTED UNION: ", info_conex[0], info_conex[1], str(info_conex[2]), str(info_conex[4])))
    print (("RESULTADO GEOMETRICO: ", str(info_conex[3])))
    insertData('salta_minas_union', info_conex[3], info_conex[4], info_conex[2], info_conex[1])
    #resp = control.control_caso_c(info_conex[2], info_conex[3], info_conex[1])
    #if resp:
        #print (("TRUE: SE INSERTARON CORRECTAMENTE"))
    #else:
        #print (("FALSE: NO SE INSERTARON DATOS"))
        #logging.info(dati + ' - No se insertaron datos de la tabla: '+ str(info_conex[3]))

control.Desconect()

#A CONTINUACION UN SELECT TOTAL PARA PROBAR
"""
SELECT *
FROM salta_minas_p2
INNER JOIN salta_minas_p3
ON salta_minas_p2.expediente = salta_minas_p3.expediente
INNER JOIN salta_minas_p4
ON salta_minas_p3.expediente = salta_minas_p4.expediente;
"""
