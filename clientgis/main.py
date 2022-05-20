# -*- coding: utf-8 -*-
from ssh.catssh import catssh
from base import base
from timer import InfiniteTimer
import os
import time

import psycopg2, psycopg2.extras
import ConfigParser
import logging
#import datetime
from poligon.concat import concat
from datetime import datetime as dt
from datetime import timedelta

def vpnconnect():
    ssh = catssh()
    tried = 0
    if ssh.checkactive():
        tried = 3
    while tried < 3:
        conexvpn = ssh.connect()
        if not conexvpn:
            logging.info('Archivo .ovpn NO encontrado en el directorio: ../ssh ')
            print (("Archivo .ovpn NO encontrado en el directorio: ../ssh"))
            tried = 3 #Si hay conexion activa, se fuerza la salida
        time.sleep(10)
        if ssh.checkactive():
            tried = 3 #Si hay conexion activa, se fuerza la salida
        tried = tried + 1
    if not ssh.checkactive():
        print (("NO HAY CONEXION SSH ACTIVA"))
        return False
    else:
        return True

def vpndisconnect():
    ssh = catssh()
    ssh.disconnect()
    del ssh


def delLogs():
    days_to_subtract = 10
    thisfolder = os.path.dirname(os.path.abspath(__file__))
    i = 0
    listaQueda = []
    while i < days_to_subtract:
        da = dt.today() - timedelta(days=i)
        logfile = thisfolder + '/log/client' + da.strftime("%Y-%m-%d") + '.log'
        listaQueda.append(logfile)
        i += 1
    ########################3
    thisfolder = os.path.dirname(os.path.abspath(__file__))
    cant = 0
    for file in os.listdir(thisfolder + '/log'):
        if file.endswith(".log"):
            cant = cant + 1
            fileovpn = os.path.join(thisfolder + '/log', file)
            if fileovpn in listaQueda:
                print(("NO BORRAR: "+fileovpn))
            else:
                os.remove(fileovpn)
                print (('BORRANDO!: '+fileovpn))
    return True
delLogs()

da = dt.today().strftime("%Y-%m-%d")
thisfolder = os.path.dirname(os.path.abspath(__file__))
logfile = thisfolder + '/log/client' + da + '.log'
print (('LOG FILE:' + logfile))

def tick():
    actualTime = dt.now().strftime("%H:%M")
    if actualTime != horaEnvio:
        print (("NOOOO ES LA MISMA " + actualTime + " = "+ horaEnvio))
        print (("SALIENDO DE LA FUNCION "))
        return
    #INICIO DE CONCATENACION DE DATOS
    control = concat()
    control.loop()
    time.sleep(30)
    #FIN CONCATENACION DE DATOS
    logging.basicConfig(filename = logfile, level=logging.INFO)
    dati = dt.now().strftime("%Y-%b-%d %H:%M:%S")
    logging.info(dati + ' - Inicio de envio  ')
    #LOGG
    control = base(logging)
    if control.vpnuse:
        print (("CONEXION POR VPN SE ENCUENTRA ACTIVADA"))
        if not vpnconnect():
            return True
    else:
        print (("CONEXION POR VPN SE ENCUENTRA DESACTIVADA"))
    #control = base()
    codigoprov = control.codprov
    print (("PROVINCIA: ", codigoprov))
    #Anulado el borrado porque se pas[o a la clase concar
    #control.delTempTables(codigoprov)
    provincia = control.cursor.execute(""" SELECT DISTINCT
        codprov_conex.link, codprov_conex.codigoprov, codprocedimientos.nombre
        FROM public.codprov_conex
        INNER JOIN public.codprocedimientos ON (codprov_conex.codigoproced = codprocedimientos.codigoproced)
        WHERE codprov_conex.activo=true AND codprov_conex.codigoprov=%s""" % codigoprov)
    #22/08/2018 Se saco la condicion de que la provioncia se encuentre activa
    #para que ingrese sea tenida en cuenta activo=true AND
    #19/09/2018 La condicion anteirormente mencionada ,se agrego nevamente para Salta
    prov_conex = control.cursor.fetchall()
    for info_conex in prov_conex:
        print (("OBTENIENDO DATOS PARA ENVIAR: Conexion_link: "+ info_conex[0] +" Procedimiento: " +str(info_conex[2]) + " Cod_provincia: "+ str(info_conex[1])))
        resp = control.control_caso_c(info_conex[0], info_conex[2], info_conex[1])
        if resp:
            print (("TRUE: SE INSERTARON CORRECTAMENTE"))
        else:
            print (("FALSE: NO SE INSERTARON DATOS"))
            logging.info(dati + ' - No se insertaron datos de la tabla: '+ str(info_conex[2]))
        #A CONTINUACION SE ELIMINA LA TABLA TEMPORTAL
    #control.delTempTables(codigoprov)
    control.Desconect()
    vpndisconnect()
    del control
    dati = dt.now().strftime("%Y-%b-%d %H:%M:%S")
    logging.info(dati + ' - Fin de envio')


thisfolder = os.path.dirname(os.path.abspath(__file__))
initfile = os.path.join(thisfolder, 'config.ini')


configMain = ConfigParser.ConfigParser()
configMain.read(initfile)

horaEnvio = configMain.get('Publicacion', 'horaEnvio')



tiempo = 60
t = InfiniteTimer(tiempo, tick)
t.start()

#tick()