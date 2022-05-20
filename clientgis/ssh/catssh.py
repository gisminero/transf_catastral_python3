# -*- coding: utf-8 -*-
import os
import sys
from subprocess import check_output
from subprocess import CalledProcessError
import subprocess
import datetime


class catssh(object):

    def __init__(self):
        super(catssh, self).__init__()

    def connect(self):
        print (("CONECTANDO SSH..."))
        #os.system('openvpn --config drodriguez.ovpn')
        thisfolder = os.path.dirname(os.path.abspath(__file__))
        cant = 0
        for file in os.listdir(thisfolder):
            if file.endswith(".ovpn"):
                cant = cant + 1
                fileovpn = os.path.join(thisfolder, file)
                print((fileovpn))
        if cant == 0:
            return False
        p1 = subprocess.Popen(['openvpn', '--config', fileovpn, '--daemon'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        output = p1.communicate()[0]
        print (("SALIDA DE LA CONEXION:"+ str(output)))
        return True

    def disconnect(self):
        #os.system('sudo killall openvpn')
        subprocess.Popen(['sudo', 'killall', 'openvpn'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        return True

    def _get_pid(self, name):
        try:
            number_pid = check_output(["pidof", name])
        except CalledProcessError as e:
            print ((e.output))
            number_pid = 0
        return number_pid

    def checkactive(self):
        r = self._get_pid('openvpn')
        if r > 0:
            return True
        else:
            return False

    def checkactiveDESARROLLO(self):
        return True
        #r = self._get_pid('openvpn')
        #if r > 0:
            #return True
        #else:
            #return False

#b = catssh()
#r = b.checkactive()
#print(("EL SSH ESTA : " + str(r)))