#!/bin/bash
#sudo apt-get upgrade -y
#sudo add-apt-repository "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -sc)-pgdg main"
#wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update -y
#sudo apt-get install postgresql-9.6
sudo apt-get install openvpn -y
sudo apt-get install python3-tk
#PRECONDICIONES DE INSTALACION EN EL SO
sudo pip install numpy -y
sudo pip install geos -y
#Shapely pide instalar geos
sudo pip install shapely -y
sudo pip install timer
sudo pip install time
sudo pip install psycopg2 -y
sudo pip install configparser
sudo pip install logging
sudo pip install datetime
sudo pip install geojson
#sudo cp reinicio.service /etc/systemd/system -y
#sudo chmod 777 /etc/systemd/system/reinicio.service -y
#sudo chmod +x /etc/systemd/system/reinicio.service -y
"""
sudo cd /opt/api3 -y
sudo chmod -R 777 clientgis -y
sudo cd clientgis -y
sudo chmod +x start.sh -y
sudo chmod +x main.py -y
sudo chmod +x /ssh/check.sh -y
"""