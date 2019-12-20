#!/bin/bash

# install mysql, python3 and java
cd /tmp && \
curl -OL https://dev.mysql.com/get/mysql-apt-config_0.8.12-1_all.deb && \
sudo dpkg -i mysql-apt-config* && \
sudo apt-get update && \
sudo apt-get -y install python3-pip python3-dev build-essential libssl-dev libffi-dev openjdk-8-jdk mysql-server
apt install

# install python libraries
sudo pip3 install apache-airflow[mysql] pandarallel lxml tqdm ipywidgets

# clone repository
cd ~
git clone https://github.com/FdeFabricio/tpcdi-airflow.git
cd tpcdi-airflow

# init airflow
export AIRFLOW_HOME="$(pwd)"
airflow initdb

# setup mysql
sudo mysql -uroot -e "CREATE USER 'tpcdi'@'localhost' IDENTIFIED BY 'pA2sw@rd';"
sudo mysql -uroot -e "GRANT ALL PRIVILEGES ON tpcdi.* TO 'tpcdi'@'localhost';"
mysql -utpcdi -e "CREATE DATABASE tpcdi;"

# setup airflow-mysql connection
airflow connections --add --conn_id mysql_tpcdi --conn_type MySQL --conn_host localhost --conn_login tpcdi --conn_password pA2sw@rd --conn_schema tpcdi

# generate data
cd Tools
sudo java -jar DIGen.jar -sf 5 -o ../data
cd ..
