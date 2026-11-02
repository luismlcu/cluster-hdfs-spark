# Práctica 1 — Instrucciones de uso
Clúster Big Data con 3 máquinas virtuales (1 master y 2 workers) que ejecuta
HDFS y Spark en contenedores Docker, desplegado con Vagrant y Ansible.

## Requisitos

Linux x86_64 con VirtualBox y Vagrant. Probado con:
- Linux Mint 22.3 (base Ubuntu 24.04)
- VirtualBox 7.0
- Vagrant 2.4.9
- Ansible core 2.16
- Colección `community.docker` 3.7

Instalación:
```bash
sudo apt update
sudo apt install -y virtualbox vagrant ansible
ansible-galaxy collection install community.docker
```

## 1. Levantar el clúster

```bash
vagrant up
```

## 2. Subir un fichero a HDFS y comprobar la replicación

```bash
vagrant ssh master
docker exec -it namenode bash
```

Dentro del contenedor:

```bash
echo "Hola HDFS desde PR1" > /tmp/prueba.txt
hdfs dfs -mkdir -p /practica1
hdfs dfs -put -f /tmp/prueba.txt /practica1/prueba.txt

hdfs dfs -ls /practica1
hdfs dfs -cat /practica1/prueba.txt
hdfs fsck /practica1/prueba.txt -files -blocks -locations
```

Salir del contenedor y de la VM:

```bash
exit
exit
```

## 3. Ejecutar un Job Spark y comprobar el resultado en HDFS

### 3.1. Crear el fichero de entrada en HDFS

```bash
vagrant ssh master
docker exec -it namenode bash
```

Dentro del contenedor:

```bash
awk 'BEGIN{for(i=1;i<=50000;i++) print "linea", i, "hdfs spark cluster big data worker" (i%2+1)}' > /tmp/datos.txt
hdfs dfs -put -f /tmp/datos.txt /practica1/datos.txt

exit
```

### 3.2. Ejecutar el job

Desde la VM master:

```bash
docker exec -it spark-master /spark/bin/spark-shell --master spark://master:7077 -i /jobs/wordcount.scala
```

### 3.3. Comprobar el resultado en HDFS

Desde la VM master:

```bash
docker exec namenode hdfs dfs -ls /practica1/resultado_wordcount
docker exec namenode hdfs dfs -cat '/practica1/resultado_wordcount/part-*' | head
docker exec namenode hdfs fsck /practica1/resultado_wordcount -files -blocks -locations
```


