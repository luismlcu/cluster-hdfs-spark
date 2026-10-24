# Práctica 1 — Infraestructura Big Data virtualizada

Clúster de 3 máquinas virtuales con HDFS sobre Docker, desplegado con
Vagrant (infraestructura) y Ansible (configuración).

## Arquitectura

| Nodo    | IP privada    | Servicios actuales |
|---------|---------------|--------------------|
| master  | 192.168.56.10 | NameNode           |
| worker1 | 192.168.56.11 | DataNode           |
| worker2 | 192.168.56.12 | DataNode           |

- Red privada: `192.168.56.0/24`
- Box: `ubuntu/jammy64`
- Imágenes: `bde2020` Hadoop 3.2.1
- Factor de replicación HDFS: `2`
- Spark: pendiente de añadir

## Estructura del proyecto
Vagrantfile
ansible/
├── inventory.ini
├── playbook.yml
├── group_vars/all.yml
└── roles/
    ├── common/   # /etc/hosts del clúster
    ├── docker/   # Docker CE + Compose
    └── hdfs/     # NameNode y DataNodes

## Instalacion 

```bash
#instalar virtual box
sudo apt install virtualbox dkms linux-headers-$(uname -r)
#añade tu usuario al grupo vboxusers para evitar problemas de permisos con la virtualización y la red privada (que creara vagrant luego):
sudo usermod -aG vboxusers $USER

#instalacion vagrant (Crear y configurar maquinas virtuales)
sudo apt install vagrant

#instalar ansible (configurar el software dentro de esas maquinas virtuales)
sudo apt install -y ansible

# en la practica pone priorizar evitar el uso del modulo shell de ansible 
# instalo modulos para que ansible pueda comunicarse con docker
ansible-galaxy collection install community.docker
```

## Puesta en marcha
```bash
# Desde la raíz del proyecto:
vagrant up

#Esto crea las 3 VMs, configura la red privada y ejecuta Ansible sobre los tres nodos (el provisioner se lanza en la última VM para que el resto ya esté arrancado).La primera vez tarda más porque descarga la box y las imágenes Docker.Si las VMs ya existen y solo quieres reaplicar la configuración:
vagrant provision

# Si se cambia el Vagrantfile:
vagrant reload --provision

#Recrear todo desde cero:
vagrant destroy -f && vagrant up
```

### Comprobacion del despliegue

```bash
# Estado de las VMs:
vagrant status
#Contenedores en cada nodo:
vagrant ssh master  -c "docker ps --format 'table {{.Names}}\t{{.Status}}'"
vagrant ssh worker1 -c "docker ps --format 'table {{.Names}}\t{{.Status}}'"
vagrant ssh worker2 -c "docker ps --format 'table {{.Names}}\t{{.Status}}'"
#Esperado: namenode en master y datanode en cada worker, ambos Up.
#Interfaces web:
- NameNode: http://192.168.56.10:9870
- DataNode worker1: http://192.168.56.11:9864
- DataNode worker2: http://192.168.56.12:9864
#Comprobar DataNodes vivos sin navegador:
vagrant ssh master -c "docker exec namenode hdfs dfsadmin -report"

```
## Prueba del HDFS 

```bash
# Subir un fichero y comprobar la replicación:
vagrant ssh master
docker exec -it namenode bash
echo "Hola HDFS desde PR1" > /tmp/prueba.txt
hdfs dfs -mkdir -p /practica1
hdfs dfs -put -f /tmp/prueba.txt /practica1/prueba.txt
hdfs dfs -ls -R /practica1
hdfs dfs -cat /practica1/prueba.txt
hdfs fsck /practica1/prueba.txt -files -blocks -locations
exit
exit

#En hdfs dfs -ls la segunda columna debe ser 2. En hdfs fsck debe aparecer Average block replication: 2.0 y dos réplicas (worker1 y worker2).

#Parar el clúster
vagrant halt


```















