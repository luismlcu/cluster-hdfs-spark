# Práctica 1 — Infraestructura Big Data virtualizada

Clúster Big Data de **3 máquinas virtuales** con **HDFS** y **Spark (modo standalone)**.
Todo el software de HDFS y Spark se ejecuta en **contenedores Docker**, desplegados con
**Ansible** sobre máquinas creadas con **Vagrant**. El clúster completo se levanta con un único
comando: `vagrant up`.

## Índice

1. [Qué se construye](#1-qué-se-construye)
2. [Requisitos y dependencias](#2-requisitos-y-dependencias)
3. [Qué hace cada archivo del repositorio](#3-qué-hace-cada-archivo-del-repositorio)
4. [Guía paso a paso (comandos y salida esperada)](#4-guía-paso-a-paso-comandos-y-salida-esperada)
5. [Resumen de cumplimiento del enunciado](#5-resumen-de-cumplimiento-del-enunciado)
6. [Apagar y limpiar](#6-apagar-y-limpiar)

---

## 1. Qué se construye

| Nodo    | IP privada    | RAM  | Contenedores Docker               |
|---------|---------------|------|-----------------------------------|
| master  | 192.168.56.10 | 4 GB | `namenode` + `spark-master`       |
| worker1 | 192.168.56.11 | 3 GB | `datanode` + `spark-worker`       |
| worker2 | 192.168.56.12 | 3 GB | `datanode` + `spark-worker`       |

```
                 master (192.168.56.10)
            ┌──────────────────────────────┐
            │  NameNode      Spark Master  │   coordinan; no guardan datos
            └──────────────────────────────┘
                 │                    │
      worker1 (192.168.56.11)   worker2 (192.168.56.12)
   ┌──────────────────────┐   ┌──────────────────────┐
   │ DataNode  SparkWorker│   │ DataNode  SparkWorker│   guardan bloques y ejecutan tareas
   └──────────────────────┘   └──────────────────────┘
```

**Reparto de responsabilidades entre las tres capas:**

| Capa | Herramienta | Función |
|---|---|---|
| Infraestructura | **Vagrant** (+ VirtualBox) | Crea las 3 VM y la red privada `192.168.56.0/24` |
| Configuración | **Ansible** | Configura cada VM: `/etc/hosts`, instala Docker y despliega los contenedores |
| Servicios | **Docker** (Compose) | Empaqueta y ejecuta HDFS y Spark |

**Decisiones de diseño:**

- **Replicación HDFS = 2** (`dfs.replication`), configurada en NameNode y DataNodes, y también en
  Spark (`spark.hadoop.dfs.replication`). Un cliente HDFS usa su propia replicación (3 por defecto) y no
  la del NameNode; sin esa línea, lo que escribiera Spark quedaría sub-replicado en un clúster de 2 DataNodes.
- **`network_mode: host`** en todos los contenedores: usan directamente la red de la VM, sin NAT de Docker.
  Evita problemas de puertos y direcciones entre nodos. Los nodos se resuelven por nombre gracias al `/etc/hosts`
  que genera Ansible (Hadoop y Spark se comunican por hostname).
- **El provisioner de Ansible se define en la última VM (`worker2`)**, para que master y worker1 ya estén
  arrancados cuando se ejecuta. Ansible configura las tres máquinas desde ahí (`ansible.limit = "all"`).
- **Sin el módulo `shell` de Ansible.** Los contenedores se gestionan con `community.docker.docker_compose_v2`.
- **Dos proyectos Compose separados** (`/opt/bigdata` para HDFS y `/opt/bigdata/spark` para Spark), para que
  los dos `docker-compose.yml` no se sobrescriban.
- Imágenes: `bde2020/hadoop-namenode`, `bde2020/hadoop-datanode` (Hadoop 3.2.1) y
  `bde2020/spark-master`, `bde2020/spark-worker` (Spark 3.2.1 con Hadoop 3.2).
- Box de Vagrant: `ubuntu/jammy64`.

---

## 2. Requisitos y dependencias

**Equipo anfitrión:** Linux x86_64 con virtualización por hardware (VT-x / AMD-V) activada en la BIOS,
al menos **12 GB de RAM libres** (las VM usan 10 GB), unos **20 GB de disco** libres y conexión a Internet
(descarga de la box y de las imágenes Docker). Probado en Ubuntu 26.04 con VirtualBox 7.2.

**Software en el anfitrión:**

| Software | Para qué se usa |
|---|---|
| VirtualBox | Hipervisor: ejecuta las 3 VM |
| Vagrant (≥ 2.4.9 si se usa VirtualBox 7.2) | Crea y gestiona las VM y lanza el provisioner |
| Ansible | Configura las VM por SSH (lo ejecuta Vagrant desde el anfitrión) |
| Colección `community.docker` de Ansible | Aporta el módulo `docker_compose_v2` |

Docker **no** hace falta en el anfitrión: se instala dentro de las VM.

Comprobación:

```bash
vboxmanage --version
vagrant --version
ansible --version | head -1
ansible-galaxy collection list community.docker
```

Salida esperada: una versión para cada comando (p. ej. `7.2.x`, `Vagrant 2.4.x`, `ansible [core 2.x]`) y la
colección `community.docker` listada.

En otras distribuciones, instala manualmente esos cuatro componentes.

---

## 3. Qué hace cada archivo del repositorio

```
.
├── Vagrantfile
├── README.md
├── .gitignore
├── scripts/
│   └── install-deps.sh
└── ansible/
    ├── inventory.ini
    ├── playbook.yml
    ├── group_vars/
    │   └── all.yml
    └── roles/
        ├── common/
        │   ├── tasks/main.yml
        │   └── templates/hosts.j2
        ├── docker/
        │   └── tasks/main.yml
        ├── hdfs/
        │   ├── tasks/main.yml
        │   └── templates/
        │       ├── namenode.j2
        │       └── datanode.j2
        └── spark/
            ├── tasks/main.yml
            ├── templates/
            │   ├── spark-master.j2
            │   ├── spark-worker.j2
            │   └── spark-defaults.conf.j2
            └── files/
                └── wordcount.scala
```

### Raíz

| Archivo | Qué hace |
|---|---|
| `Vagrantfile` | Define las 3 VM (nombre, hostname, IP privada, RAM y CPUs) sobre la box `ubuntu/jammy64` y VirtualBox, y declara el provisioner de Ansible en `worker2`, que configura los tres nodos. Es el punto de entrada de `vagrant up`. |
| `README.md` | Esta guía. |
| `.gitignore` | Excluye de Git el directorio `.vagrant/` (estado y claves privadas de las VM) y ficheros temporales. |


### `ansible/` — configuración global

| Archivo | Qué hace |
|---|---|
| `inventory.ini` | Inventario de Ansible: lista las tres VM con su IP y su clave SSH (generada por Vagrant), y las agrupa en `masters`, `workers` y `cluster` (todos). Los roles usan estos grupos para saber qué desplegar en cada nodo. |
| `playbook.yml` | Orquestador. Ejecuta, en orden y sobre todo el clúster: (1) roles `common` y `docker`, (2) rol `hdfs`, (3) rol `spark`. |
| `group_vars/all.yml` | Variables compartidas por todos los roles: versiones de imágenes, puertos, `hdfs_replication: 2`, memoria y cores de cada Spark Worker, y rutas de datos. Para cambiar la replicación o un puerto basta editar este fichero. |

### Rol `common`

| Archivo | Qué hace |
|---|---|
| `tasks/main.yml` | Escribe `/etc/hosts` en cada VM a partir de la plantilla. |
| `templates/hosts.j2` | Genera una línea `IP nombre` por cada nodo del inventario. Es imprescindible: el NameNode comprueba que sabe resolver el nombre de cada DataNode que se registra, y Spark usa hostnames. |

### Rol `docker`

| Archivo | Qué hace |
|---|---|
| `tasks/main.yml` | Instala Docker CE y el plugin Compose siguiendo la instalación oficial: dependencias, clave GPG y repositorio de Docker, paquetes `docker-ce`, `docker-ce-cli`, `containerd.io`, `docker-compose-plugin` y `python3-docker`. Habilita el servicio y añade el usuario `vagrant` al grupo `docker`. |

### Rol `hdfs`

| Archivo | Qué hace |
|---|---|
| `tasks/main.yml` | Crea el directorio de datos (`/opt/bigdata/namenode` en el master, `/opt/bigdata/datanode` en los workers), copia el `docker-compose.yml` adecuado a `/opt/bigdata`, levanta el contenedor con `docker_compose_v2` y, en el master, espera a que la web del NameNode (puerto 9870) responda. |
| `templates/namenode.j2` | Compose del NameNode: imagen, red `host`, volumen de datos persistente y variables `CORE_CONF_*` / `HDFS_CONF_*`, que la imagen convierte en `core-site.xml` y `hdfs-site.xml` (p. ej. `dfs.replication=2`). |
| `templates/datanode.j2` | Compose del DataNode: igual que el anterior, más el nombre con el que se registra y `SERVICE_PRECONDITION`, que lo hace esperar a que el NameNode esté disponible. |

### Rol `spark`

| Archivo | Qué hace |
|---|---|
| `tasks/main.yml` | Crea `/opt/bigdata/spark` y `/opt/bigdata/spark/jobs`, genera `spark-defaults.conf`, copia el job de ejemplo al master, despliega el Compose según el rol del nodo y lo levanta. Si cambia `spark-defaults.conf` recrea los contenedores. Al final espera al Spark Master y **verifica que los 2 workers se han registrado** (si no, el aprovisionamiento falla). |
| `templates/spark-master.j2` | Compose del Spark Master (puertos 7077 y 8080). Monta `spark-defaults.conf` y la carpeta de jobs. |
| `templates/spark-worker.j2` | Compose del Spark Worker: se registra en `spark://master:7077`, con la memoria y cores de `all.yml`. |
| `templates/spark-defaults.conf.j2` | Configuración por defecto de Spark: master, `fs.defaultFS` de HDFS, **`dfs.replication=2`** y uso de hostnames para llegar a los DataNodes. |
| `files/wordcount.scala` | Job de demostración. Lee `/practica1/datos.txt` de HDFS, cuenta palabras (wordcount), guarda el resultado en `/practica1/resultado_wordcount` (HDFS) e imprime cuántas particiones (tareas) procesó cada worker. |

### Lo que se genera al desplegar (no está en el repositorio)

| Dónde | Qué |
|---|---|
| `.vagrant/` (anfitrión) | Estado de las VM y sus claves SSH privadas. No se entrega. |
| `/opt/bigdata/{namenode,datanode}` (VM) | Datos persistentes de HDFS. |
| `/opt/bigdata/docker-compose.yml` (VM) | Compose de HDFS. |
| `/opt/bigdata/spark/` (VM) | Compose de Spark, `spark-defaults.conf` y `jobs/`. |

---

## 4. Guía paso a paso (comandos y salida esperada)

Todos los comandos se ejecutan **desde la raíz del repositorio**. Se indica dónde corre cada uno:
**[PC]** en el anfitrión, **[master]** dentro de la VM master (`vagrant ssh master`) y
**[namenode]** dentro del contenedor del NameNode (`docker exec -it namenode bash`). Para salir de una
sesión interactiva, `exit`.

Las salidas son orientativas: los identificadores, tamaños, tiempos y fechas variarán.

### Paso 1 — Desplegar el clúster

```bash
# [PC]
vagrant up
```

Crea las 3 VM, configura la red y ejecuta Ansible sobre ellas. La primera vez tarda varios minutos
(descarga la box y las imágenes Docker). Para partir de cero: `vagrant destroy -f && vagrant up`.

**Salida esperada:** al final, el resumen de Ansible sin fallos en los tres nodos:

```
PLAY RECAP *********************************************************************
master   : ok=...  changed=...  unreachable=0  failed=0  ...
worker1  : ok=...  changed=...  unreachable=0  failed=0  ...
worker2  : ok=...  changed=...  unreachable=0  failed=0  ...
```

`unreachable=0` y `failed=0` en las tres líneas. Si la tarea *"Verificar que todos los Spark Workers se han
registrado en el Master"* terminó en `ok`, Spark ya tiene sus 2 workers.

### Paso 2 — Comprobar que los nodos están levantados

```bash
# [PC]
vagrant status
```

```
master                    running (virtualbox)
worker1                   running (virtualbox)
worker2                   running (virtualbox)
```

Contenedores en cada nodo:

```bash
# [PC]
for n in master worker1 worker2; do echo "--- $n"; vagrant ssh $n -c "docker ps --format 'table {{.Names}}\t{{.Status}}'"; done
```

```
--- master
NAMES          STATUS
namenode       Up N minutes (healthy)
spark-master   Up N minutes
--- worker1
NAMES          STATUS
datanode       Up N minutes (healthy)
spark-worker   Up N minutes
--- worker2
NAMES          STATUS
datanode       Up N minutes (healthy)
spark-worker   Up N minutes
```

DataNodes y Spark Workers registrados, sin navegador:

```bash
# [PC]
vagrant ssh master -c "docker exec namenode hdfs dfsadmin -report | grep -E 'Live datanodes|Hostname'"
vagrant ssh master -c "curl -s http://master:8080/json/ | grep aliveworkers"
```

```
Live datanodes (2):
Hostname: worker1
Hostname: worker2
```
```
  "aliveworkers" : 2,
```

**Interfaces web** (desde el navegador del anfitrión):

| Servicio | URL | Qué se debe ver |
|---|---|---|
| NameNode | http://192.168.56.10:9870 | Pestaña *Datanodes*: 2 nodos *In service* (Overview: *Live Nodes 2*) |
| DataNode worker1 | http://192.168.56.11:9864 | Página del DataNode, activo |
| DataNode worker2 | http://192.168.56.12:9864 | Página del DataNode, activo |
| Spark Master | http://192.168.56.10:8080 | `Status: ALIVE`, `Alive Workers: 2`, ambos workers `ALIVE` |
| Spark Worker worker1 | http://192.168.56.11:8081 | `State: ALIVE`, conectado a `spark://master:7077` |
| Spark Worker worker2 | http://192.168.56.12:8081 | `State: ALIVE`, conectado a `spark://master:7077` |

### Paso 3 — Subir un fichero a HDFS y comprobar la replicación (factor 2)

```bash
# [PC]
vagrant ssh master
# [master]
docker exec -it namenode bash
# [namenode]
echo "Hola HDFS desde PR1" > /tmp/prueba.txt
hdfs dfs -mkdir -p /practica1
hdfs dfs -put -f /tmp/prueba.txt /practica1/prueba.txt
hdfs dfs -ls -R /practica1
hdfs dfs -cat /practica1/prueba.txt
hdfs fsck /practica1/prueba.txt -files -blocks -locations
exit    # sale del contenedor
exit    # sale de la VM
```

**Salida esperada:**

`hdfs dfs -ls -R /practica1`: la **segunda columna es el factor de replicación (2)**:

```
-rw-r--r--   2 root supergroup         20 2026-... /practica1/prueba.txt
```

`hdfs dfs -cat`:

```
Hola HDFS desde PR1
```

`hdfs fsck ...`: la prueba de que el bloque está en **dos nodos distintos**:

```
/practica1/prueba.txt 20 bytes, replicated: replication=2, 1 block(s):  OK
0. BP-...:blk_... len=20 Live_repl=2  [DatanodeInfoWithStorage[192.168.56.12:9866,...], DatanodeInfoWithStorage[192.168.56.11:9866,...]]

Status: HEALTHY
 Number of data-nodes:  2
 Under-replicated blocks:       0 (0.0 %)
 Default replication factor:    2
 Average block replication:     2.0
 Missing blocks:                0
```

Qué mirar: `replication=2`, `Live_repl=2` con las IP de worker1 (`.11`) y worker2 (`.12`),
`Average block replication: 2.0`, `Under-replicated blocks: 0` y `Status: HEALTHY`.

En el navegador: http://192.168.56.10:9870 → *Utilities* → *Browse the file system* → `/practica1/prueba.txt`
muestra `Replication: 2` y los dos nodos que tienen el bloque.

### Paso 4 — Ejecutar un job de Spark: resultado en HDFS y trabajo repartido entre workers

El job [`wordcount.scala`](ansible/roles/spark/files/wordcount.scala) lee de HDFS, cuenta palabras con Spark
(el trabajo se divide en 8 tareas repartidas entre los executors de los dos workers), guarda el resultado en
HDFS e imprime el reparto.

**4.1. Crear los datos de entrada y subirlos a HDFS:**

```bash
# [PC]
vagrant ssh master
# [master]
docker exec -it namenode bash
# [namenode]
awk 'BEGIN{for(i=1;i<=50000;i++) print "linea", i, "hdfs spark cluster big data worker" (i%2+1)}' > /tmp/datos.txt
hdfs dfs -mkdir -p /practica1
hdfs dfs -put -f /tmp/datos.txt /practica1/datos.txt
hdfs dfs -ls -h /practica1
exit    # sale del contenedor (sigue dentro de la VM master)
```

Salida esperada del `ls`: `datos.txt` de unos 2 MB con replicación `2`:

```
-rw-r--r--   2 root supergroup      2.2 M 2026-... /practica1/datos.txt
```

**4.2. Lanzar el job** (todavía dentro de la VM master; el driver corre en el contenedor `spark-master`):

```bash
# [master]
docker exec -it spark-master /spark/bin/spark-shell --master spark://master:7077 -i /jobs/wordcount.scala
```

**Salida esperada** (entre otras líneas de la consola de Spark):

```
Spark context available as 'sc' (master = spark://master:7077, app id = app-...).
...
Resultado guardado en hdfs://master:9000/practica1/resultado_wordcount (50008 palabras distintas)
==== REPARTO DE TAREAS ENTRE WORKERS ====
worker1: N particiones, N lineas
worker2: N particiones, N lineas
```

Qué mirar: aparecen **worker1 y worker2**, cada uno con al menos una partición; entre los dos suman 8
particiones y 50000 líneas. El número exacto por worker puede variar entre ejecuciones. Al terminar, el
script cierra el shell solo.

**4.3. Comprobar que el resultado está en HDFS y replicado:**

```bash
# [master]
docker exec namenode hdfs dfs -ls /practica1/resultado_wordcount
docker exec namenode hdfs dfs -cat '/practica1/resultado_wordcount/part-*' | sort -t, -k2 -nr | head
docker exec namenode hdfs fsck /practica1/resultado_wordcount -files -blocks -locations | grep -E "replication|Under|HEALTHY"
exit    # sale de la VM
```

Salida esperada:

- `ls`: un fichero `_SUCCESS` y ficheros `part-00000`, `part-00001`, … con **replicación `2`** en la segunda columna.
- `cat | sort | head`: las palabras más frecuentes (las seis primeras con 50000 apariciones):

  ```
  (linea,50000)
  (hdfs,50000)
  (spark,50000)
  (cluster,50000)
  (big,50000)
  (data,50000)
  (worker1,25000)
  (worker2,25000)
  (1,1)
  ...
  ```
  (el orden entre palabras con el mismo conteo puede variar)
- `fsck`: `Average block replication: 2.0`, `Under-replicated blocks: 0 (0.0 %)` y `Status: HEALTHY`
  (aparecerá `replication=2` en cada fichero).

**4.4. Ver el reparto en la interfaz de Spark:** abre http://192.168.56.10:8080 → *Completed Applications* →
aplicación **Spark shell** (estado `FINISHED`, 2 cores). En su página aparecen **dos executors, uno en cada
worker** (`192.168.56.11` y `192.168.56.12`).

### Paso 5 — Parar un worker y comprobar que los datos siguen disponibles

Los datos de `/practica1/prueba.txt` y `/practica1/datos.txt` están en worker1 **y** worker2. Se apaga
worker1 por completo:

```bash
# [PC]
vagrant halt worker1
```

Se leen los datos desde HDFS con worker1 apagado:

```bash
# [PC]
vagrant ssh master -c "docker exec namenode hdfs dfs -cat /practica1/prueba.txt"
vagrant ssh master -c "docker exec namenode hdfs dfs -cat /practica1/datos.txt | head -3"
```

**Salida esperada:** el contenido de los ficheros, leído desde la réplica de worker2:

```
Hola HDFS desde PR1
```
```
linea 1 hdfs spark cluster big data worker2
linea 2 hdfs spark cluster big data worker1
linea 3 hdfs spark cluster big data worker2
```

Puede aparecer algún `WARN` indicando que no se pudo conectar con worker1 antes de leer de worker2; es normal.

Estado de HDFS:

```bash
# [PC]
vagrant ssh master -c "docker exec namenode hdfs fsck /practica1/prueba.txt -files -blocks -locations | grep -E 'Live_repl|HEALTHY|replication'"
vagrant ssh master -c "docker exec namenode hdfs dfsadmin -report | grep -E 'Live datanodes|Dead datanodes'"
```

El NameNode tarda unos **10 minutos** (≈10,5 min por defecto) en declarar muerto un DataNode que no responde:

- Justo después de apagar worker1 puede seguir mostrando `Live datanodes (2)`; el fichero se lee igualmente.
- Pasado ese tiempo: `Dead datanodes (1)` y los bloques con `Live_repl=1`. Es lo esperado: con solo 2 DataNodes
  no hay dónde reponer la segunda réplica. **Los datos siguen disponibles**, que es lo que se comprueba.

Restaurar el clúster:

```bash
# [PC]
vagrant up worker1
vagrant ssh master -c "docker exec namenode hdfs dfsadmin -report | grep 'Live datanodes'"
```

Los contenedores arrancan solos (política `restart: unless-stopped`); tras uno o dos minutos debe volver a
mostrar `Live datanodes (2):`.

---

## 5. Resumen de cumplimiento del enunciado

| Requisito | Dónde se cumple | Cómo comprobarlo |
|---|---|---|
| 3 VM en red privada (1 master, 2 workers) | `Vagrantfile` | Paso 2: `vagrant status` |
| NameNode en master, DataNode en los workers | rol `hdfs` | Paso 2: `docker ps` y `dfsadmin -report` |
| Replicación HDFS = 2 | `group_vars/all.yml` (`hdfs_replication`), plantillas HDFS y `spark-defaults.conf` | Paso 3: `fsck` |
| Spark Master en master y un Worker en cada worker | rol `spark` | Paso 2: `docker ps` y `aliveworkers` = 2 |
| Todo HDFS y Spark en contenedores Docker desplegados con Ansible | roles `docker`, `hdfs`, `spark` (módulo `docker_compose_v2`, sin `shell`) | Paso 2: `docker ps` |
| Levantable desde cero con `vagrant up` | `Vagrantfile` + `playbook.yml` | Paso 1 |
| Funcional 1: webs de cada servicio | puertos 9870, 9864, 8080, 8081 | Paso 2: tabla de interfaces web |
| Funcional 2: fichero en HDFS con replicación 2 | rol `hdfs` | Paso 3 |
| Funcional 3: job Spark con resultado en HDFS y trabajo repartido | rol `spark`, `wordcount.scala` | Paso 4 |
| Funcional 4: con un worker parado, los datos siguen disponibles | replicación 2 | Paso 5 |

---

## 6. Apagar y limpiar

```bash
vagrant halt           # apaga las VM y conserva los datos; `vagrant up` las vuelve a arrancar
vagrant destroy -f     # borra las VM y todos los datos