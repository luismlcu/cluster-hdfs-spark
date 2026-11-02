// Job de ejemplo para spark-shell: wordcount HDFS -> Spark -> HDFS.
// Lee /practica1/datos.txt, escribe /practica1/resultado_wordcount e imprime
// cuántas particiones (tareas) ha procesado cada worker.
import java.net.InetAddress
import org.apache.hadoop.fs.{FileSystem, Path}

val entrada = "hdfs://master:9000/practica1/datos.txt"
val salida  = "hdfs://master:9000/practica1/resultado_wordcount"

// Permite relanzar el job: Spark falla si el directorio de salida ya existe
val fs = FileSystem.get(new java.net.URI(entrada), sc.hadoopConfiguration)
val borrado = fs.delete(new Path(salida), true)

val lineas = sc.textFile(entrada, 8)

val conteo = lineas.flatMap(_.split("\\s+")).filter(_.nonEmpty).map(p => (p, 1)).reduceByKey(_ + _)
conteo.saveAsTextFile(salida)
println(s"Resultado guardado en $salida (${conteo.count()} palabras distintas)")

// Reparto del trabajo: hostname del executor que procesa cada partición
val reparto = lineas.mapPartitionsWithIndex { (i, it) => Iterator((InetAddress.getLocalHost.getHostName, i, it.size)) }.collect()
println("==== REPARTO DE TAREAS ENTRE WORKERS ====")
reparto.groupBy(_._1).toSeq.sortBy(_._1).foreach { case (host, ts) => println(s"$host: ${ts.length} particiones, ${ts.map(_._3).sum} lineas") }

sys.exit(0)
