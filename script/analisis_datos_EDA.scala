:load Utils.scala
:load edaUtils.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.debug.maxToStringFields", 200)
val PATH = "/home/usuario/regresion/proyecto/"
val RAWDATA = "dataset/used_cars_data.csv"

val RAWPARQUET = "dataset/parquet/raw_data"
val REALIZAR_EDA = true    
val FORCE_CREATE_PARQUET = false
val FORCE_PREPROCESS =false
val spark = SparkSession.builder().appName("EDA Regresion").master("local[*]").getOrCreate()

  

println("[EDA] Cargando dataset completo desde CSV o  parquet ...")
val dfcarsdataFull=loadDataParquet(spark, PATH, RAWDATA, RAWPARQUET, FORCE_CREATE_PARQUET)
val nFilasOriginal = dfcarsdataFull.count()
val nColumnas = dfcarsdataFull.columns.length

 

println("[EDA] Esquema del DataFrame:")
dfcarsdataFull.printSchema()

println("[EDA]Dimensiones del dataset:")
println(s"📌 Filas: $nFilasOriginal  |  Columnas: $nColumnas\n")
 
println("\n")
if (REALIZAR_EDA) {
  
  dfcarsdataFull.persist(StorageLevel.MEMORY_AND_DISK)
  dfcarsdataFull.count()  
  analisisEDA(dfcarsdataFull)
  dfcarsdataFull.unpersist()  


} else {
  println("=== Datos procesados cargados ===")
   showTable(dfcarsdataFull, 10)
}
 
 
val dfFinal =  prepararDataset(spark, dfcarsdataFull, PATH, FORCE_PREPROCESS)
 println(s"[EDA] Tratados: ${nFilasOriginal - dfFinal.count()}")
mostrarResumenFinal(dfFinal)
 
 