:load Utils.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.debug.maxToStringFields", 200)
val PATH = "/home/usuario/regresion/proyecto/"
val RAWDATA = "dataset/used_cars_data.csv"

val RAWPARQUET = "dataset/parquet/raw_data"
val REALIZAR_EDA = false    
val FORCE_CREATE_PARQUET = false
val FORCE_PREPROCESS =false
val spark = SparkSession.builder().appName("EDA Regresion").master("local[*]").getOrCreate()

val df=Utils.loadDataParquet(spark, PATH, RAWDATA, RAWPARQUET, FORCE_CREATE_PARQUET)
println(s"📌 Filas: ${df.count()}  |  Columnas: ${df.columns.length}\n")
println("📌 Esquema del DataFrame:")
df.printSchema()
println("\n")
if (REALIZAR_EDA) {
  println("=== Análisis Exploratorio de Datos ===")
  Utils.analisisEDA(df)
  //Utils.showTable(df, 10)
} else {
  println("=== Datos procesados cargados ===")
   Utils.showTable(df, 10)
}
 
val dfFinal =  Utils.prepararDataset(spark, df, PATH, FORCE_PREPROCESS)

Utils.mostrarResumenFinal(dfFinal)
 