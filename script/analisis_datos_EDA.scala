:load Utils.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val PATH = "/home/usuario/regresion/proyecto/"
val RAWDATA = "dataset/used_cars_data_1000.csv"
val PARQUET = "dataset/Parquet/data_processed"
val REALIZAR_EDA = true    

val spark = SparkSession.builder().appName("EDA Regresion").master("local[*]").getOrCreate()
val df = Utils.loadOrProcessData( spark, PATH, RAWDATA, PARQUET, REALIZAR_EDA)
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
 // Utils.showTable(df, 10)
}
 
Utils.imprimirDiccionarioAnalisis(df)