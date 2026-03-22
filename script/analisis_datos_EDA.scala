:load Utils.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val PATH = "/home/usuario/regresion/proyecto/"
val RAWDATA = "dataset/used_cars_data.csv"

val RAWPARQUET = "dataset/parquet/raw_data"
val REALIZAR_EDA = true    
val FORCE_CREATE_PARQUET = false
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
 // Utils.showTable(df, 10)
}
 
val df2 = Utils.addGamaAlta(df)
 
df2.groupBy("gama_alta")
  .agg(
    count("*").alias("n"),
    avg("price").alias("avg_price"),
    expr("percentile_approx(price, 0.5)").alias("median_price")
  )
  .show(false)