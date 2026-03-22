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

 
/*
Utils.analyzeStringColumnsContent(  df,  Seq("power", "torque", "engine_cylinders","back_legroom",
"front_legroom","fuel_tank_volume","height","length","maximum_seating","wheelbase","width"))
 
if (REALIZAR_EDA) {
  println("=== Análisis Exploratorio de Datos ===")
  Utils.analisisEDA(df)
  //Utils.showTable(df, 10)
} else {
  println("=== Datos procesados cargados ===")
 // Utils.showTable(df, 10)
}
 */
 val df2 = Utils.addIsPickupAndClean(df)
 
val df3 = Utils.addGamaAlta(df2)
val df4 = Utils.addGeographicFeatures(df3)
 
val df5 = Utils.addIsPickupAndClean(df4)

val df6 = Utils.extractStringNumericFeatures(df5) 

val df7 = Utils.fillBooleanAsCategory(df6, Seq("fleet", "frame_damaged", "has_accidents", "salvage", "theft_title"))
val df8 = Utils.treatOwnerCount(df7)
val df9 = Utils.addTemporalFeatures(df8)
val df10 = Utils.cleanFuelType(df9)
val df11 = Utils.dropIrrelevantColumns(df10)
val df12 = Utils.addMissingDimensionsFlag(df11)
val df13 = Utils.imputeDimensionsByGroup(df12)
println("📌 Esquema del DataFrame:")
df13.printSchema()
Utils.mostrarNulosPorColumna(df13 )
 