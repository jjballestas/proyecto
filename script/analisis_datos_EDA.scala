:load Utils.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.debug.maxToStringFields", 200)
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
   Utils.showTable(df, 10)
}
 
var dfWork = df
val colsToDrop =  Seq("combine_fuel_economy","is_certified","vehicle_damage_category","vin","listing_id","sp_id",
"main_picture_url","description","transmission_display","wheel_system_display","listing_color","dealer_zip","sp_name",
"bed","cabin","is_cpo","is_oemcpo","isCab")
dfWork = Utils.dropColumns(dfWork, colsToDrop)

dfWork = Utils.addGamaAlta(dfWork)
dfWork = Utils.addIsPickupAndClean(dfWork)
dfWork = Utils.cleanFuelType(dfWork)
dfWork = Utils.extractStringNumericFeatures(dfWork)
dfWork = Utils.addGeographicFeatures(dfWork)
dfWork = Utils.addTemporalFeatures(dfWork)
dfWork = Utils.fillBooleanAsCategory(dfWork, Seq("fleet", "frame_damaged", "has_accidents", "salvage", "theft_title"))
dfWork = Utils.treatOwnerCount(dfWork)
dfWork = Utils.treatMileageOutliers(dfWork)
dfWork = Utils.treatDaysOnMarket(dfWork)
dfWork = Utils.treatSavingsAmount(dfWork, mode = "drop")  

dfWork.write.mode("overwrite").parquet( PATH + "dataset/parquet/pre_imputation")
val dfBase = spark.read.parquet(PATH + "dataset/parquet/pre_imputation")
var dfImp = dfBase
dfImp = Utils.imputeDimensions(dfImp)
dfImp = Utils.imputeEngineSpecs(dfImp)
dfImp = Utils.imputeFuelEconomy(dfImp)
dfImp = Utils.imputeRemainingNumeric(dfImp)
dfImp = Utils.addPowerDensity(dfImp)
dfImp = Utils.filterPriceErrors(dfImp)
dfImp = Utils.addIsClassicFlag(dfImp)
dfImp = Utils.fillCategoricalUnknown(dfImp,  Seq("interior_color","body_type", "engine_type", "franchise_make", "transmission", "trim_name", "wheel_system"))
val colsToDropPostProcessing =  Seq("engine_displacement","city_fuel_economy","power","torque","engine_cylinders","trimId","listed_date","bed_height","bed_length","major_options")
dfImp = Utils.dropColumns(dfImp, colsToDropPostProcessing)
dfImp = Utils.transformTargetToLog(dfImp)
val dfFinal = dfImp

 
println("📌 Esquema del DataFrame:")
dfFinal.printSchema()
 
Utils.mostrarNulosPorColumna(dfFinal) 
 