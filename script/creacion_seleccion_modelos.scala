:load Utils.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val PATH             = "/home/usuario/regresion/proyecto/"
val RAWDATA          = "dataset/used_cars_data.csv"

val RAWPARQUET       = "dataset/parquet/raw_data"
val FORCE_CREATE_PARQUET = false
val FORCE_PREPROCESS     = false
val FORCE_SPLIT = false  
val trainPath = PATH + "dataset/parquet/train"
val testPath  = PATH + "dataset/parquet/test"

val spark = SparkSession.builder().appName("Modelado Regresion").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 200)

val dfload = Utils.cargarOPrepararDataset(spark, PATH, RAWDATA, RAWPARQUET,
forceCreateParquet = FORCE_CREATE_PARQUET,forcePreprocess    = FORCE_PREPROCESS)

 Utils.mostrarResumenFinal(dfload)
val dfFinal = Utils.crearSubconjuntoControlado(dfload,targetSize = 200000,seed = 42L,minRowsPerStratum = 500)
println(s"📌 Filas: ${dfFinal.count()}  |  Columnas: ${dfFinal.columns.length}\n")
  

val fs        = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
val trainExiste = fs.exists(new org.apache.hadoop.fs.Path(trainPath))
val testExiste  = fs.exists(new org.apache.hadoop.fs.Path(testPath))

val (dfTrain, dfTest) = if (!FORCE_SPLIT && trainExiste && testExiste) {
  println("  ✅ Cargando train/test desde disco...")
  val tr = spark.read.parquet(trainPath)
  val te = spark.read.parquet(testPath)
  (tr, te)
} else {
  println("  🔄 Generando split train/test...")
  val Array(tr, te) = dfFinal.randomSplit(Array(0.8, 0.2), seed = 42)
  tr.write.mode("overwrite").parquet(trainPath)
  te.write.mode("overwrite").parquet(testPath)
  println("  ✅ Train y Test guardados en parquet")
  (spark.read.parquet(trainPath), spark.read.parquet(testPath))
}

println(f"\n  📌 Train : ${dfTrain.count()}%,d registros")
println(f"  📌 Test  : ${dfTest.count()}%,d registros")
println(f"  📌 Ratio : ${dfTrain.count().toDouble / dfFinal.count() * 100}%.1f%% / ${dfTest.count().toDouble / dfFinal.count() * 100}%.1f%%")
 