:load creacion_seleccion_modelos.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.log4j.{Level, Logger}

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

val spark = SparkSession.builder().appName("Evaluacion Modelo Final").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 200)

val PATH        = "/home/usuario/regresion/proyecto/"
val testPath    = PATH + "dataset/parquet/test"
val modelosPath = PATH + "modelos/"

// ── 1. Cargar test — nunca usado en entrenamiento ─────────────
println("  🔄 Cargando conjunto de test...")
val dfTest = spark.read.parquet(testPath)
println(s"  ✅ Test cargado: ${dfTest.count()} registros | ${dfTest.columns.length} columnas")

// ── 2. Cast Boolean → Double (mismo que en entrenamiento) ─────
val dfTestCast = dfTest.withColumn("is_new",           col("is_new").cast("double")).withColumn("franchise_dealer", col("franchise_dealer").cast("double"))

// ── 3. Cargar modelos finales ─────────────────────────────────
println("\n  🔄 Cargando modelos...")
val modeloLR  = PipelineModel.load(modelosPath + "lr_optimizado")
val modeloGBT = PipelineModel.load(modelosPath + "gbt_optimizado")
println("  ✅ Modelos cargados")

// ── 4. Evaluador ──────────────────────────────────────────────
val evaluator = new RegressionEvaluator().setLabelCol("log_price").setPredictionCol("prediction")

// ── 5. Evaluación final sobre test ────────────────────────────
println("\n" + "=" * 60)
println("  EVALUACIÓN FINAL SOBRE CONJUNTO DE TEST")
println("=" * 60)

Utils.evaluarModelo("LR Optimizado (ElasticNet)", modeloLR,  dfTestCast, evaluator)
Utils.evaluarModelo("GBT Optimizado",             modeloGBT, dfTestCast, evaluator)

// ── 6. Selección del modelo final ─────────────────────────────
println("\n" + "=" * 60)
println("  MODELO FINAL SELECCIONADO")
println("=" * 60)

val predGBT  = modeloGBT.transform(dfTestCast)
val r2GBT    = evaluator.setMetricName("r2").evaluate(predGBT)
val predLR   = modeloLR.transform(dfTestCast)
val r2LR     = evaluator.setMetricName("r2").evaluate(predLR)

val nombreFinal: String    = if (r2GBT > r2LR) "GBT Optimizado" else "LR Optimizado"
val modeloFinal: org.apache.spark.ml.PipelineModel = if (r2GBT > r2LR) modeloGBT else modeloLR
val predFinal: org.apache.spark.sql.DataFrame    = if (r2GBT > r2LR) predGBT else predLR

println(s"  ✅ Modelo seleccionado: $nombreFinal")

// ── 7. Muestra de predicciones vs valores reales ──────────────
println("\n  📌 Muestra de predicciones (escala original):")
predFinal.select(
    col("log_price"),
    col("prediction"),
    expr("round(exp(log_price), 0)").alias("precio_real"),
    expr("round(exp(prediction), 0)").alias("precio_predicho"),
    expr("round(abs(exp(prediction) - exp(log_price)) / exp(log_price) * 100, 2)").alias("error_pct")
  ).orderBy(col("error_pct").asc).show(20, truncate = false)

println("\n  ✅ Evaluación completada")