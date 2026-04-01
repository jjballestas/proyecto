
:load Utils.scala
:load Utils_model.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.log4j.{Level, Logger}

// ── Silenciar logs irrelevantes ───────────────────────────────
Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)
Logger.getLogger("org.apache.spark.scheduler.DAGScheduler").setLevel(Level.ERROR)
Logger.getLogger("org.apache.spark.util.SizeEstimator").setLevel(Level.OFF)

 
val spark = SparkSession.builder().appName("Evaluacion Modelo Final — TESCR 2026").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 200)

val PATH        = "/home/usuario/regresion/proyecto/"
val testPath    = PATH + "dataset/parquet/test"
val modelosPath = PATH + "modelos/"

 
println("\n" + "═" * 60)
println("  CARGA DEL CONJUNTO DE PRUEBA")
println("═" * 60)



val dfTest = spark.read.parquet(testPath)
val nTest  = dfTest.count()
println(f"  ✅ Test cargado: $nTest%,d registros | ${dfTest.columns.length} columnas")

// ── Mismo preprocesamiento aplicado en entrenamiento ─────────
val dfTestCast = {
  dfTest
  .withColumn("is_new",           col("is_new").cast("double"))
  .withColumn("franchise_dealer", col("franchise_dealer").cast("double"))
}

dfTestCast.cache()
dfTestCast.count()
println("  ✅ Test cacheado y listo")

// ══════════════════════════════════════════════════════════════
// 2. CARGA DE LOS TRES MODELOS FINALES
// ══════════════════════════════════════════════════════════════
println("\n" + "═" * 60)
println("  CARGA DE MODELOS DESDE DISCO")
println("═" * 60)

val modeloLR  = PipelineModel.load(modelosPath + "lr_optimizado")
println("  ✅ Modelo 1: Regresión Lineal (ElasticNet) cargado")

val modeloGBT = PipelineModel.load(modelosPath + "gbt_optimizado")
println("  ✅ Modelo 2: Gradient Boosted Trees cargado")

val modeloRF  = PipelineModel.load(modelosPath + "rf_optimizado")
println("  ✅ Modelo 3: Random Forest cargado")

// ══════════════════════════════════════════════════════════════
// 3. EVALUADOR COMÚN
//    Variable objetivo: log_price (log natural del precio)
//    Métricas: RMSE, MAE, R², MSE
// ══════════════════════════════════════════════════════════════
val evaluator = new RegressionEvaluator().setLabelCol("log_price").setPredictionCol("prediction")

// ══════════════════════════════════════════════════════════════
// 4. EVALUACIÓN COMPARATIVA DE LOS TRES MODELOS SOBRE TEST
// ══════════════════════════════════════════════════════════════
println("\n" + "═" * 60)
println("  EVALUACIÓN COMPARATIVA — CONJUNTO DE PRUEBA")
println("═" * 60)
println("  (Variable objetivo en escala logarítmica: log_price)")
println("  (RMSE interpretable como: error ≈ (e^RMSE - 1) × 100 %)")

Utils_models.evaluarModelo("LR  Optimizado (ElasticNet)", modeloLR,  dfTestCast, evaluator)
Utils_models.evaluarModelo("GBT Optimizado",              modeloGBT, dfTestCast, evaluator)
Utils_models.evaluarModelo("RF  Optimizado",              modeloRF,  dfTestCast, evaluator)

// ══════════════════════════════════════════════════════════════
// 5. SELECCIÓN AUTOMÁTICA DEL MODELO FINAL (por R² en test)
// ══════════════════════════════════════════════════════════════
println("\n" + "═" * 60)
println("  SELECCIÓN DEL MODELO FINAL")
println("═" * 60)

val predLR  = modeloLR.transform(dfTestCast)
val predGBT = modeloGBT.transform(dfTestCast)
val predRF  = modeloRF.transform(dfTestCast)

val r2LR  = evaluator.setMetricName("r2").evaluate(predLR)
val r2GBT = evaluator.setMetricName("r2").evaluate(predGBT)
val r2RF  = evaluator.setMetricName("r2").evaluate(predRF)

println(f"  R² LR  : $r2LR%.4f")
println(f"  R² GBT : $r2GBT%.4f")
println(f"  R² RF  : $r2RF%.4f")

val (nombreFinal, modeloFinal, predFinal) = {
  if (r2GBT >= r2LR && r2GBT >= r2RF)
    ("GBT Optimizado", modeloGBT, predGBT)
  else if (r2RF >= r2LR && r2RF >= r2GBT)
    ("RF  Optimizado", modeloRF,  predRF)
  else
    ("LR  Optimizado", modeloLR,  predLR)
}

println(s"\n  ✅ Modelo final seleccionado: $nombreFinal")
println(s"  Criterio: mayor R² sobre el conjunto de prueba")

// ══════════════════════════════════════════════════════════════
// 6. MÉTRICAS FINALES DEL MODELO SELECCIONADO (escala log)
// ══════════════════════════════════════════════════════════════
println("\n" + "═" * 60)
println(s"  MÉTRICAS FINALES — $nombreFinal")
println("═" * 60)

val rmseFinal = evaluator.setMetricName("rmse").evaluate(predFinal)
val maeFinal  = evaluator.setMetricName("mae").evaluate(predFinal)
val r2Final   = evaluator.setMetricName("r2").evaluate(predFinal)
val mseFinal  = rmseFinal * rmseFinal

println(f"  R²   : $r2Final%.4f")
println(f"  RMSE : $rmseFinal%.4f  (error típico ≈ ${(math.exp(rmseFinal) - 1) * 100}%.1f%% del precio)")
println(f"  MAE  : $maeFinal%.4f")
println(f"  MSE  : $mseFinal%.6f")

// ══════════════════════════════════════════════════════════════
// 7. PREDICCIONES EN ESCALA ORIGINAL (dólares)
//    Transformación inversa: precio = exp(log_price)
// ══════════════════════════════════════════════════════════════
val predConPrecio = {
  predFinal
  .withColumn("precio_real",     round(exp(col("log_price")),  0).cast("int"))
  .withColumn("precio_predicho", round(exp(col("prediction")), 0).cast("int"))
  .withColumn("error_abs_usd",   abs(col("precio_real") - col("precio_predicho")))
  .withColumn("error_pct",       round(abs(col("precio_real") - col("precio_predicho")) / col("precio_real") * 100, 2))
}

// ── 7.1 Muestra aleatoria ─────────────────────────────────────
println("\n" + "═" * 60)
println(s"  EJEMPLOS DE PREDICCIÓN — MUESTRA ALEATORIA ($nombreFinal)")
println("═" * 60)
predConPrecio.select("make_name", "year", "mileage","precio_real", "precio_predicho", "error_abs_usd", "error_pct")
.orderBy(rand(42L)).show(15, truncate = false)

// ── 7.2 Mejores predicciones (error < 5%) ────────────────────
println("\n" + "═" * 60)
println("  MEJORES PREDICCIONES — error < 5%")
println("═" * 60)
predConPrecio.select("make_name", "year", "mileage","precio_real", "precio_predicho", "error_abs_usd", "error_pct")
.filter(col("error_pct") < 5.0).orderBy(col("error_pct").asc).show(10, truncate = false)

// ── 7.3 Peores predicciones (error > 30%) ────────────────────
println("\n" + "═" * 60)
println("  PREDICCIONES CON MAYOR ERROR — error > 30%")
println("═" * 60)
predConPrecio.select("make_name", "year", "mileage","precio_real", "precio_predicho", "error_abs_usd", "error_pct")
.filter(col("error_pct") > 30.0).orderBy(col("error_pct").desc).show(10, truncate = false)

// ── 7.4 Resumen estadístico del error porcentual ──────────────
println("\n" + "═" * 60)
println("  RESUMEN DEL ERROR PORCENTUAL EN ESCALA ORIGINAL")
println("═" * 60)
predConPrecio.select(
  count("*")                                          .alias("total_predicciones"),
  round(mean("error_pct"),   2)                       .alias("error_pct_medio"),
  round(expr("percentile_approx(error_pct, 0.5)"), 2) .alias("error_pct_mediana"),
  round(min("error_pct"),    2)                       .alias("error_pct_min"),
  round(max("error_pct"),    2)                       .alias("error_pct_max"),
  count(when(col("error_pct") < 5,  true))            .alias("n_error_menor_5pct"),
  count(when(col("error_pct") < 10, true))            .alias("n_error_menor_10pct"),
  count(when(col("error_pct") < 20, true))            .alias("n_error_menor_20pct")
).show(truncate = false)

// ══════════════════════════════════════════════════════════════
// 8. LIBERAR CACHÉ
// ══════════════════════════════════════════════════════════════
dfTestCast.unpersist()

println("\n" + "═" * 60)
println(s"  ✅ Evaluación completada — Modelo final: $nombreFinal")
println("═" * 60)