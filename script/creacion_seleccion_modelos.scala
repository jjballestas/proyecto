:load Utils_model.scala
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.{LinearRegression, GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder

// ══════════════════════════════════════════════════════════════
// 0. SESIÓN Y PARÁMETROS GLOBALES
// ══════════════════════════════════════════════════════════════
val spark = SparkSession.builder().appName("Modelado Regresion").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 200)

val PATH                 = "/home/usuario/regresion/proyecto/"
val RAWDATA              = "dataset/used_cars_data.csv"
val RAWPARQUET           = "dataset/parquet/raw_data"
val FORCE_CREATE_PARQUET = false
val FORCE_PREPROCESS     = false
val FORCE_SPLIT          = false
val trainPath            = PATH + "dataset/parquet/train"
val testPath             = PATH + "dataset/parquet/test"
val FORCE_SEARCH         = false
// ══════════════════════════════════════════════════════════════
// 1. CARGA Y SPLIT TRAIN / TEST
// ══════════════════════════════════════════════════════════════
val fs          = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
val trainExiste = fs.exists(new org.apache.hadoop.fs.Path(trainPath))
val testExiste  = fs.exists(new org.apache.hadoop.fs.Path(testPath))


val (dfTrain, dfTest) = if (!FORCE_SPLIT && trainExiste && testExiste) {
  (spark.read.parquet(trainPath), spark.read.parquet(testPath))
} else {
  val dfload = cargarOPrepararDataset(spark, PATH, RAWDATA, RAWPARQUET, FORCE_CREATE_PARQUET, FORCE_PREPROCESS)

  val colsToDropModelo = Seq("city", "exterior_color", "interior_color", "model_name", "trim_name")

  val dfClean = dropColumns(dfload, colsToDropModelo)
  println(s"   Columnas eliminadas: ${colsToDropModelo.length} → quedan ${dfClean.columns.length}")

  val dfFinal = crearSubconjuntoControlado(
    dfClean, targetSize = 150000, seed = 42L, minRowsPerStratum = 250
  )
  println(s"   Filas: ${dfFinal.count()} | Columnas: ${dfFinal.columns.length}")
  crearOCargarSplit(
    spark       = spark,
    df          = dfFinal,
    trainPath   = trainPath,
    testPath    = testPath,
    trainExiste = trainExiste,
    testExiste  = testExiste,
    forceSplit  = FORCE_SPLIT
  )
}



  println("  RESUMEN DATASET TRAIN ")

  mostrarResumenFinal(dfTrain)

  println(" \n RESUMEN DATASET TEST ")
  mostrarResumenFinal(dfTest)

    
  val numColsCandidatas = Array(
    "latitude", "longitude", "gama_alta", "is_pickup", "daysonmarket",
    "highway_fuel_economy", "horsepower", "mileage", "owner_count",
    "seller_rating", "year",
    "power_num", "torque_num", "engine_cylinders_num", "back_legroom_num",
    "front_legroom_num", "fuel_tank_volume_num", "height_num", "length_num",
    "maximum_seating_num", "wheelbase_num", "width_num",
    "vehicle_age_at_listing", "owner_count_missing", "height_num_missing",
    "length_num_missing", "width_num_missing", "wheelbase_num_missing",
    "maximum_seating_num_missing", "front_legroom_num_missing",
    "back_legroom_num_missing", "missing_dimensions", "power_num_missing",
    "torque_num_missing", "engine_cylinders_num_missing",
    "engine_displacement_missing", "fuel_tank_volume_num_missing",
    "city_fuel_economy_missing", "highway_fuel_economy_missing",
    "horsepower_missing", "mileage_missing", "seller_rating_missing",
    "power_density",
    "description_length", "option_count",
    "has_offroadpackage", "has_navigationsystem", "has_thirdrowseating",
    "has_sunroof_moonroof", "has_parkingsensors", "has_heatedseats",
    "has_adaptivecruisecontrol", "has_blindspotmonitoring",
    "has_backupcamera", "has_leatherseats", "has_multizoneclimatecontrol","segmento_estado"
  )


val strColsCandidatas = Array(
  "make_name", "body_type", "fuel_type_clean", "engine_type",
  "fleet", "has_accidents", "transmission", "wheel_system"
)

val boolColsCandidatas = Array("is_new", "franchise_dealer")

// ══════════════════════════════════════════════════════════════
// 3. RESOLVER COLUMNAS DISPONIBLES EN EL DATAFRAME
// ══════════════════════════════════════════════════════════════
val (numCols, strCols, boolCols) = resolverColumnas(
  dfTrain, numColsCandidatas, strColsCandidatas, boolColsCandidatas
)
val boolAsCols = boolCols

// ══════════════════════════════════════════════════════════════
// 4. CAST BOOLEANOS → DOUBLE
// ══════════════════════════════════════════════════════════════
val dfTrainCast = dfTrain.withColumn("is_new",col("is_new").cast("double"))
.withColumn("franchise_dealer", col("franchise_dealer").cast("double")).cache()

dfTrainCast.count()

val dfTestCast = dfTest.withColumn("is_new",col("is_new").cast("double"))
.withColumn("franchise_dealer", col("franchise_dealer").cast("double"))


println("\n  🔍 Verificando varianza de columnas numéricas...")

val numColsValidas = numCols.filter { c =>
  val row = dfTrainCast.select(stddev(col(c))).first()
  val esValida = !row.isNullAt(0) && {
    val std = row.getDouble(0)
    !std.isNaN && std > 0.0
  }
  if (!esValida) println(s"  ⚠️  Excluida (varianza cero): $c")
  esValida
}
println(s"  ✅ numCols válidas: ${numColsValidas.length} / ${numCols.length}")

// ══════════════════════════════════════════════════════════════
// 6. CONSTRUCCIÓN DE PIPELINES
// ══════════════════════════════════════════════════════════════

// ── 6.1 Feature columns finales ──────────────────────────────
val allFeatureCols = strCols.map(c => s"${c}_ohe") ++ numColsValidas ++ boolAsCols

// ── 6.2 Etapas de encoding (dependen de strCols resueltos) ───
val indexers = strCols.map { c =>new StringIndexer().setInputCol(c).setOutputCol(s"${c}_idx").setHandleInvalid("keep")}

val encoder = new OneHotEncoder().setInputCols(strCols.map(c => s"${c}_idx")).setOutputCols(strCols.map(c => s"${c}_ohe")).setDropLast(true)

// ── 6.3 Assembler escalado (LR) ───────────────────────────────
val assemblerScaled = new VectorAssembler().setInputCols(allFeatureCols).setOutputCol("features_raw").setHandleInvalid("keep")

val scaler = new StandardScaler().setInputCol("features_raw").setOutputCol("features").setWithMean(true).setWithStd(true)

// ── 6.4 Assembler sin escalar (árboles) ───────────────────────
val assemblerRaw = new VectorAssembler().setInputCols(allFeatureCols).setOutputCol("features_raw").setHandleInvalid("keep")

// ── 6.5 Etapas base por tipo de modelo ───────────────────────
val etapasLR  = indexers ++ Array(encoder, assemblerScaled, scaler)
val etapasArb = indexers ++ Array(encoder, assemblerRaw)

println(s"\n   Pipeline LR  : ${etapasLR.length} etapas (con scaler)")
println(s"   Pipeline ARB : ${etapasArb.length} etapas (sin scaler)")
println(s"   Features: ${strCols.length} OHE + ${numColsValidas.length} num + ${boolAsCols.length} bool")

// ── 6.6 Definición de modelos ─────────────────────────────────
val lr = new LinearRegression().setLabelCol("log_price").setFeaturesCol("features").setMaxIter(100).setElasticNetParam(0.5).setRegParam(0.01)

//val gbt = new GBTRegressor().setLabelCol("log_price").setFeaturesCol("features_raw").setMaxIter(100).setMaxDepth(2).setStepSize(0.1)


val gbt = new GBTRegressor().setLabelCol("log_price").setFeaturesCol("features_raw")
.setMaxIter(300).setMaxDepth(5).setStepSize(0.1).setValidationIndicatorCol("is_validation")
.setSeed(42L)


val rf = new RandomForestRegressor().setLabelCol("log_price").setFeaturesCol("features_raw").setNumTrees(150).setMaxDepth(11).setSeed(42L)

// ── 6.7 Pipelines completos ───────────────────────────────────
val pipelineLR  = new Pipeline().setStages(etapasLR  ++ Array(lr))
val pipelineGBT = new Pipeline().setStages(etapasArb ++ Array(gbt))
val pipelineRF  = new Pipeline().setStages(etapasArb ++ Array(rf))

// ══════════════════════════════════════════════════════════════
// 7. EVALUADOR COMÚN
// ══════════════════════════════════════════════════════════════
val evaluator = new RegressionEvaluator().setLabelCol("log_price").setPredictionCol("prediction")

val resultadosPath = PATH + "resultados/"

// ══════════════════════════════════════════════════════════════
// 8. MODELO 1: REGRESIÓN LINEAL — 2 RONDAS TVS
// ══════════════════════════════════════════════════════════════
println("\n" + "═" * 60)
println("  MODELO 1: REGRESIÓN LINEAL (ElasticNet)")
println("═" * 60)

// ── Ronda 1: grid amplio ──────────────────────────────────────
val modeloLR_R1 = Utils_models.ejecutarBusquedaTVS(
  spark, "LR Ronda 1", pipelineLR,
  new ParamGridBuilder().addGrid(lr.regParam,Array(0.0002, 0.0005, 0.001, 0.002, 0.005))
  .addGrid(lr.elasticNetParam,  Array(0.7, 0.9, 1.0)).build(),
  dfTrainCast, evaluator,
  trainRatio     = 0.8,
  resultadosPath = resultadosPath + "tvs_lr_r1",
  forceSearch    = FORCE_SEARCH
)

 

val bestPipelineLR_R1 = modeloLR_R1.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejorLR_R1        = bestPipelineLR_R1.stages.last.asInstanceOf[org.apache.spark.ml.regression.LinearRegressionModel]
val mejorRegParam     = mejorLR_R1.getRegParam
val mejorElastic      = mejorLR_R1.getElasticNetParam
println(f"  📌 Mejor R1 LR → regParam=$mejorRegParam%.4f  elasticNet=$mejorElastic%.4f")

// ── Ronda 2: refinamiento alrededor del mejor ─────────────────
val elasticNetGrid: Array[Double] = {
  if (mejorElastic == 0.0) Array(0.0002, 0.0005, 0.001, 0.002, 0.005)
  else if (mejorElastic == 1.0) Array(0.7, 0.9, 1.0)
  else Array(
    math.max(0.0, mejorElastic - 0.2),
    mejorElastic,
    math.min(1.0, mejorElastic + 0.2)
  )
}


val regParamGrid: Array[Double] = Array(
  math.max(0.0001, mejorRegParam / 5),
  math.max(0.0001, mejorRegParam / 2),
  mejorRegParam,
  math.min(1.0,    mejorRegParam * 2),
  math.min(1.0,    mejorRegParam * 5)
).distinct.sorted

val modeloLR_R2 = Utils_models.ejecutarBusquedaTVS(
  spark, "LR Ronda 2", pipelineLR,
  new ParamGridBuilder().addGrid(lr.regParam,regParamGrid).addGrid(lr.elasticNetParam, elasticNetGrid).build(),
  dfTrainCast, evaluator,
  trainRatio     = 0.8,
  resultadosPath = resultadosPath + "tvs_lr_r2",
  forceSearch    = FORCE_SEARCH
)

val bestPipelineLR  = modeloLR_R2.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejoresParamsLR = bestPipelineLR.stages.last.asInstanceOf[org.apache.spark.ml.regression.LinearRegressionModel]
println(f"  📌 FINAL LR → regParam=${mejoresParamsLR.getRegParam}%.4f  elasticNet=${mejoresParamsLR.getElasticNetParam}%.4f")



Utils_models.analizarCoeficientesLR(spark = spark,bestPipelineLR = bestPipelineLR,dfTrainCast = dfTrainCast,
topN = 30,outputPathOpt = Some(resultadosPath + "lr_final"))

// ══════════════════════════════════════════════════════════════
// 9. MODELO 2: GRADIENT BOOSTED TREES — 2 RONDAS TVS
// ══════════════════════════════════════════════════════════════
println("\n" + "═" * 60)
println("  MODELO 2: GRADIENT BOOSTED TREES")
println("═" * 60)


val dfTrainGBT = dfTrainCast.withColumn("is_validation", rand(seed = 42L) < 0.2)

// ── Ronda 1: grid amplio ──────────────────────────────────────
val modeloGBT_R1 = Utils_models.ejecutarBusquedaTVS(
  spark, "GBT Ronda 1", pipelineGBT,
  new ParamGridBuilder().addGrid(gbt.maxDepth, Array(7, 8, 9))
  .addGrid(gbt.stepSize, Array(0.15, 0.2, 0.25)).build(),dfTrainGBT,evaluator,
  trainRatio     = 0.8,resultadosPath = resultadosPath + "tvs_gbt_r1",
  forceSearch    = FORCE_SEARCH
)


val bestPipelineGBT_R1 = modeloGBT_R1.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejorGBT_R1        = bestPipelineGBT_R1.stages.last.asInstanceOf[org.apache.spark.ml.regression.GBTRegressionModel]
val mejorDepth         = mejorGBT_R1.getMaxDepth
val mejorStepSize      = mejorGBT_R1.getStepSize
val mejorIter     = mejorGBT_R1.getMaxIter
val maxIterR1 = 150
val iterGridR2 = if (mejorIter >= maxIterR1) { 
    Array(mejorIter, mejorIter * 2) 
} else {
     
    Array(math.max(10, (mejorIter * 0.8).toInt), mejorIter).distinct.sorted
}

println(f"  📌 Mejor R1 GBT → maxDepth=$mejorDepth  stepSize=$mejorStepSize%.4f  maxIter=$mejorIter")

// ── Ronda 2: refinamiento ─────────────────────────────────────
val modeloGBT_R2 = Utils_models.ejecutarBusquedaTVS(
  spark, "GBT Ronda 2", pipelineGBT,
  new ParamGridBuilder().addGrid(gbt.maxDepth, Array(
        math.max(2, mejorDepth - 1),
        mejorDepth,
        math.min(8, mejorDepth + 1)
      ).distinct.sorted)
    .addGrid(gbt.stepSize, Array(
        math.max(0.01, mejorStepSize / 2),
        mejorStepSize,
        math.min(0.5,  mejorStepSize * 1.5)
      ).distinct.sorted).addGrid(gbt.maxIter, iterGridR2).build(),
  dfTrainGBT, evaluator,
  trainRatio     = 0.8,
  resultadosPath = resultadosPath + "tvs_gbt_r2",
  forceSearch    = FORCE_SEARCH

)

val bestPipelineGBT  = modeloGBT_R2.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejoresParamsGBT = bestPipelineGBT.stages.last.asInstanceOf[org.apache.spark.ml.regression.GBTRegressionModel]
println(f"  📌 FINAL GBT → maxDepth=${mejoresParamsGBT.getMaxDepth}  stepSize=${mejoresParamsGBT.getStepSize}%.4f  maxIter=${mejoresParamsGBT.getMaxIter}")

// ══════════════════════════════════════════════════════════════
// 10. MODELO 3: RANDOM FOREST — 2 RONDAS TVS
// ══════════════════════════════════════════════════════════════
println("\n" + "═" * 60)
println("  MODELO 3: RANDOM FOREST")
println("═" * 60)

// ── Ronda 1: grid amplio ──────────────────────────────────────
val modeloRF_R1 = Utils_models.ejecutarBusquedaTVS(
  spark, "RF Ronda 1", pipelineRF,
  new ParamGridBuilder().addGrid(rf.numTrees, Array(150, 200, 300)).addGrid(rf.maxDepth, Array(12, 14, 16)).build(),
  dfTrainCast, evaluator,
  trainRatio     = 0.8,
  resultadosPath = resultadosPath + "tvs_rf_r1",
  forceSearch    = FORCE_SEARCH

)

val bestPipelineRF_R1 = modeloRF_R1.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejorRF_R1        = bestPipelineRF_R1.stages.last.asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel].asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel]
val mejorNumTrees     = mejorRF_R1.getNumTrees
val mejorRFDepth      = mejorRF_R1.getMaxDepth
println(f"  📌 Mejor R1 RF → numTrees=$mejorNumTrees  maxDepth=$mejorRFDepth")

 
 
val depthGridR2 = (if (mejorRFDepth >= 14) {
  Array(mejorRFDepth, mejorRFDepth + 1, mejorRFDepth + 2)
} else if (mejorRFDepth >= 12) {
  Array(12, 13, 14)
} else {
  Array(math.max(3, mejorRFDepth - 1), mejorRFDepth, mejorRFDepth + 1)
}).distinct.sorted

 
val treesGridR2 = Array(mejorNumTrees)

val modeloRF_R2 = Utils_models.ejecutarBusquedaTVS(
  spark, "RF Ronda 2", pipelineRF,
  new ParamGridBuilder().addGrid(rf.numTrees, treesGridR2).addGrid(rf.maxDepth,depthGridR2)
  .addGrid(rf.minInstancesPerNode, Array(1, 3, 5)).build(),
  dfTrainCast, evaluator,
  trainRatio     = 0.8,
  resultadosPath = resultadosPath + "tvs_rf_r2",
  forceSearch    = FORCE_SEARCH
)
 



val bestPipelineRF  = modeloRF_R2.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejoresParamsRF = bestPipelineRF.stages.last.asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel]
println(f"  📌 FINAL RF → numTrees=${mejoresParamsRF.getNumTrees}  maxDepth=${mejoresParamsRF.getMaxDepth}  minInst=${mejoresParamsRF.getMinInstancesPerNode}")

// ══════════════════════════════════════════════════════════════
// 11. GUARDAR MODELOS FINALES
// ══════════════════════════════════════════════════════════════
val modelosPath = PATH + "modelos/"
bestPipelineLR.write.overwrite().save(modelosPath  + "lr_optimizado")
bestPipelineGBT.write.overwrite().save(modelosPath + "gbt_optimizado")
bestPipelineRF.write.overwrite().save(modelosPath  + "rf_optimizado")
println("\n  ✅ Tres modelos guardados en disco")

// ══════════════════════════════════════════════════════════════
// 12. EVALUACIÓN COMPARATIVA FINAL SOBRE TEST
// ══════════════════════════════════════════════════════════════
println("\n" + "═" * 60)
println("  COMPARATIVA FINAL — TEST SET")
println("═" * 60)

Utils_models.evaluarModelo("LR  Ronda 1 (baseline)",
  modeloLR_R1.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel],
  dfTestCast, evaluator, Some(dfTrainCast))

Utils_models.evaluarModelo("LR  Optimizado",
  bestPipelineLR, dfTestCast, evaluator, Some(dfTrainCast))

Utils_models.evaluarModelo("GBT Ronda 1 (baseline)",
  modeloGBT_R1.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel],
  dfTestCast, evaluator, Some(dfTrainCast))

Utils_models.evaluarModelo("GBT Optimizado",
  bestPipelineGBT, dfTestCast, evaluator, Some(dfTrainCast))

Utils_models.evaluarModelo("RF  Ronda 1 (baseline)",
  modeloRF_R1.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel],
  dfTestCast, evaluator, Some(dfTrainCast))

Utils_models.evaluarModelo("RF  Optimizado",
  bestPipelineRF, dfTestCast, evaluator, Some(dfTrainCast))


dfTrainCast.unpersist()