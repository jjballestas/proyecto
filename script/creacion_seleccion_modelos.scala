:load Utils.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.{LinearRegression, GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
  
val spark = SparkSession.builder().appName("Modelado Regresion").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 200)

 
val PATH                 = "/home/usuario/regresion/proyecto/"
val RAWDATA              = "dataset/used_cars_data.csv"
val RAWPARQUET           = "dataset/parquet/raw_data"
val FORCE_CREATE_PARQUET = false
val FORCE_PREPROCESS     = false
val FORCE_SPLIT          = true
val trainPath            = PATH + "dataset/parquet/train"
val testPath             = PATH + "dataset/parquet/test"

 
val fs          = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
val trainExiste = fs.exists(new org.apache.hadoop.fs.Path(trainPath))
val testExiste  = fs.exists(new org.apache.hadoop.fs.Path(testPath))
 
val (dfTrain, dfTest) = if (!FORCE_SPLIT && trainExiste && testExiste) {
  
  (spark.read.parquet(trainPath), spark.read.parquet(testPath))

} else {
  
  val dfload = Utils.cargarOPrepararDataset(spark, PATH, RAWDATA, RAWPARQUET,forceCreateParquet = FORCE_CREATE_PARQUET,forcePreprocess= FORCE_PREPROCESS)

  val colsToDropModelo = Seq("city","exterior_color","interior_color", "model_name","trim_name","listed_year","listed_month","is_classic")
  val dfClean = Utils.dropColumns(dfload, colsToDropModelo)
  println(s"   Columnas eliminadas: ${colsToDropModelo.length} → quedan ${dfClean.columns.length}")

  Utils.mostrarResumenFinal(dfClean)

  val dfFinal = Utils.crearSubconjuntoControlado(
    dfClean, targetSize = 200000, seed = 42L, minRowsPerStratum = 500)
  println(s"   Filas: ${dfFinal.count()} | Columnas: ${dfFinal.columns.length}")

  Utils.crearOCargarSplit(
    spark       = spark,
    df          = dfFinal,
    trainPath   = trainPath,
    testPath    = testPath,
    trainExiste = trainExiste,
    testExiste  = testExiste,
    forceSplit  = FORCE_SPLIT
  )
}

println(f"\n   Train : ${dfTrain.count()}%,d registros")
println(f"   Test  : ${dfTest.count()}%,d registros")

 //Utils.mostrarResumenFinal(dfTest)
 

 import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.{LinearRegression, GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

// ── 1. Definir columnas por tipo ──────────────────────────────
val strCols = Array("make_name","body_type","fuel_type_clean","engine_type",
  "fleet","frame_damaged","franchise_make","has_accidents",
  "salvage","theft_title","transmission","wheel_system",
  "geo_region","urban_level")

val boolCols = Array("is_new","franchise_dealer")

val numCols = Array("gama_alta","is_pickup","daysonmarket","highway_fuel_economy",
  "horsepower","mileage","owner_count","seller_rating","year",
  "power_num","torque_num","engine_cylinders_num","back_legroom_num",
  "front_legroom_num","fuel_tank_volume_num","height_num","length_num",
  "maximum_seating_num","wheelbase_num","width_num","dist_to_major_city_miles",
  "vehicle_age_at_listing","owner_count_missing","height_num_missing",
  "length_num_missing","width_num_missing","wheelbase_num_missing",
  "maximum_seating_num_missing","front_legroom_num_missing",
  "back_legroom_num_missing","missing_dimensions","power_num_missing",
  "torque_num_missing","engine_cylinders_num_missing",
  "engine_displacement_missing","fuel_tank_volume_num_missing",
  "city_fuel_economy_missing","highway_fuel_economy_missing",
  "horsepower_missing","mileage_missing","seller_rating_missing",
  "power_density")

// ── 2. Cast Boolean → Double ──────────────────────────────────
val dfTrainCast = dfTrain.withColumn("is_new",col("is_new").cast("double"))
.withColumn("franchise_dealer", col("franchise_dealer").cast("double"))

val dfTestCast = dfTest.withColumn("is_new",col("is_new").cast("double"))
.withColumn("franchise_dealer", col("franchise_dealer").cast("double"))

val boolAsCols = Array("is_new","franchise_dealer")

// ── 3. StringIndexer para cada categórica ─────────────────────
val indexers = strCols.map { c =>
  new StringIndexer().setInputCol(c).setOutputCol(s"${c}_idx").setHandleInvalid("keep")
}

// ── 4. OneHotEncoder para todas las indexadas ─────────────────
val encoder = new OneHotEncoder().setInputCols(strCols.map(c => s"${c}_idx")).setOutputCols(strCols.map(c => s"${c}_ohe")).setDropLast(true)

// ── 5. VectorAssembler — une todo ─────────────────────────────
val allFeatureCols = strCols.map(c => s"${c}_ohe") ++ numCols ++ boolAsCols

val assembler = new VectorAssembler().setInputCols(allFeatureCols).setOutputCol("features_raw").setHandleInvalid("keep")

// ── 6. StandardScaler — necesario para Regresión Lineal ───────
val scaler = new StandardScaler().setInputCol("features_raw").setOutputCol("features").setWithMean(true).setWithStd(true)

// ── 7. Etapas comunes a todos los modelos ─────────────────────
val etapasBase = indexers ++ Array(encoder, assembler, scaler)

println(s"   Pipeline base definido: ${etapasBase.length} etapas")
println(s"   Features totales estimadas: ${strCols.length} OHE + ${numCols.length} num + ${boolAsCols.length} bool")

 
// ── Evaluador común ───────────────────────────────────────────
val evaluator = new RegressionEvaluator().setLabelCol("log_price").setPredictionCol("prediction")

// ── Modelo 1: Regresión Lineal con ElasticNet ─────────────────
val lr = new LinearRegression().setLabelCol("log_price").setFeaturesCol("features").setMaxIter(100)
.setElasticNetParam(0.5).setRegParam(0.01)

val pipelineLR = new Pipeline().setStages(etapasBase ++ Array(lr))

// ── Modelo 2: Gradient Boosted Trees ─────────────────────────
val gbt = new GBTRegressor().setLabelCol("log_price").setFeaturesCol("features_raw").setMaxIter(50).setMaxDepth(5).setStepSize(0.1)

val etapasBaseGBT = indexers ++ Array(encoder, assembler)   

val pipelineGBT = new Pipeline().setStages(etapasBaseGBT ++ Array(gbt))

// ── Modelo 3: Random Forest ───────────────────────────────────
val rf = new RandomForestRegressor().setLabelCol("log_price").setFeaturesCol("features_raw").setNumTrees(50).setMaxDepth(5)
 
val pipelineRF = new Pipeline().setStages(etapasBaseGBT ++ Array(rf))   

// ── Entrenar los tres modelos ─────────────────────────────────
println("   Entrenando Regresión Lineal...")
val modeloLR  = pipelineLR.fit(dfTrainCast)
println("   LR entrenado")

println("   Entrenando GBT...")
val modeloGBT = pipelineGBT.fit(dfTrainCast)
println("   GBT entrenado")

println("   Entrenando Random Forest...")
val modeloRF  = pipelineRF.fit(dfTrainCast)
println("   RF entrenado")

 
Utils.evaluarModelo("Regresión Lineal (ElasticNet)", modeloLR,  dfTestCast, evaluator, Some(dfTrainCast))
Utils.evaluarModelo("Gradient Boosted Trees",        modeloGBT, dfTestCast, evaluator, Some(dfTrainCast))
Utils.evaluarModelo("Random Forest",                 modeloRF,  dfTestCast, evaluator, Some(dfTrainCast))



 
 import org.apache.spark.ml.tuning.ParamGridBuilder

val resultadosPath = PATH + "resultados/"

// ══════════════════════════════════════════════════════════════
// LR — Ronda 1
// ══════════════════════════════════════════════════════════════
val modeloLR_R1 = Utils.ejecutarBusquedaCV(
  spark, "LR Ronda 1", pipelineLR,
  new ParamGridBuilder()
    .addGrid(lr.regParam,        Array(0.001, 0.01, 0.1))
    .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
    .build(),
  dfTrainCast, evaluator, numFolds = 5,
  resultadosPath = resultadosPath + "cv_lr_r1"
)

val bestPipelineLR_R1 = modeloLR_R1.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejorLR_R1        = bestPipelineLR_R1.stages.last.asInstanceOf[org.apache.spark.ml.regression.LinearRegressionModel]
val mejorRegParam     = mejorLR_R1.getRegParam
val mejorElastic      = mejorLR_R1.getElasticNetParam
println(f"  📌 Mejor R1 → regParam=$mejorRegParam%.4f  elasticNet=$mejorElastic%.4f")

// ── LR Ronda 2 — grid dinámico alrededor del mejor ────────────
val modeloLROpt = Utils.ejecutarBusquedaCV(
  spark, "LR Ronda 2", pipelineLR,
  new ParamGridBuilder()
    .addGrid(lr.regParam, Array(
      math.max(0.0001, mejorRegParam / 5),
      math.max(0.0001, mejorRegParam / 2),
      mejorRegParam,
      math.min(1.0, mejorRegParam * 2),
      math.min(1.0, mejorRegParam * 5)).distinct.sorted)
    .addGrid(lr.elasticNetParam,
      if (mejorElastic == 0.0)      Array(0.0, 0.1, 0.3)
      else if (mejorElastic == 1.0) Array(0.7, 0.9, 1.0)
      else Array(
        math.max(0.0, mejorElastic - 0.2),
        mejorElastic,
        math.min(1.0, mejorElastic + 0.2)))
    .build(),
  dfTrainCast, evaluator, numFolds = 3,
  resultadosPath = resultadosPath + "cv_lr_r2"
)

val bestPipelineLR  = modeloLROpt.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejoresParamsLR = bestPipelineLR.stages.last.asInstanceOf[org.apache.spark.ml.regression.LinearRegressionModel]
println(f"  📌 FINAL LR → regParam=${mejoresParamsLR.getRegParam}%.4f  elasticNet=${mejoresParamsLR.getElasticNetParam}%.4f")

// ══════════════════════════════════════════════════════════════
// GBT — Ronda 1
// ══════════════════════════════════════════════════════════════
val modeloGBT_R1 = Utils.ejecutarBusquedaCV(
  spark, "GBT Ronda 1", pipelineGBT,
  new ParamGridBuilder()
    .addGrid(gbt.maxDepth, Array(3, 5, 7))
    .addGrid(gbt.stepSize, Array(0.05, 0.1, 0.2))
    .build(),
  dfTrainCast, evaluator, numFolds = 5,
  resultadosPath = resultadosPath + "cv_gbt_r1"
)

val bestPipelineGBT_R1 = modeloGBT_R1.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejorGBT_R1        = bestPipelineGBT_R1.stages.last.asInstanceOf[org.apache.spark.ml.regression.GBTRegressionModel]
val mejorDepth         = mejorGBT_R1.getMaxDepth
val mejorStepSize      = mejorGBT_R1.getStepSize
println(f"  📌 Mejor R1 → maxDepth=$mejorDepth  stepSize=$mejorStepSize%.4f")

// ── GBT Ronda 2 — grid dinámico alrededor del mejor ──────────
val modeloGBTOpt = Utils.ejecutarBusquedaCV(
  spark, "GBT Ronda 2", pipelineGBT,
  new ParamGridBuilder()
    .addGrid(gbt.maxDepth, Array(
      math.max(2, mejorDepth - 1),
      mejorDepth,
      math.min(8, mejorDepth + 1)).distinct.sorted)
    .addGrid(gbt.stepSize, Array(
      math.max(0.01, mejorStepSize / 2),
      mejorStepSize,
      math.min(0.5,  mejorStepSize * 1.5)).distinct.sorted)
    .addGrid(gbt.maxIter, Array(50, 100))
    .build(),
  dfTrainCast, evaluator, numFolds = 3,
  resultadosPath = resultadosPath + "cv_gbt_r2"
)

val bestPipelineGBT  = modeloGBTOpt.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val mejoresParamsGBT = bestPipelineGBT.stages.last.asInstanceOf[org.apache.spark.ml.regression.GBTRegressionModel]
println(f"  📌 FINAL GBT → maxDepth=${mejoresParamsGBT.getMaxDepth}  stepSize=${mejoresParamsGBT.getStepSize}%.4f  maxIter=${mejoresParamsGBT.getMaxIter}")

// ── Guardar modelos finales ───────────────────────────────────
val modelosPath = PATH + "modelos/"
bestPipelineLR.write.overwrite().save(modelosPath + "lr_optimizado")
bestPipelineGBT.write.overwrite().save(modelosPath + "gbt_optimizado")
println("  ✅ Modelos guardados en disco")

// ── Evaluación comparativa final ─────────────────────────────
Utils.evaluarModelo("LR  Baseline",   modeloLR,        dfTestCast, evaluator, Some(dfTrainCast))
Utils.evaluarModelo("GBT Baseline",   modeloGBT,       dfTestCast, evaluator, Some(dfTrainCast))
Utils.evaluarModelo("LR  Optimizado", bestPipelineLR,  dfTestCast, evaluator, Some(dfTrainCast))
Utils.evaluarModelo("GBT Optimizado", bestPipelineGBT, dfTestCast, evaluator, Some(dfTrainCast))