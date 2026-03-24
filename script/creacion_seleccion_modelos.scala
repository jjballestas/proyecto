:load Utils.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

  
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

 
val fs          = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
val trainExiste = fs.exists(new org.apache.hadoop.fs.Path(trainPath))
val testExiste  = fs.exists(new org.apache.hadoop.fs.Path(testPath))
 
val (dfTrain, dfTest) = if (!FORCE_SPLIT && trainExiste && testExiste) {
  
  (spark.read.parquet(trainPath), spark.read.parquet(testPath))

} else {
  
  val dfload = Utils.cargarOPrepararDataset(
    spark, PATH, RAWDATA, RAWPARQUET,
    forceCreateParquet = FORCE_CREATE_PARQUET,
    forcePreprocess    = FORCE_PREPROCESS
  )

  val colsToDropModelo = Seq("city","exterior_color","interior_color",
    "model_name","trim_name","listed_year","listed_month","is_classic")
  val dfClean = Utils.dropColumns(dfload, colsToDropModelo)
  println(s"  ✅ Columnas eliminadas: ${colsToDropModelo.length} → quedan ${dfClean.columns.length}")

  Utils.mostrarResumenFinal(dfClean)

  val dfFinal = Utils.crearSubconjuntoControlado(
    dfClean, targetSize = 200000, seed = 42L, minRowsPerStratum = 500)
  println(s"  📌 Filas: ${dfFinal.count()} | Columnas: ${dfFinal.columns.length}")

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

println(f"\n  📌 Train : ${dfTrain.count()}%,d registros")
println(f"  📌 Test  : ${dfTest.count()}%,d registros")

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
val dfTrainCast = dfTrain
  .withColumn("is_new",           col("is_new").cast("double"))
  .withColumn("franchise_dealer", col("franchise_dealer").cast("double"))

val dfTestCast = dfTest
  .withColumn("is_new",           col("is_new").cast("double"))
  .withColumn("franchise_dealer", col("franchise_dealer").cast("double"))

val boolAsCols = Array("is_new","franchise_dealer")

// ── 3. StringIndexer para cada categórica ─────────────────────
val indexers = strCols.map { c =>
  new StringIndexer()
    .setInputCol(c)
    .setOutputCol(s"${c}_idx")
    .setHandleInvalid("keep")
}

// ── 4. OneHotEncoder para todas las indexadas ─────────────────
val encoder = new OneHotEncoder()
  .setInputCols(strCols.map(c => s"${c}_idx"))
  .setOutputCols(strCols.map(c => s"${c}_ohe"))
  .setDropLast(true)

// ── 5. VectorAssembler — une todo ─────────────────────────────
val allFeatureCols = strCols.map(c => s"${c}_ohe") ++ numCols ++ boolAsCols

val assembler = new VectorAssembler()
  .setInputCols(allFeatureCols)
  .setOutputCol("features_raw")
  .setHandleInvalid("keep")

// ── 6. StandardScaler — necesario para Regresión Lineal ───────
val scaler = new StandardScaler()
  .setInputCol("features_raw")
  .setOutputCol("features")
  .setWithMean(true)
  .setWithStd(true)

// ── 7. Etapas comunes a todos los modelos ─────────────────────
val etapasBase = indexers ++ Array(encoder, assembler, scaler)

println(s"  ✅ Pipeline base definido: ${etapasBase.length} etapas")
println(s"  ✅ Features totales estimadas: ${strCols.length} OHE + ${numCols.length} num + ${boolAsCols.length} bool")





import org.apache.spark.ml.regression.{LinearRegression, GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

// ── Evaluador común ───────────────────────────────────────────
val evaluator = new RegressionEvaluator()
  .setLabelCol("log_price")
  .setPredictionCol("prediction")

// ── Modelo 1: Regresión Lineal con ElasticNet ─────────────────
val lr = new LinearRegression()
  .setLabelCol("log_price")
  .setFeaturesCol("features")
  .setMaxIter(100)
  .setElasticNetParam(0.5)   // 0=Ridge, 1=Lasso, 0.5=ElasticNet
  .setRegParam(0.01)

val pipelineLR = new Pipeline()
  .setStages(etapasBase ++ Array(lr))

// ── Modelo 2: Gradient Boosted Trees ─────────────────────────
val gbt = new GBTRegressor()
  .setLabelCol("log_price")
  .setFeaturesCol("features_raw")  // GBT no necesita scaler
  .setMaxIter(50)
  .setMaxDepth(5)
  .setStepSize(0.1)

val etapasBaseGBT = indexers ++ Array(encoder, assembler)  // sin scaler

val pipelineGBT = new Pipeline()
  .setStages(etapasBaseGBT ++ Array(gbt))

// ── Modelo 3: Random Forest ───────────────────────────────────
val rf = new RandomForestRegressor()
  .setLabelCol("log_price")
  .setFeaturesCol("features_raw")  // RF no necesita scaler
  .setNumTrees(50)
  .setMaxDepth(5)

val pipelineRF = new Pipeline()
  .setStages(etapasBaseGBT ++ Array(rf))  // reutiliza etapas sin scaler

// ── Entrenar los tres modelos ─────────────────────────────────
println("  🔄 Entrenando Regresión Lineal...")
val modeloLR  = pipelineLR.fit(dfTrainCast)
println("  ✅ LR entrenado")

println("  🔄 Entrenando GBT...")
val modeloGBT = pipelineGBT.fit(dfTrainCast)
println("  ✅ GBT entrenado")

println("  🔄 Entrenando Random Forest...")
val modeloRF  = pipelineRF.fit(dfTrainCast)
println("  ✅ RF entrenado")

Utils.evaluarModelo("Regresión Lineal (ElasticNet)", modeloLR,  dfTrainCast, dfTestCast, evaluator)
Utils.evaluarModelo("Gradient Boosted Trees",        modeloGBT, dfTrainCast, dfTestCast, evaluator)
Utils.evaluarModelo("Random Forest",                 modeloRF,  dfTrainCast, dfTestCast, evaluator)


import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

// ── Optimización Regresión Lineal ─────────────────────────────
val paramGridLR = new ParamGridBuilder().addGrid(lr.regParam, Array(0.001, 0.01, 0.1)).addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).build()

val cvLR = new CrossValidator().setEstimator(pipelineLR).setEvaluator(evaluator.setMetricName("rmse")).setEstimatorParamMaps(paramGridLR).setNumFolds(5).setSeed(42)

println("  🔄 Optimizando Regresión Lineal (9 combinaciones x 5 folds)...")
val modeloLROpt = cvLR.fit(dfTrainCast)
println("  ✅ LR optimizado")

// Mejores parámetros LR
val mejoresParamsLR = modeloLROpt.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages.last.asInstanceOf[org.apache.spark.ml.regression.LinearRegressionModel]

println(f"  📌 Mejor regParam       : ${mejoresParamsLR.getRegParam}%.4f")
println(f"  📌 Mejor elasticNetParam: ${mejoresParamsLR.getElasticNetParam}%.4f")

// ── Optimización GBT ─────────────────────────────────────────
val paramGridGBT = new ParamGridBuilder().addGrid(gbt.maxDepth,  Array(3, 5, 7)).addGrid(gbt.stepSize,  Array(0.05, 0.1, 0.2)).build()

val cvGBT = new CrossValidator().setEstimator(pipelineGBT).setEvaluator(evaluator.setMetricName("rmse")).setEstimatorParamMaps(paramGridGBT).setNumFolds(5).setSeed(42)

println("  🔄 Optimizando GBT (9 combinaciones x 5 folds)...")
val modeloGBTOpt = cvGBT.fit(dfTrainCast)
println("  ✅ GBT optimizado")

// Mejores parámetros GBT
val mejoresParamsGBT = modeloGBTOpt.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages.last.asInstanceOf[org.apache.spark.ml.regression.GBTRegressionModel]

println(f"  📌 Mejor maxDepth: ${mejoresParamsGBT.getMaxDepth}")
println(f"  📌 Mejor stepSize: ${mejoresParamsGBT.getStepSize}%.4f")

// ── Guardar modelos optimizados ───────────────────────────────
val modelosPath = PATH + "modelos/"

modeloLROpt.bestModel.save(modelosPath + "lr_optimizado")
println("  ✅ LR optimizado guardado en disco")

modeloGBTOpt.bestModel.save(modelosPath + "gbt_optimizado")
println("  ✅ GBT optimizado guardado en disco")

// ── Evaluar modelos optimizados ───────────────────────────────
Utils.evaluarModelo("LR Optimizado",  modeloLROpt.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel], dfTrainCast, dfTestCast, evaluator)
Utils.evaluarModelo("GBT Optimizado", modeloGBTOpt.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel], dfTrainCast, dfTestCast, evaluator)