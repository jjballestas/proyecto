:load Utils.scala

object Utils_models {

   //esta función muestra los resultados de la búsqueda TVS , con formato tabular y con las métricas .
    def mostrarResultadosTVS(nombre: String,tvsModel: org.apache.spark.ml.tuning.TrainValidationSplitModel,paramGrid: Array[org.apache.spark.ml.param.ParamMap]): Unit = {

      val metrics = tvsModel.validationMetrics

      def limpiarParams(pm: org.apache.spark.ml.param.ParamMap): String =
        pm.toSeq.map { pair =>
          val valor = pair.value match {
            case d: Double => f"$d%.4f".replaceAll("\\.?0+$", "")
            case other     => other.toString
          }
          s"${pair.param.name}=$valor"
        }.mkString("  ")

      println(s"\n  ══════════════════════════════════════════════════════════")
      println(s"  RESULTADOS TVS — $nombre")
      println(s"  ══════════════════════════════════════════════════════════")
      println(f"  ${"#"}%-4s ${"Parámetros"}%-50s ${"RMSE_Val"}%10s")
      println("  " + "-" * 67)

      paramGrid.zip(metrics)
        .sortBy(_._2)
        .zipWithIndex
        .foreach { case ((pm, rmse), idx) =>
          println(f"  ${idx + 1}%-4d ${limpiarParams(pm)}%-50s $rmse%10.4f")
        }

      println(f"\n  ✅ Mejor RMSE Val : ${metrics.min}%.4f")
      println(f"  ⚠️  Peor  RMSE Val : ${metrics.max}%.4f")
      println(f"  📌 Mejora vs peor : ${metrics.max - metrics.min}%.4f")
    }

    //esta función guarda los resultados de la búsqueda TVS en un archivo CSV, con los parámetros y las métricas 
    //ordenados por RMSE de validación.
    def guardarResultadosTVS(spark: SparkSession,nombre: String,tvsModel: org.apache.spark.ml.tuning.TrainValidationSplitModel,paramGrid: Array[org.apache.spark.ml.param.ParamMap],path: String): Unit = {
      import spark.implicits._

      val rows = paramGrid.zip(tvsModel.validationMetrics).map { case (pm, rmse) =>
        (pm.toString.replaceAll("[{}]", "").trim, rmse)
      }.toSeq

      spark.createDataFrame(rows).toDF("params", "rmse_val")
        .orderBy(col("rmse_val").asc)
        .coalesce(1)
        .write.mode("overwrite").option("header", "true").csv(path)

      println(s"  ✅ Resultados TVS $nombre guardados en: $path")
    }

   

    //esta función ejecuta la búsqueda de hiperparámetros utilizando TrainValidationSplit, 
    //con opciones para cargar modelos guardados, forzar reentrenamiento, mostrar resultados y guardar resultados.
    def ejecutarBusquedaTVS(spark: SparkSession,nombre: String,pipeline: org.apache.spark.ml.Pipeline, 
    paramGrid: Array[org.apache.spark.ml.param.ParamMap],dfTrain: DataFrame,
    evaluador: org.apache.spark.ml.evaluation.RegressionEvaluator,trainRatio: Double = 0.8,seed: Long = 42L,
    resultadosPath: String,forceSearch:   Boolean = false ): org.apache.spark.ml.tuning.TrainValidationSplitModel = {

          val modelPath = resultadosPath + "_model"
          val fs        = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
          val modelExists = fs.exists(new org.apache.hadoop.fs.Path(modelPath))

          // ── Cargar modelo guardado si existe y no se fuerza reentrenamiento ──
          if (!forceSearch && modelExists) {
            println(s"\n  ✅ $nombre — cargando modelo guardado (forceSearch=false)")
            println(s"  📂 Ruta: $modelPath")
            val tvsModel = org.apache.spark.ml.tuning.TrainValidationSplitModel.load(modelPath)
            mostrarResultadosTVS(nombre, tvsModel, paramGrid)
            return tvsModel
          }

          // ── Entrenar ──────────────────────────────────────────────────────────
          val pct    = "%.0f".format(trainRatio * 100)
          val pctVal = "%.0f".format((1 - trainRatio) * 100)

          if (forceSearch && modelExists)
            println(s"\n  🔄 $nombre — forceSearch=true, reentrenando...")
          else
            println(s"\n  🔄 $nombre — modelo no encontrado, entrenando...")

          println(s"  🔄 TVS ${pct}/${pctVal} — ${paramGrid.length} combinaciones...")

          val tvs = new org.apache.spark.ml.tuning.TrainValidationSplit()
            .setEstimator(pipeline)
            .setEvaluator(evaluador.setMetricName("rmse"))
            .setEstimatorParamMaps(paramGrid)
            .setTrainRatio(trainRatio)
            .setSeed(seed)

          val tvsModel = tvs.fit(dfTrain)
          println(s"  ✅ $nombre completada")

          // ── Guardar modelo completo ───────────────────────────────────────────
          tvsModel.write.overwrite().save(modelPath)
          println(s"  💾 Modelo TVS guardado en: $modelPath")

          mostrarResultadosTVS(nombre, tvsModel, paramGrid)
          guardarResultadosTVS(spark, nombre, tvsModel, paramGrid, resultadosPath)

          tvsModel
        }

        //esta función evalúa un modelo dado en el conjunto de test, mostrando métricas  como RMSE, MAE, R² y MSE.
        def evaluarModelo(nombre: String,modelo: org.apache.spark.ml.PipelineModel,dfTe: DataFrame,
        evaluador: org.apache.spark.ml.evaluation.RegressionEvaluator,dfTr: Option[DataFrame] = None    ): Unit = {

          val predTest  = modelo.transform(dfTe)
          val rmseTest  = evaluador.setMetricName("rmse").evaluate(predTest)
          val maeTest   = evaluador.setMetricName("mae").evaluate(predTest)
          val r2Test    = evaluador.setMetricName("r2").evaluate(predTest)
          val mseTest   = rmseTest * rmseTest

          dfTr match {
            case Some(train) =>
              // ── Modo completo: train + test ──────────────────────────
              val predTrain = modelo.transform(train)
              val rmseTrain = evaluador.setMetricName("rmse").evaluate(predTrain)
              val maeTrain  = evaluador.setMetricName("mae").evaluate(predTrain)
              val r2Train   = evaluador.setMetricName("r2").evaluate(predTrain)

              
              println(s"  Modelo: $nombre") 
              println(f"  ${"Métrica"}%-10s ${"Train"}%12s ${"Test"}%12s ${"Diferencia"}%12s")
              println(s"  " + "-" * 48)
              println(f"  ${"RMSE"}%-10s $rmseTrain%12.4f $rmseTest%12.4f ${rmseTest - rmseTrain}%12.4f")
              println(f"  ${"MAE"}%-10s $maeTrain%12.4f $maeTest%12.4f ${maeTest - maeTrain}%12.4f")
              println(f"  ${"R²"}%-10s $r2Train%12.4f $r2Test%12.4f ${r2Test - r2Train}%12.4f")
              val overfitting = if (math.abs(r2Train - r2Test) > 0.05) "⚠️  posible overfitting" else "✅ sin overfitting"
              println(s"  $overfitting")

            case None =>
            
              println(s"  Modelo: $nombre") 
              println(f"  R²   : $r2Test%.4f")
              println(f"  RMSE : $rmseTest%.4f  (error típico ≈ ${(math.exp(rmseTest) - 1) * 100}%.1f%% del precio)")
              println(f"  MAE  : $maeTest%.4f")
              println(f"  MSE  : $mseTest%.6f")
          }
        }

        //esta función analiza los coeficientes del modelo de regresión lineal, 
        //mostrando los coeficientes más importantes, estadísticas sobre coeficientes nulos y un resumen por grupo de variables.
        def analizarCoeficientesLR(spark:         SparkSession,bestPipelineLR: org.apache.spark.ml.PipelineModel,dfTrainCast:   DataFrame,topN:          Int = 20,outputPathOpt: Option[String] = None): Unit = {

      // 1. Extraer el modelo lineal final
      val modeloLinealFinal = bestPipelineLR.stages.last.asInstanceOf[org.apache.spark.ml.regression.LinearRegressionModel]

      // 2. Transformar train aplicando todas las etapas EXCEPTO el modelo final
      //    ── FIX: aplicar etapas una a una en lugar de new PipelineModel ──
      val etapasPreprocesado = bestPipelineLR.stages.dropRight(1).map(_.asInstanceOf[org.apache.spark.ml.Transformer])

      val dfTrainTransformado = etapasPreprocesado.foldLeft(dfTrainCast) {
        (dfAcc, etapa) => etapa.transform(dfAcc)
      }

      // 3. Extraer nombres de features desde metadata del vector
      val featureField = dfTrainTransformado.schema("features")
      val attrsOpt = org.apache.spark.ml.attribute.AttributeGroup.fromStructField(featureField).attributes

      val featureNames: Array[String] = attrsOpt match {
        case Some(attrs) =>
          attrs.zipWithIndex.map { case (att, idx) =>
            att.name.getOrElse(s"feature_$idx")
          }
        case None =>
          modeloLinealFinal.coefficients.toArray.indices
            .map(i => s"feature_$i").toArray
      }

      // 4. Extraer coeficientes
      val coeficientes = modeloLinealFinal.coefficients.toArray
      val intercepto   = modeloLinealFinal.intercept

      // 5. Emparejar nombres y coeficientes
      val n = math.min(featureNames.length, coeficientes.length)
      val coefDF = featureNames.take(n).zip(coeficientes.take(n)).toSeq
        .toDF("feature", "coef")
        .withColumn("abs_coef", abs(col("coef")))
        .orderBy(desc("abs_coef"))

      // 6. Estadísticas globales
      val eps        = 1e-12
      val nTotal     = coeficientes.length
      val nCeros     = coeficientes.count(c => math.abs(c) < eps)
      val nNoCeros   = nTotal - nCeros
      val pctCeros   = if (nTotal > 0) nCeros   * 100.0 / nTotal else 0.0
      val pctNoCeros = if (nTotal > 0) nNoCeros * 100.0 / nTotal else 0.0

      println("\n  ANÁLISIS DE COEFICIENTES — REGRESIÓN LINEAL")
      println(f"  Intercepto           : $intercepto%.6f")
      println(s"  Total coeficientes   : $nTotal")
      println(s"  Coeficientes en cero : $nCeros")
      println(f"  Porcentaje anulados  : $pctCeros%.2f%%")
      println(s"  Coeficientes no nulos: $nNoCeros")
      println(f"  Porcentaje no nulos  : $pctNoCeros%.2f%%")

      println(s"\n  Top $topN coeficientes por valor absoluto:")
      coefDF.select("feature", "coef", "abs_coef").show(topN, truncate = false)

      println(s"\n  Primeros $topN coeficientes anulados (Lasso):")
      coefDF.filter(abs(col("coef")) < eps).select("feature", "coef").show(topN, truncate = false)

      // 7. Resumen por grupo de variables
      val resumenPorGrupo = coefDF.withColumn("grupo",
          when(col("feature").contains("_ohe_"),
              regexp_extract(col("feature"), "^(.*)_ohe_", 1)).otherwise(
              regexp_extract(col("feature"), "^([^_]+(?:_[^_]+)*)", 1))
        ).withColumn("es_cero", abs(col("coef")) < eps).groupBy("grupo").agg(
          count(lit(1)).alias("n_coef"),
          sum(when(col("es_cero"),  1).otherwise(0)).alias("n_ceros"),
          sum(when(!col("es_cero"), 1).otherwise(0)).alias("n_no_ceros"),
          max(col("abs_coef")).alias("max_abs_coef")
        ).withColumn("pct_ceros", col("n_ceros") * 100.0 / col("n_coef")).orderBy(desc("max_abs_coef"))

      println("\n  Resumen por grupo de variables:")
      resumenPorGrupo.show(50, truncate = false)

      // 8. Guardar si se especifica ruta
      outputPathOpt.foreach { basePath =>
        coefDF.coalesce(1).write.mode("overwrite").option("header", "true").csv(basePath + "_coeficientes")

        resumenPorGrupo.coalesce(1).write.mode("overwrite").option("header", "true").csv(basePath + "_coeficientes_resumen")

        println(s"  💾 Coeficientes guardados en: ${basePath}_coeficientes")
        println(s"  💾 Resumen guardado en      : ${basePath}_coeficientes_resumen")
      }
  }
}