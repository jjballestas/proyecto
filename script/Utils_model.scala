:load Utils.scala

object Utils_models {

    // ══════════════════════════════════════════════════════════════
    // FUNCIONES TVS — añadir dentro del object Utils en Utils.scala
    // (justo antes del último cierre de llave del objeto)
    // ══════════════════════════════════════════════════════════════

    // ── Mostrar tabla de resultados TVS por consola ───────────────
    def mostrarResultadosTVS(
        nombre: String,
        tvsModel: org.apache.spark.ml.tuning.TrainValidationSplitModel,
        paramGrid: Array[org.apache.spark.ml.param.ParamMap]
    ): Unit = {

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

    // ── Guardar resultados TVS a CSV ──────────────────────────────
    def guardarResultadosTVS(
        spark: SparkSession,
        nombre: String,
        tvsModel: org.apache.spark.ml.tuning.TrainValidationSplitModel,
        paramGrid: Array[org.apache.spark.ml.param.ParamMap],
        path: String
    ): Unit = {
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

    // ── Ejecutar búsqueda TVS completa (wrapper principal) ────────
    def ejecutarBusquedaTVS(
        spark: SparkSession,
        nombre: String,
        pipeline: org.apache.spark.ml.Pipeline,
        paramGrid: Array[org.apache.spark.ml.param.ParamMap],
        dfTrain: DataFrame,
        evaluador: org.apache.spark.ml.evaluation.RegressionEvaluator,
        trainRatio: Double = 0.8,
        seed: Long = 42L,
        resultadosPath: String
    ): org.apache.spark.ml.tuning.TrainValidationSplitModel = {

      val pct = "%.0f".format(trainRatio * 100)
      val pctVal = "%.0f".format((1 - trainRatio) * 100)

      val tvs = new org.apache.spark.ml.tuning.TrainValidationSplit()
        .setEstimator(pipeline)
        .setEvaluator(evaluador.setMetricName("rmse"))
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(trainRatio)
        .setSeed(seed)

      println(s"\n  🔄 $nombre — TVS ${pct}/${pctVal} — ${paramGrid.length} combinaciones...")
      val tvsModel = tvs.fit(dfTrain)
      println(s"  ✅ $nombre completada")

      mostrarResultadosTVS(nombre, tvsModel, paramGrid)
      guardarResultadosTVS(spark, nombre, tvsModel, paramGrid, resultadosPath)

      tvsModel
    }
        
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

}