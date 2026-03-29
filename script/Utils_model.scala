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

}