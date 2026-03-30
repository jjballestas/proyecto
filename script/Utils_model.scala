:load Utils.scala

object Utils_models {

    // ══════════════════════════════════════════════════════════════
    // FUNCIONES TVS — añadir dentro del object Utils en Utils.scala
    // (justo antes del último cierre de llave del objeto)
    // ══════════════════════════════════════════════════════════════

    // ── Mostrar tabla de resultados TVS por consola ───────────────
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

    // ── Guardar resultados TVS a CSV ──────────────────────────────
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

    // ── Ejecutar búsqueda TVS completa (wrapper principal) ────────
    def ejecutarBusquedaTVS(spark: SparkSession,nombre: String,pipeline: org.apache.spark.ml.Pipeline,
    paramGrid: Array[org.apache.spark.ml.param.ParamMap],dfTrain: DataFrame,
    evaluador: org.apache.spark.ml.evaluation.RegressionEvaluator,trainRatio: Double = 0.8,
    seed: Long = 42L,resultadosPath: String): org.apache.spark.ml.tuning.TrainValidationSplitModel = {

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

 
    def calcularGrid(df: DataFrame,tipoModelo: String,lr:org.apache.spark.ml.regression.LinearRegression= null,
    gbt: org.apache.spark.ml.regression.GBTRegressor = null,
    rf: org.apache.spark.ml.regression.RandomForestRegressor= null,ronda: Int = 1,mejorRegParam:  Double = 0.001,
    mejorElastic:   Double = 1.0,mejorDepth:  Int  = 5,mejorStepSize:  Double = 0.1,
    mejorNumTrees:  Int    = 100,mejorRFDepth:   Int    = 8
    ): Array[org.apache.spark.ml.param.ParamMap] = {

      val n = df.count()
 
      val esGrande  = n >= 500000
      val esMediano = n >= 50000 && n < 500000
      val esPequeño = n < 50000

      println(s"\n  📐 Dataset: $n registros → " +
        s"${if (esGrande) "GRANDE" else if (esMediano) "MEDIANO" else "PEQUEÑO"}")
      println(s"  📐 Modelo : ${tipoModelo.toUpperCase}  |  Ronda : $ronda")

      val builder = new org.apache.spark.ml.tuning.ParamGridBuilder()

      tipoModelo.toLowerCase match {

        // ── REGRESIÓN LINEAL ───────────────────────────────────────
        case "lr" =>
          require(lr != null, "Se Debe pasar el objeto 'lr' para tipoModelo='lr'")

          if (ronda == 1) {
      
            val regParams = {
            if      (esGrande)  Array(0.0001, 0.0005, 0.001, 0.005, 0.01)
            else if (esMediano) Array(0.0002, 0.001, 0.01, 0.05, 0.1)
            else                Array(0.001, 0.01, 0.1)
            }

            val elastics = {
            if      (esGrande)  Array(0.7, 0.9, 1.0)
            else if (esMediano) Array(0.5, 0.7, 1.0)
            else                Array(0.0, 0.5, 1.0)
            }

            println(s"  📐 LR R1 → regParam: ${regParams.mkString(",")}  elastic: ${elastics.mkString(",")}")
            builder.addGrid(lr.regParam, regParams).addGrid(lr.elasticNetParam, elastics).build()

          } else {
            // Ronda 2: refinamiento alrededor del mejor de R1
                val elasticGrid: Array[Double] = {
                if (mejorElastic == 0.0) {
                  Array(0.0, 0.1, 0.2)
                } else if (mejorElastic == 1.0) {
                  Array(0.7, 0.9, 1.0)
                } else {
                  Array(
                    math.max(0.0, mejorElastic - 0.15),
                    mejorElastic,
                    math.min(1.0, mejorElastic + 0.15)
                  )
                }
              }

              val step = if (esGrande) 3.0 else if (esMediano) 4.0 else 5.0

              val regGrid = Array(math.max(0.00001, mejorRegParam / step),math.max(0.00001, mejorRegParam / 2),
              mejorRegParam,math.min(1.0, mejorRegParam * 2),math.min(1.0, mejorRegParam * step)).distinct.sorted

              println(s"  📐 LR R2 → regParam: ${regGrid.mkString(",")}  elastic: ${elasticGrid.mkString(",")}")

              builder.addGrid(lr.regParam, regGrid).addGrid(lr.elasticNetParam, elasticGrid).build()
          }

        // ── GRADIENT BOOSTED TREES ────────────────────────────────
        case "gbt" =>
          require(gbt != null, "Debes pasar el objeto 'gbt' para tipoModelo='gbt'")

          if (ronda == 1) {
            val depths = if (esGrande) Array(4, 5, 6, 7) else if (esMediano) Array(3, 5, 7) else Array(2, 3, 5)

            val steps    = if (esGrande) Array(0.05, 0.1) else if (esMediano) Array(0.05, 0.1, 0.2) else Array(0.05, 0.1, 0.2)

            val iters    = if (esGrande) Array(100, 150) else if (esMediano) Array(50, 100) else Array(50, 100)

            println(s"  📐 GBT R1 → depth: ${depths.mkString(",")}  step: ${steps.mkString(",")}  iter: ${iters.mkString(",")}")
            builder.addGrid(gbt.maxDepth, depths).addGrid(gbt.stepSize, steps).addGrid(gbt.maxIter,  iters).build()

          } else {
            // Ronda 2: zoom alrededor del mejor
            val depthGrid = Array(math.max(2, mejorDepth - 1),mejorDepth,math.min(if (esGrande) 9 else 8, mejorDepth + 1)).distinct.sorted

            val stepGrid = Array(math.max(0.01, mejorStepSize / 2),mejorStepSize,math.min(0.5,  mejorStepSize * 1.5)).distinct.sorted

            val iterGrid = if (esGrande) Array(100, 150, 200) else Array(50, 100) else   Array(50, 100)

            println(s"  📐 GBT R2 → depth: ${depthGrid.mkString(",")}  step: ${stepGrid.mkString(",")}  iter: ${iterGrid.mkString(",")}")
            builder.addGrid(gbt.maxDepth, depthGrid).addGrid(gbt.stepSize, stepGrid).addGrid(gbt.maxIter,  iterGrid).build()
          }

        // ── RANDOM FOREST ─────────────────────────────────────────
        case "rf" =>
          require(rf != null, "Debes pasar el objeto 'rf' para tipoModelo='rf'")

          if (ronda == 1) {
            val trees    = if (esGrande)       Array(100, 150, 200) else if (esMediano) Array(50, 100, 150)    else                Array(50, 100)

            val depths   = if (esGrande)       Array(8, 10, 12)  else if (esMediano) Array(6, 8, 10)  else                Array(5, 8, 10)

            println(s"  📐 RF R1 → trees: ${trees.mkString(",")}  depth: ${depths.mkString(",")}")
            builder.addGrid(rf.numTrees, trees).addGrid(rf.maxDepth, depths).build()

          } else {
            val treesGrid = Array(math.max(20,  mejorNumTrees - 50),mejorNumTrees, math.min(300, mejorNumTrees + 50)).distinct.sorted

            val depthGrid = Array(math.max(3,  mejorRFDepth - 1),mejorRFDepth,math.min(if (esGrande) 15 else 12, mejorRFDepth + 1)).distinct.sorted

            val minInstGrid = if (esGrande) Array(1, 3, 5) else          Array(1, 5)

            println(s"  📐 RF R2 → trees: ${treesGrid.mkString(",")}  depth: ${depthGrid.mkString(",")}  minInst: ${minInstGrid.mkString(",")}")
            builder.addGrid(rf.numTrees, treesGrid).addGrid(rf.maxDepth, depthGrid)
            .addGrid(rf.minInstancesPerNode, minInstGrid).build()
          }

        case otro =>
          throw new IllegalArgumentException(
            s"tipoModelo '$otro' no reconocido. Usa: 'lr', 'gbt' o 'rf'"
          )
      }
    }




}