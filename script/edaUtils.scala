
 
     
        

        def mostrarNulosPorColumna(df: DataFrame): Unit = {

          val countExpr = count("*").alias("__total__")
          val nullExprs = df.dtypes.map { case (colName, dataType) =>
            val nullCondition = dataType match {
              case "DoubleType" | "FloatType" => col(colName).isNull || col(colName).isNaN
              case "StringType"               => col(colName).isNull || trim(col(colName)) === ""
              case _                          => col(colName).isNull
            }
            sum(when(nullCondition, 1).otherwise(0)).alias(colName)
          }
          val resultRow  = df.select(countExpr +: nullExprs: _*).head()
          val totalRows  = resultRow.getLong(0)

          println("  Valores nulos por columna:")
          println(f"${"Columna"}%-25s ${"Nulos"}%12s ${"% Nulos"}%12s")
          println("-" * 52)

          df.columns.zipWithIndex.foreach { case (colName, idx) =>
            val nulls      = resultRow.getLong(idx + 1)   // +1: saltamos __total__
            val porcentaje = if (totalRows > 0) nulls.toDouble / totalRows * 100.0 else 0.0
            println(f"$colName%-25s $nulls%12d $porcentaje%11.2f%%")
          }

          println()
        }

        
        //esta función realiza un análisis exploratorio de datos (EDA) específico para la variable objetivo "price",
        
        //esta función analiza la correlación entre variables numéricas y la variable objetivo "price", 
        //calcula el coeficiente de correlación de Pearson para cada variable numérica en relación con "price".
        def analyzeCorrelationWithPrice(df: DataFrame,targetCol: String = "price",excludeCols: Seq[String] = Seq() ): Unit = {

          
          val numCols = df.dtypes.filter { case (name, dtype) =>
              dtype == "DoubleType"  ||
              dtype == "FloatType"   ||
              dtype == "IntegerType" ||
              dtype == "LongType"
            }.map(_._1).filterNot(c => c == targetCol || excludeCols.contains(c))
            val corrExprs = numCols.map { c =>
            corr(col(c), col(targetCol)).alias(c)
          }

          val corrRow = df.filter(col(targetCol).isNotNull).select(corrExprs: _*).head()
          
          val results = numCols.map { c =>
            val idx   = corrRow.fieldIndex(c)
            val value = if (corrRow.isNullAt(idx)) Double.NaN else corrRow.getDouble(idx)
            (c, value)
          }.sortBy { case (_, v) => -math.abs(v) }

          // Imprimir tabla
          val sep = "+" + "-" * 32 + "+" + "-" * 12 + "+" + "-" * 18 + "+"
          println(s"\n  Correlación de variables numéricas con '$targetCol':")
          println(sep)
          println(f"| ${"Variable"}%-30s | ${"Corr"}%10s | ${"Interpretación"}%-16s |")
          println(sep)

          results.foreach { case (colName, v) =>
            val bar = {
              val filled = math.round(math.abs(v) * 10).toInt
              val sign   = if (v >= 0) "▲" else "▼"
              sign + "█" * filled + "░" * (10 - filled)
            }
            val label = math.abs(v) match {
              case x if x.isNaN  => "sin datos"
              case x if x >= 0.6 => "fuerte"
              case x if x >= 0.3 => "moderada"
              case x if x >= 0.1 => "débil"
              case _             => "nula"
            }
            println(f"| $colName%-30s | ${v}%+10.4f | $bar $label%-6s |")
          }

          println(sep)
          println("  ▲ positiva  ▼ negativa  █ intensidad  (ordenado por |corr| desc)\n")
        }
        
    
        //esta función filtra un DataFrame para obtener solo filas donde la columna numérica especificada no es nula,
        // y opcionalmente solo valores positivos.
        def getValidNumericDF(df: DataFrame,numericCol: String,positiveOnly: Boolean = false): DataFrame = {
          val base = df.filter(col(numericCol).isNotNull)

          if (positiveOnly) base.filter(col(numericCol) > 0)
          else base
        }

      
        //esta función calcula las estadísticas de una variable numérica específica, 
        //como conteo, mínimo, máximo, media, mediana, desviación estándar, asimetría y curtosis.
        def analyzeNumericGlobalStats(df: DataFrame,numericCol: String,positiveOnly: Boolean = false  ): DataFrame = {
          val dfNum = getValidNumericDF(df, numericCol, positiveOnly)

          dfNum.select(
            count("*").alias("n"),
            min(numericCol).alias("min_value"),
            max(numericCol).alias("max_value"),
            avg(numericCol).alias("mean_value"),
            expr(s"percentile_approx($numericCol, 0.5)").alias("median_value"),
            stddev(numericCol).alias("std_value"),
            skewness(numericCol).alias("skew_value"),
            kurtosis(numericCol).alias("kurt_value")
          )
        }

        //esta función calcula los percentiles de una variable numérica específica,
        // permitiendo filtrar solo valores positivos y especificar los percentiles deseados.
        def analyzeNumericPercentiles(df: DataFrame,numericCol: String,positiveOnly: Boolean = false,
          probs: Seq[Double] = Seq(0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.999)): DataFrame = {
          val spark = df.sparkSession   
          val dfNum = getValidNumericDF(df, numericCol, positiveOnly)
          val percentileValues = dfNum.stat.approxQuantile(numericCol, probs.toArray, 0.001)

          probs.zip(percentileValues).map { case (p, v) => (p, v) }.toDF("percentile", s"${numericCol}_value")
        }





        //esta función compara la distribución de una variable numérica con su transformación logarítmica,
        // calculando y mostrando los valores de asimetría (skewness) y curtosis (kurtosis) para ambas versiones de la variable.
          def analyzeNumericVsLog(df: DataFrame,numericCol: String,positiveOnly: Boolean = true
          ): DataFrame = {
            val dfNum = getValidNumericDF(df, numericCol, positiveOnly)
              .withColumn("log_value_tmp", log1p(col(numericCol)))

            dfNum.select(
              skewness(numericCol).alias("skew_value"),
              kurtosis(numericCol).alias("kurt_value"),
              skewness("log_value_tmp").alias("skew_log_value"),
              kurtosis("log_value_tmp").alias("kurt_log_value")
            )
          }

        //esta función obtiene los registros con los valores más altos de una variable numérica ,
        // filtrando solo valores positivos, y permite incluir columnas adicionales para contexto.
          def getTopExtremeValues(df: DataFrame,numericCol: String,positiveOnly: Boolean = false,topN: Int = 30,extraCols: Seq[String] = Seq()
          ): DataFrame = {
            val dfNum = getValidNumericDF(df, numericCol, positiveOnly)
            val selectedCols = (Seq(numericCol) ++ extraCols.filter(df.columns.contains)).distinct

            dfNum.select(selectedCols.map(col): _*).orderBy(desc(numericCol)).limit(topN)
          }


        //esta función agrega columnas al DataFrame con los límites de outliers basados en el rango intercuartílico (IQR) 
        //para una variable numérica específica, utilizando los cuartiles Q1 y Q3.
        //la utilizo para refactorizar el código y evitar repetir la lógica de cálculo de límites IQR 
        //(getGlobalIQROutlierBounds y getNumericSegmentStats)
        def addIQRBounds(df: DataFrame, q1Col: String = "q1", q3Col: String = "q3"): DataFrame = {
          df.withColumn("iqr", col(q3Col) - col(q1Col))
            .withColumn("lower_bound_iqr_1_5", col(q1Col) - lit(1.5) * col("iqr"))
            .withColumn("upper_bound_iqr_1_5", col(q3Col) + lit(1.5) * col("iqr"))
            .withColumn("lower_bound_iqr_3_0", col(q1Col) - lit(3.0) * col("iqr"))
            .withColumn("upper_bound_iqr_3_0", col(q3Col) + lit(3.0) * col("iqr"))
        }

        //esta función calcula los límites de outliers basados en el rango intercuartílico (IQR) 
        //para una variable numérica específica a nivel global,
        def getGlobalIQROutlierBounds(df: DataFrame,numericCol: String,positiveOnly: Boolean = false): DataFrame = {
          val dfNum = getValidNumericDF(df, numericCol, positiveOnly)

          val stats = dfNum.selectExpr(
            s"percentile_approx($numericCol, 0.25) as q1",
            s"percentile_approx($numericCol, 0.50) as median",
            s"percentile_approx($numericCol, 0.75) as q3"
          )

          addIQRBounds(stats)
        }

        //esta función calcula estadísticas  de una variable numérica específica,segmentada por una o más columnas categóricas
        //  y agrega límites de outliers basados en el rango intercuartílico (IQR)  para cada segmento
        // permitiendo filtrar solo valores positivos y establecer un tamaño mínimo de grupo 
        //para considerar las estadísticas válidas.


        def getNumericSegmentStats(df: DataFrame, numericCol: String, segmentCols: Seq[String], 
                                  positiveOnly: Boolean = false, minGroupSize: Int = 30): DataFrame = {
          
          val dfNum = getValidNumericDF(df, numericCol, positiveOnly)
          val validSegmentCols = segmentCols.filter(dfNum.columns.contains)

          val stats = dfNum.groupBy(validSegmentCols.map(col): _*).agg(
              count("*").alias("group_n"),
              min(numericCol).alias("min_value"),
              expr(s"percentile_approx($numericCol, 0.25)").alias("q1"),
              expr(s"percentile_approx($numericCol, 0.5)").alias("median_value"),
              expr(s"percentile_approx($numericCol, 0.75)").alias("q3"),
              expr(s"percentile_approx($numericCol, 0.95)").alias("p95"),
              expr(s"percentile_approx($numericCol, 0.99)").alias("p99"),
              avg(numericCol).alias("mean_value"),
              max(numericCol).alias("max_value"),
              // 1. Agregamos Desviación Estándar para entender la dispersión real
              stddev(numericCol).alias("stddev_value"),
              skewness(numericCol).alias("skew_value")
            )
            .filter(col("group_n") >= minGroupSize).withColumn("cv_percentage", (col("stddev_value") / col("mean_value")) * 100)

          val statsWithBounds = addIQRBounds(stats)
            statsWithBounds.withColumn("lower_bound_iqr_1_5", when(col("lower_bound_iqr_1_5") < 0, 0).otherwise(col("lower_bound_iqr_1_5")))
        }


        //esta función identifica outliers sospechosos en una variable numérica específica,
        // segmentada por una o más columnas categóricas,  utilizando límites basados en el rango intercuartílico (IQR) 
        //y permitiendo filtrar solo valores positivos y establecer un valor mínimo absoluto para considerar un outlier como sospechoso.
          def detectSuspiciousNumericOutliersBySegment(df: DataFrame,numericCol: String,segmentCols: Seq[String],
          positiveOnly: Boolean = false,minGroupSize: Int = 30,iqrMultiplier: Double = 3.0,
          minAbsoluteValue: Double = Double.MinValue,extraCols: Seq[String] = Seq()): DataFrame = {

          val dfNum = getValidNumericDF(df, numericCol, positiveOnly)
          val validSegmentCols = segmentCols.filter(dfNum.columns.contains)
          val selectedExtraCols = extraCols.filter(dfNum.columns.contains).distinct

          val baseStats = dfNum.groupBy(validSegmentCols.map(col): _*).agg(
              count("*").alias("group_n"),
              expr(s"percentile_approx($numericCol, 0.25)").alias("q1"),
              expr(s"percentile_approx($numericCol, 0.5)").alias("segment_median_value"),
              expr(s"percentile_approx($numericCol, 0.75)").alias("q3"),
              expr(s"percentile_approx($numericCol, 0.95)").alias("segment_p95"),
              expr(s"percentile_approx($numericCol, 0.99)").alias("segment_p99")
            ).filter(col("group_n") >= minGroupSize)

          val statsWithBounds = addIQRBounds(baseStats, "q1", "q3")
            .withColumn("segment_upper_bound", col("q3") + lit(iqrMultiplier) * col("iqr"))
            .withColumn("segment_lower_bound", col("q1") - lit(iqrMultiplier) * col("iqr"))

          dfNum.join(statsWithBounds, validSegmentCols, "inner").withColumn(
              s"${numericCol}_to_median_ratio",
              when(col("segment_median_value").isNotNull && col("segment_median_value") =!= 0,
                col(numericCol) / col("segment_median_value")
              ).otherwise(lit(null).cast("double"))
            ).withColumn(
              s"${numericCol}_to_p95_ratio",
              when(col("segment_p95").isNotNull && col("segment_p95") =!= 0,
                col(numericCol) / col("segment_p95")
              ).otherwise(lit(null).cast("double"))
            ).withColumn(
              s"${numericCol}_to_p99_ratio",
              when(col("segment_p99").isNotNull && col("segment_p99") =!= 0,
                col(numericCol) / col("segment_p99")
              ).otherwise(lit(null).cast("double"))
            ).filter(
              (col(numericCol) > col("segment_upper_bound") || col(numericCol) < col("segment_lower_bound")) &&
              col(numericCol) >= lit(minAbsoluteValue)
            ).select(
              validSegmentCols.map(col) ++
              Seq(col("group_n"),col(numericCol),col("q1"),col("segment_median_value"),col("q3"),col("segment_p95"),
              col("segment_p99"),col("iqr"),col("segment_lower_bound"),col("segment_upper_bound"),
              col(s"${numericCol}_to_median_ratio"),col(s"${numericCol}_to_p95_ratio"),col(s"${numericCol}_to_p99_ratio")
              ) ++
              selectedExtraCols.map(col): _*
            ).orderBy(desc(numericCol))
        }

          // ---------------------------------------------------------
          // Resumen de outliers sospechosos por segmento
          // ---------------------------------------------------------
          def summarizeSuspiciousNumericOutliers(suspiciousDF: DataFrame,numericCol: String,segmentCols: Seq[String]): DataFrame = {
            suspiciousDF.groupBy(segmentCols.map(col): _*).agg(
                count("*").alias("n_suspicious"),
                min(numericCol).alias("min_suspicious_value"),
                expr(s"percentile_approx($numericCol, 0.5)").alias("median_suspicious_value"),
                max(numericCol).alias("max_suspicious_value")
              ).orderBy(desc("n_suspicious"), desc("max_suspicious_value"))
          }

          // ---------------------------------------------------------
          // Mostrar DataFrame con título
          // ---------------------------------------------------------
          def showDF(title: String, df: DataFrame, n: Int = 50): Unit = {
            println(s"\n=== $title ===")
            df.show(n, truncate = false)
          }

          

          def getValidPriceDF(df: DataFrame, priceCol: String = "price"): DataFrame = {
            getValidNumericDF(df, priceCol, positiveOnly = true)
          }

          def analyzePriceGlobalStats(df: DataFrame, priceCol: String = "price"): DataFrame = {
            analyzeNumericGlobalStats(df, priceCol, positiveOnly = true)
              .withColumnRenamed("min_value", "min_price")
              .withColumnRenamed("max_value", "max_price")
              .withColumnRenamed("mean_value", "mean_price")
              .withColumnRenamed("median_value", "median_price")
              .withColumnRenamed("std_value", "std_price")
              .withColumnRenamed("skew_value", "skew_price")
              .withColumnRenamed("kurt_value", "kurt_price")
          }
        
          def analyzePricePercentiles( df: DataFrame,priceCol: String = "price",
          probs: Seq[Double] = Seq(0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.999)): DataFrame = {
            analyzeNumericPercentiles(df, priceCol, positiveOnly = true, probs = probs)
          }

        


          def analyzePriceVsLogPrice(df: DataFrame, priceCol: String = "price"): DataFrame = {
            analyzeNumericVsLog(df, priceCol, positiveOnly = true)
              .withColumnRenamed("skew_value", "skew_price")
              .withColumnRenamed("kurt_value", "kurt_price")
              .withColumnRenamed("skew_log_value", "skew_log_price")
              .withColumnRenamed("kurt_log_value", "kurt_log_price")
          }

          def analyzePriceByCategory(df: DataFrame,categoryCol: String,priceCol: String = "price",topN: Int = 50,minCount: Int = 1
          ): DataFrame = {
            val dfPrice = getValidPriceDF(df, priceCol)

            dfPrice.groupBy(categoryCol).agg(count("*").alias("n"),
                min(priceCol).alias("min_price"),
                expr(s"percentile_approx($priceCol, 0.25)").alias("p25"),
                expr(s"percentile_approx($priceCol, 0.5)").alias("median_price"),
                expr(s"percentile_approx($priceCol, 0.75)").alias("p75"),
                expr(s"percentile_approx($priceCol, 0.95)").alias("p95"),
                max(priceCol).alias("max_price"),
                avg(priceCol).alias("mean_price"),
                skewness(priceCol).alias("skew_price")
              ).filter(col("n") >= minCount).orderBy(desc("median_price")).limit(topN)
          }

          def analyzePriceByTopCategories(
              df: DataFrame,categoryCol: String,priceCol: String = "price",topCategories: Int = 20
          ): DataFrame = {
            val dfPrice = getValidPriceDF(df, priceCol)

            val topCats = dfPrice.groupBy(categoryCol).count().orderBy(desc("count")).limit(topCategories).select(categoryCol)

            dfPrice.join(topCats, Seq(categoryCol)).groupBy(categoryCol).agg(
                count("*").alias("n"),
                min(priceCol).alias("min_price"),
                expr(s"percentile_approx($priceCol, 0.25)").alias("p25"),
                expr(s"percentile_approx($priceCol, 0.5)").alias("median_price"),
                expr(s"percentile_approx($priceCol, 0.75)").alias("p75"),
                expr(s"percentile_approx($priceCol, 0.95)").alias("p95"),
                max(priceCol).alias("max_price"),
                avg(priceCol).alias("mean_price")
              ).orderBy(desc("median_price"))
          }

          def analyzePriceByYear(df: DataFrame, priceCol: String = "price"): DataFrame = {
            val dfPrice = getValidPriceDF(df, priceCol)

            dfPrice.groupBy("year").agg(
                count("*").alias("n"),
                expr(s"percentile_approx($priceCol, 0.5)").alias("median_price"),
                expr(s"percentile_approx($priceCol, 0.95)").alias("p95"),
                max(priceCol).alias("max_price"),
                avg(priceCol).alias("mean_price"),
                skewness(priceCol).alias("skew_price")
              ).orderBy(desc("year"))
          }

          def getTopExpensiveVehicles(df: DataFrame,priceCol: String = "price",topN: Int = 30): DataFrame = {
            getTopExtremeValues(
              df = df,
              numericCol = priceCol,
              positiveOnly = true,
              topN = topN,
              extraCols = Seq("make_name", "model_name", "trim_name", "year", "body_type", "fuel_type", "is_new", "mileage", "horsepower")
            )
          }

          def getTopKPriceByCategory( df: DataFrame,categoryCol: String,priceCol: String = "price",topCategories: Int = 15,k: Int = 5): DataFrame = {
            val dfPrice = getValidPriceDF(df, priceCol)

            val topCats = dfPrice.groupBy(categoryCol).count().orderBy(desc("count")).limit(topCategories).select(categoryCol)

            val w = org.apache.spark.sql.expressions.Window.partitionBy(categoryCol).orderBy(desc(priceCol))

            dfPrice.join(topCats, Seq(categoryCol)).withColumn("rn", row_number().over(w)).filter(col("rn") <= k).select(
                col(categoryCol),col("model_name"),col("trim_name"),col("year"),col("body_type"),
                col("is_new"),col("mileage"),col("horsepower"),col(priceCol),col("rn")
              ).orderBy(col(categoryCol), desc(priceCol))
          }

          def summarizePriceTransformationDecision(df: DataFrame,priceCol: String = "price"): DataFrame = {
            val stats = analyzeNumericGlobalStats(df, priceCol, positiveOnly = true)
            val vsLog = analyzeNumericVsLog(df, priceCol, positiveOnly = true)

            stats.crossJoin(vsLog.select(
              col("skew_log_value"),
              col("kurt_log_value")
            )).withColumnRenamed("min_value", "min_price").withColumnRenamed("max_value", "max_price").withColumnRenamed("mean_value", "mean_price")
            .withColumnRenamed("median_value", "median_price").withColumnRenamed("std_value", "std_price").withColumnRenamed("skew_value", "skew_price")
            .withColumnRenamed("kurt_value", "kurt_price").withColumnRenamed("skew_log_value", "skew_log_price").withColumnRenamed("kurt_log_value", "kurt_log_price")
          }

          def getGlobalPricePercentiles(df: DataFrame, priceCol: String = "price"): DataFrame = {
            analyzeNumericPercentiles(df,numericCol = priceCol,positiveOnly = true,probs = Seq(0.25, 0.50, 0.75, 0.95, 0.99, 0.995, 0.999))
            .withColumnRenamed("percentiles", "price_percentiles")
          }

        
          def getPriceSegmentStats(df: DataFrame,segmentCols: Seq[String],priceCol: String = "price",minGroupSize: Int = 30): DataFrame = {
            getNumericSegmentStats(
              df = df,
              numericCol = priceCol,
              segmentCols = segmentCols,
              positiveOnly = true,
              minGroupSize = minGroupSize
            ).withColumnRenamed("min_value", "min_price").withColumnRenamed("median_value", "median_price")
            .withColumnRenamed("mean_value", "mean_price").withColumnRenamed("max_value", "max_price").withColumnRenamed("skew_value", "skew_price")
          }

          def detectSuspiciousPriceOutliersBySegment(df: DataFrame,segmentCols: Seq[String],priceCol: String = "price",
          minGroupSize: Int = 30,iqrMultiplier: Double = 3.0,minAbsolutePrice: Double = 100000.0): DataFrame = {
            detectSuspiciousNumericOutliersBySegment(
              df = df,
              numericCol = priceCol,
              segmentCols = segmentCols,
              positiveOnly = true,
              minGroupSize = minGroupSize,
              iqrMultiplier = iqrMultiplier,
              minAbsoluteValue = minAbsolutePrice,
              extraCols = Seq("make_name", "model_name", "trim_name", "year", "body_type", "is_new", "mileage", "horsepower")
            )
            .withColumnRenamed("segment_median_value", "segment_median_price").withColumnRenamed("price_to_median_ratio", "price_to_median_ratio")
            .withColumnRenamed("price_to_p95_ratio", "price_to_p95_ratio").withColumnRenamed("price_to_p99_ratio", "price_to_p99_ratio")
          }

          def detectSuspiciousPriceOutliersHierarchical(df: DataFrame,priceCol: String = "price",minGroupSize: Int = 30,
              iqrMultiplier: Double = 3.0,
              minAbsolutePrice: Double = 100000.0
          ): DataFrame = {
            val segmentCols = Seq("make_name", "body_type", "is_new").filter(df.columns.contains)

            detectSuspiciousPriceOutliersBySegment(
              df = df,
              segmentCols = segmentCols,
              priceCol = priceCol,
              minGroupSize = minGroupSize,
              iqrMultiplier = iqrMultiplier,
              minAbsolutePrice = minAbsolutePrice
            )
          }

          def summarizeSuspiciousPriceOutliers(
              suspiciousDF: DataFrame,
              segmentCols: Seq[String]
          ): DataFrame = {
            summarizeSuspiciousNumericOutliers(suspiciousDF, "price", segmentCols)
              .withColumnRenamed("min_suspicious_value", "min_suspicious_price")
              .withColumnRenamed("median_suspicious_value", "median_suspicious_price")
              .withColumnRenamed("max_suspicious_value", "max_suspicious_price")
          }

          def topSuspiciousPriceOutliersByMake(
              suspiciousDF: DataFrame,
              topN: Int = 100
          ): DataFrame = {
            suspiciousDF.orderBy(desc("price")).limit(topN)
          }
            
            


          // ---------------------------------------------------------
          // Analizar contenido de columnas string antes de procesarlas
          // ---------------------------------------------------------
          def analyzeStringColumnsContent(df: DataFrame,colsToAnalyze: Seq[String],sampleTopN: Int = 20          ): Unit = {

            val validCols = colsToAnalyze.filter(df.columns.contains)

            validCols.foreach { c =>
              println(s"\n==================== Análisis de: $c ====================")

              val base = df.select(col(c))

              val summary = base.agg(
                count("*").alias("n_total"),
                sum(when(col(c).isNull, 1).otherwise(0)).alias("n_null"),
                sum(when(trim(col(c)) === "", 1).otherwise(0)).alias("n_empty"),
                sum(when(trim(lower(col(c))).isin("null", "none", "--", "n/a", "na"), 1).otherwise(0)).alias("n_placeholder"),
                countDistinct(col(c)).alias("n_distinct"),
                sum(
                  when(
                    regexp_extract(col(c), "([0-9]+\\.?[0-9]*)", 1) =!= "",
                    1
                  ).otherwise(0)
                ).alias("n_with_number")
              )

              println("---- Resumen ----")
              summary.show(false)

              println("---- Top valores más frecuentes ----")
              base.groupBy(col(c)).count().orderBy(desc("count")).show(sampleTopN, truncate = false)

              println("---- Ejemplos con número extraíble ----")
              base.filter(regexp_extract(col(c), "([0-9]+\\.?[0-9]*)", 1) =!= "").groupBy(col(c)).count()
              .orderBy(desc("count")).show(sampleTopN, truncate = false)

              println("---- Ejemplos sin número extraíble ----")
              base.filter(
                col(c).isNotNull &&
                trim(col(c)) =!= "" &&
                !trim(lower(col(c))).isin("null", "none", "--", "n/a", "na") &&
                regexp_extract(col(c), "([0-9]+\\.?[0-9]*)", 1) === ""
              ).groupBy(col(c)).count().orderBy(desc("count")).show(sampleTopN, truncate = false)
            }
          }
 
          def edaAddGeoRegion(   df: DataFrame,    latitudeCol: String = "latitude",    longitudeCol: String = "longitude",    regionCol: String = "geo_region"): DataFrame = {
            df.withColumn(
              regionCol,
              when(col(latitudeCol).isNull || col(longitudeCol).isNull, "unknown")
                .when(col(longitudeCol) < -100 && col(latitudeCol) >= 37, "west_north")
                .when(col(longitudeCol) < -100 && col(latitudeCol) < 37, "west_south")
                .when(col(longitudeCol) >= -100 && col(longitudeCol) < -85 && col(latitudeCol) >= 37, "central_north")
                .when(col(longitudeCol) >= -100 && col(longitudeCol) < -85 && col(latitudeCol) < 37, "central_south")
                .when(col(longitudeCol) >= -85 && col(latitudeCol) >= 37, "east_north")
                .otherwise("east_south")
            )
          }



          def analyzePriceByGeoRegion(    df: DataFrame,    latitudeCol: String = "latitude",    
          longitudeCol: String = "longitude",    regionCol: String = "geo_region"): DataFrame = {
            val dfRegion = edaAddGeoRegion(df, latitudeCol, longitudeCol, regionCol)
            val dfPrice = getValidNumericDF(dfRegion, "price", positiveOnly = true)

            dfPrice
              .groupBy(regionCol)
              .agg(
                count("*").alias("n"),
                min("price").alias("min_price"),
                expr("percentile_approx(price, 0.25)").alias("p25"),
                expr("percentile_approx(price, 0.5)").alias("median_price"),
                expr("percentile_approx(price, 0.75)").alias("p75"),
                expr("percentile_approx(price, 0.95)").alias("p95"),
                max("price").alias("max_price"),
                round(avg("price"), 2).alias("mean_price"),
                skewness("price").alias("skew_price")
              )
              .orderBy(desc("median_price"))
          }
  
  
      def analyzePriceByUrbanProximity(df: DataFrame): DataFrame = {
        val dfPrice = getValidNumericDF(df, "price", positiveOnly = true)

        dfPrice.filter(col("urban_level").isNotNull).groupBy("urban_level").agg(
            count("*").alias("n"),
            min("price").alias("min_price"),
            expr("percentile_approx(price, 0.25)").alias("p25"),
            expr("percentile_approx(price, 0.5)").alias("median_price"),
            expr("percentile_approx(price, 0.75)").alias("p75"),
            expr("percentile_approx(price, 0.95)").alias("p95"),
            max("price").alias("max_price"),
            round(avg("price"), 2).alias("mean_price"),
            skewness("price").alias("skew_price")
          ).orderBy(
            when(col("urban_level") === "urban", 1).when(col("urban_level") === "suburban", 2).otherwise(3)
          )
      }







      def resumenNumericoTabular(df: DataFrame): Unit = {
          val nTotal = df.count().toDouble
          if (nTotal == 0) { println("⛔  DataFrame vacío."); return }

          val numericCols = df.schema.fields.filter(_.dataType match {
          case _: IntegerType | _: LongType |
          _: DoubleType  | _: FloatType | _: DecimalType => true
          case _ => false
          }).map(_.name)

          if (numericCols.isEmpty) {
          println("⛔  No hay columnas numéricas en el DataFrame.")
          return
          }

          // ── PASADA ÚNICA ──────────────────────────────────────────────────────────
          val aggExprs = numericCols.flatMap { c =>
          Seq(
          count(col(c)).alias(s"${c}__n"),
          min(col(c)).alias(s"${c}__min"),
          max(col(c)).alias(s"${c}__max"),
          avg(col(c)).alias(s"${c}__mean"),
          expr(s"percentile_approx(`$c`, 0.5)").alias(s"${c}__median"),
          stddev(col(c)).alias(s"${c}__stddev"),
          skewness(col(c)).alias(s"${c}__skew"),
          kurtosis(col(c)).alias(s"${c}__kurt"),
          sum(when(col(c) === 0, 1).otherwise(0)).alias(s"${c}__zeros")
          )
          }

          val statsRow = df.select(aggExprs: _*).collect()(0)

          // ── HELPERS ───────────────────────────────────────────────────────────────
          def getDouble(field: String): Option[Double] = {
          val idx = statsRow.fieldIndex(field)
          if (statsRow.isNullAt(idx)) None
          else Try(statsRow.get(idx).toString.toDouble).toOption
          .filterNot(d => d.isNaN || d.isInfinite)
          }

          def fmt(field: String, decimals: Int = 4): String =
          getDouble(field) match {
          case None    => "null"
          case Some(d) => if (decimals == 0) d.toLong.toString
          else s"%.${decimals}f".format(d)
          }

          // ── LÓGICA DE RECOMENDACIONES ─────────────────────────────────────────────

          def recImputacion(n: Long): String = {
          val pctNull = (nTotal - n) * 100.0 / nTotal
          if      (pctNull == 0.0) "✅ Completa — sin imputación"
          else if (pctNull <  5.0) "Imputar: mediana (nulos marginales)"
          else if (pctNull < 20.0) "Imputar: mediana o modelo (KNN/RF)"
          else if (pctNull < 50.0) "⚠️  Imputar + flag_nulo como feature auxiliar"
          else                      "❌ Cobertura crítica — evaluar descarte"
          }

          def recTransformacion(skew: Option[Double]): String = skew match {
          case None                    => "skew incalculable"
          case Some(s) if s.abs < 0.5 => "✅ Simétrica — sin transformación"
          case Some(s) if s.abs < 1.0 => "Leve asimetría — transformación opcional"
          case Some(s) if s >  1.0    => "Asimetría positiva — probar log1p / sqrt"
          case Some(s) if s < -1.0    => "Asimetría negativa — probar reflejar + log"
          case _                       => "Revisar"
          }

          def recOutliers(kurt: Option[Double]): String = kurt match {
          case None                    => "incalculable"
          case Some(k) if k <  0.0    => "✅ Distribución plana — outliers improbables"
          case Some(k) if k <  3.0    => "✅ Kurtosis normal — outliers moderados"
          case Some(k) if k <  7.0    => "⚠️  Colas pesadas — revisar con IQR/Z-score"
          case Some(k) if k >= 7.0    => "❌ Kurtosis alta — outliers extremos presentes"
          case _                       => "Revisar"
          }

          def recEscalado(minV: Option[Double], maxV: Option[Double], std: Option[Double]): String =
          (minV, maxV, std) match {
          case (Some(mn), Some(mx), Some(s)) =>
          val rango = mx - mn
          if      (rango == 0.0)            "⚠️  Varianza cero — columna constante, descartar"
          else if (rango > 1000 || s > 500) "Escalar: StandardScaler o MinMaxScaler"
          else if (rango > 100)             "Escalar: MinMaxScaler recomendado"
          else                              "✅ Rango acotado — escalado opcional"
          case _ => "incalculable"
          }

          def recCeros(zeros: Long): String = {
          val pct = zeros * 100.0 / nTotal
          if      (pct == 0.0) "✅ Sin ceros"
          else if (pct <  5.0) "Ceros marginales — sin tratamiento especial"
          else if (pct < 20.0) "⚠️  Ceros frecuentes — flag_es_cero como feature auxiliar"
          else if (pct < 50.0) "❌ Inflación de ceros — flag obligatorio"
          else                  "❌ Mayoría ceros — evaluar si aporta señal"
          }

          // ── SEPARADORES ───────────────────────────────────────────────────────────
          val sep    = "═" * 135
          val subSep = "─" * 135

          // ── BLOQUE 1: ESTADÍSTICOS ────────────────────────────────────────────────
          println(s"\n$sep")
          println("  RESUMEN ESTADÍSTICO — VARIABLES NUMÉRICAS")
          println(sep)
          println(f"  ${"feature"}%-25s ${"n (no-nulos)"}%-14s ${"min"}%-12s ${"max"}%-12s ${"media"}%-12s ${"mediana"}%-12s ${"stddev"}%-12s ${"skew"}%-10s ${"kurt"}%-10s")
          println(subSep)

          numericCols.foreach { c =>
          val isIntLike = df.schema(c).dataType match {
          case _: IntegerType | _: LongType => true
          case _ => false
          }
          val dec = if (isIntLike) 0 else 4

          println(f"  $c%-25s ${fmt(s"${c}__n", 0)}%-14s ${fmt(s"${c}__min", dec)}%-12s ${fmt(s"${c}__max", dec)}%-12s ${fmt(s"${c}__mean", 4)}%-12s ${fmt(s"${c}__median", dec)}%-12s ${fmt(s"${c}__stddev", 4)}%-12s ${fmt(s"${c}__skew", 4)}%-10s ${fmt(s"${c}__kurt", 4)}%-10s")
          }
          println(s"$sep\n")

          // ── BLOQUE 2a: IMPUTACIÓN + TRANSFORMACIÓN ────────────────────────────────
          println(s"\n$sep")
          println("  RECOMENDACIONES (1/2) — IMPUTACIÓN Y TRANSFORMACIÓN")
          println(sep)
          println(f"  ${"feature"}%-25s ${"imputación"}%-50s ${"transformación"}")
          println(subSep)

          numericCols.foreach { c =>
          val n    = getDouble(s"${c}__n").map(_.toLong).getOrElse(0L)
          val skew = getDouble(s"${c}__skew")

          val rImput = recImputacion(n)
          val rTrans = recTransformacion(skew)

          println(f"  $c%-25s $rImput%-50s $rTrans")
          }
          println(s"$sep\n")

          // ── BLOQUE 2b: OUTLIERS + ESCALADO + CEROS ────────────────────────────────
          println(s"\n$sep")
          println("  RECOMENDACIONES (2/2) — OUTLIERS, ESCALADO Y CEROS")
          println(sep)
          println(f"  ${"feature"}%-25s ${"outliers"}%-48s ${"escalado"}%-42s ${"ceros"}")
          println(subSep)

          numericCols.foreach { c =>
          val zeros = getDouble(s"${c}__zeros").map(_.toLong).getOrElse(0L)
          val kurt  = getDouble(s"${c}__kurt")
          val minV  = getDouble(s"${c}__min")
          val maxV  = getDouble(s"${c}__max")
          val std   = getDouble(s"${c}__stddev")

          val rOutl  = recOutliers(kurt)
          val rEsc   = recEscalado(minV, maxV, std)
          val rCeros = recCeros(zeros)

          println(f"  $c%-25s $rOutl%-48s $rEsc%-42s $rCeros")
          }

          println(subSep)
          println(s"  Total columnas analizadas: ${numericCols.size}  |  Filas totales: ${nTotal.toLong}")
          println(s"$sep\n")
        }


        def resumenCategoricoTabular(df: DataFrame, cols: Seq[String]): Unit = {

          val nTotal = df.count().toDouble
          if (nTotal == 0) { println("⛔  DataFrame vacío."); return }

          val colsExistentes = cols.filter(df.columns.contains)
          val colsAusentes   = cols.filterNot(df.columns.contains)

          if (colsExistentes.isEmpty) {
          println("⛔  Ninguna de las columnas indicadas existe en el DataFrame.")
          return
          }

          // ── PASADA 1: nulos + cardinalidad en una sola agregación ─────────────────
          val aggExprs = colsExistentes.flatMap { c =>
          Seq(
          sum(when(col(c).isNull, 1).otherwise(0)).alias(s"${c}__nulls"),
          countDistinct(col(c)).alias(s"${c}__card")
          )
          }
          val statsRow = df.select(aggExprs: _*).collect()(0)

          def getLong(field: String): Long = {
          val idx = statsRow.fieldIndex(field)
          if (statsRow.isNullAt(idx)) 0L else statsRow.getAs[Long](field)
          }

          // ── PASADA 2: top categoría por columna (groupBy individual, inevitable) ──
          // Se cachea para que los N groupBy no relean desde disco
          df.cache()

          val topValues: Map[String, (String, Long)] = colsExistentes.map { c =>
          val topRow   = df.groupBy(col(c)).count().orderBy(desc("count")).first()
          val topVal   = if (topRow.isNullAt(0)) "null"
          else Option(topRow.get(0)).map(_.toString).getOrElse("null")
          val topCount = topRow.getAs[Long]("count")   // por nombre, no por índice
          c -> (topVal, topCount)
          }.toMap

          df.unpersist()

          // ── LÓGICA DE RECOMENDACIONES ─────────────────────────────────────────────

          def recImputacion(nullPct: Double): String =
          if      (nullPct == 0.0) "✅ Completa — sin imputación"
          else if (nullPct <  5.0) "Imputar: moda (nulos marginales)"
          else if (nullPct < 20.0) "Imputar: moda + flag_nulo auxiliar"
          else if (nullPct < 50.0) "⚠️  Imputar + flag_nulo como feature"
          else                      "❌ Cobertura crítica — evaluar descarte"

          def recEncoding(card: Long): String =
          if      (card <=    2) "✅ Binaria — Label Encoding (0/1)"
          else if (card <=   10) "One-Hot Encoding directo"
          else if (card <=   50) "One-Hot con cautela / Target Encoding"
          else if (card <=  500) "Target Encoding / Frequency Encoding"
          else if (card <= 5000) "Frequency Encoding + agrupar < 1% en 'Otros'"
          else                    "Hashing / agrupación fuerte por prefijo"

          def recConcentracion(topPct: Double, card: Long): String =
          if      (topPct > 95.0) "❌ Cuasi-constante — descartar"
          else if (topPct > 80.0) "⚠️  Alta concentración — bajo poder predictivo"
          else if (topPct > 50.0) "⚠️  Categoría dominante — revisar distribución"
          else if (card   > 1000) "⚠️  Alta cardinalidad — agrupar categorías raras"
          else                     "✅ Distribución aceptable"

          // ── SEPARADORES ───────────────────────────────────────────────────────────
          val sep    = "═" * 115
          val subSep = "─" * 115

          // ── BLOQUE 1: ESTADÍSTICOS ────────────────────────────────────────────────
          println(s"\n$sep")
          println("  RESUMEN ESTADÍSTICO — VARIABLES CATEGÓRICAS")
          println(sep)
          println(f"  ${"feature"}%-25s ${"cardinalidad"}%-14s ${"nulos"}%-10s ${"null %"}%-12s ${"top categoría"}%-30s ${"top count"}%-12s ${"top %"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val nulls              = getLong(s"${c}__nulls")
          val card               = getLong(s"${c}__card")
          val nullPct            = nulls * 100.0 / nTotal
          val (topVal, topCount) = topValues(c)
          val topPct             = topCount * 100.0 / nTotal

          // Pre-formatear valores con % para evitar conflicto del interpolador f
          val nullFmt   = f"$nullPct%.2f%%"
          val topPctFmt = f"$topPct%.2f%%"

          println(f"  $c%-25s $card%-14d $nulls%-10d $nullFmt%-12s ${topVal.take(28)}%-30s $topCount%-12d $topPctFmt")
          }
          println(s"$sep\n")

          // ── BLOQUE 2a: IMPUTACIÓN + ENCODING ──────────────────────────────────────
          println(s"\n$sep")
          println("  RECOMENDACIONES (1/2) — IMPUTACIÓN Y ENCODING")
          println(sep)
          println(f"  ${"feature"}%-25s ${"imputación"}%-48s ${"encoding sugerido"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val nullPct = getLong(s"${c}__nulls") * 100.0 / nTotal
          val card    = getLong(s"${c}__card")

          val rImput = recImputacion(nullPct)
          val rEnc   = recEncoding(card)

          println(f"  $c%-25s $rImput%-48s $rEnc")
          }
          println(s"$sep\n")

          // ── BLOQUE 2b: CONCENTRACIÓN Y UTILIDAD ───────────────────────────────────
          println(s"\n$sep")
          println("  RECOMENDACIONES (2/2) — CONCENTRACIÓN Y UTILIDAD")
          println(sep)
          println(f"  ${"feature"}%-25s ${"top %"}%-12s ${"cardinalidad"}%-14s ${"diagnóstico"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val card               = getLong(s"${c}__card")
          val (_, topCount)      = topValues(c)
          val topPct             = topCount * 100.0 / nTotal
          val topPctFmt          = f"$topPct%.2f%%"
          val rConc              = recConcentracion(topPct, card)

          println(f"  $c%-25s $topPctFmt%-12s $card%-14d $rConc")
          }

          if (colsAusentes.nonEmpty)
          println(s"\n  ⚠️  Columnas no encontradas en el DF: ${colsAusentes.mkString(", ")}")

          println(subSep)
          println(s"  Total columnas analizadas: ${colsExistentes.size}  |  Filas totales: ${nTotal.toLong}")
          println(s"$sep\n")
        }


        def resumenBooleanasTabular(df: DataFrame, cols: Seq[String]): Unit = {

            val nTotal = df.count().toDouble
            if (nTotal == 0) { println("⛔  DataFrame vacío."); return }

            // Validar existencia y tipo booleano
            val colsExistentes = cols.filter(df.columns.contains)
            val colsAusentes   = cols.filterNot(df.columns.contains)
            val colsNoBool     = colsExistentes.filterNot { c =>
            df.schema(c).dataType.typeName == "boolean"
            }
            val colsValidas = colsExistentes.filter { c =>
            df.schema(c).dataType.typeName == "boolean"
            }

            if (colsValidas.isEmpty) {
            println("⛔  Ninguna de las columnas indicadas es de tipo booleano.")
            if (colsNoBool.nonEmpty)
            println(s"  Tipo incorrecto: ${colsNoBool.mkString(", ")}")
            return
            }

            // ── PASADA ÚNICA ──────────────────────────────────────────────────────────
            val aggExprs = colsValidas.flatMap { c =>
            Seq(
            sum(when(col(c).isNull,  1).otherwise(0)).alias(s"${c}__nulls"),
            sum(when(col(c) === true, 1).otherwise(0)).alias(s"${c}__trues"),
            sum(when(col(c) === false,1).otherwise(0)).alias(s"${c}__falses")
            )
            }

            val statsRow = df.select(aggExprs: _*).collect()(0)

            def getLong(field: String): Long = {
            val idx = statsRow.fieldIndex(field)
            if (statsRow.isNullAt(idx)) 0L else statsRow.getAs[Long](field)
            }

            // ── LÓGICA DE RECOMENDACIONES ─────────────────────────────────────────────

            def recImputacion(nullPct: Double): String =
            if      (nullPct == 0.0) "✅ Completa — sin imputación"
            else if (nullPct <  5.0) "Imputar: moda booleana (nulos marginales)"
            else if (nullPct < 20.0) "Imputar: moda + flag_nulo auxiliar"
            else if (nullPct < 50.0) "⚠️  Imputar + flag_nulo como feature"
            else                      "❌ Cobertura crítica — evaluar descarte"

            def recBalance(truePct: Double): String = {
            val minority = math.min(truePct, 100.0 - truePct)
            if      (minority <  1.0)  "❌ Cuasi-constante — descartar"
            else if (minority <  5.0)  "⚠️  Desbalance severo — SMOTE o peso de clase"
            else if (minority < 20.0)  "⚠️  Desbalance moderado — monitorear en validación"
            else                        "✅ Balance aceptable — usar directamente"
            }

            def recUtilidad(truePct: Double, nullPct: Double): String =
            if      (nullPct > 50.0)                                  "❌ Demasiados nulos — señal no confiable"
            else if (truePct > 98.0 || truePct < 2.0)                "❌ Sin varianza efectiva — descartar"
            else if (truePct.abs > 80.0 || (100 - truePct) > 80.0)  "⚠️  Baja varianza — usar con cautela"
            else                                                       "✅ Feature útil para modelado"

            // ── SEPARADORES ───────────────────────────────────────────────────────────
            val sep    = "═" * 115
            val subSep = "─" * 115

            // ── BLOQUE 1: ESTADÍSTICOS ────────────────────────────────────────────────
            println(s"\n$sep")
            println("  RESUMEN ESTADÍSTICO — VARIABLES BOOLEANAS")
            println(sep)
            println(f"  ${"feature"}%-25s ${"nulos"}%-10s ${"null %"}%-12s ${"true count"}%-14s ${"true %"}%-12s ${"false count"}%-14s ${"false %"}")
            println(subSep)

            colsValidas.foreach { c =>
            val nulls      = getLong(s"${c}__nulls")
            val trues      = getLong(s"${c}__trues")
            val falses     = getLong(s"${c}__falses")
            val nullPct    = nulls  * 100.0 / nTotal
            val truePct    = trues  * 100.0 / nTotal
            val falsePct   = falses * 100.0 / nTotal

            // Pre-formatear para evitar conflicto del interpolador f con %%
            val nullFmt  = f"$nullPct%.2f%%"
            val trueFmt  = f"$truePct%.2f%%"
            val falseFmt = f"$falsePct%.2f%%"

            println(f"  $c%-25s $nulls%-10d $nullFmt%-12s $trues%-14d $trueFmt%-12s $falses%-14d $falseFmt")
            }
            println(s"$sep\n")

            // ── BLOQUE 2a: IMPUTACIÓN + BALANCE ───────────────────────────────────────
            println(s"\n$sep")
            println("  RECOMENDACIONES (1/2) — IMPUTACIÓN Y BALANCE")
            println(sep)
            println(f"  ${"feature"}%-25s ${"imputación"}%-50s ${"balance de clases"}")
            println(subSep)

            colsValidas.foreach { c =>
            val nullPct  = getLong(s"${c}__nulls") * 100.0 / nTotal
            val truePct  = getLong(s"${c}__trues") * 100.0 / nTotal

            val rImput   = recImputacion(nullPct)
            val rBalance = recBalance(truePct)

            println(f"  $c%-25s $rImput%-50s $rBalance")
            }
            println(s"$sep\n")

            // ── BLOQUE 2b: UTILIDAD PARA MODELADO ─────────────────────────────────────
            println(s"\n$sep")
            println("  RECOMENDACIONES (2/2) — UTILIDAD PARA MODELADO")
            println(sep)
            println(f"  ${"feature"}%-25s ${"true %"}%-12s ${"null %"}%-12s ${"diagnóstico"}")
            println(subSep)

            colsValidas.foreach { c =>
            val nullPct   = getLong(s"${c}__nulls") * 100.0 / nTotal
            val truePct   = getLong(s"${c}__trues") * 100.0 / nTotal
            val nullFmt   = f"$nullPct%.2f%%"
            val trueFmt   = f"$truePct%.2f%%"
            val rUtilidad = recUtilidad(truePct, nullPct)

            println(f"  $c%-25s $trueFmt%-12s $nullFmt%-12s $rUtilidad")
            }

            if (colsAusentes.nonEmpty)
            println(s"\n  ⚠️  No encontradas en el DF  : ${colsAusentes.mkString(", ")}")
            if (colsNoBool.nonEmpty)
            println(s"  ⚠️  Tipo no booleano (omitidas): ${colsNoBool.mkString(", ")}")

            println(subSep)
            println(s"  Total columnas analizadas: ${colsValidas.size}  |  Filas totales: ${nTotal.toLong}")
            println(s"$sep\n")
        }


        def resumenTextoTabular(df: DataFrame, cols: Seq[String], nEjemplos: Int = 3): Unit = {

          val nTotal = df.count().toDouble
          if (nTotal == 0) { println("⛔  DataFrame vacío."); return }

          val colsExistentes = cols.filter(df.columns.contains)
          val colsAusentes   = cols.filterNot(df.columns.contains)

          if (colsExistentes.isEmpty) {
          println("⛔  Ninguna de las columnas indicadas existe en el DataFrame.")
          return
          }

          // ── EXPRESIONES AUXILIARES ────────────────────────────────────────────────
          def noVacio(c: String): Column =
          col(c).isNotNull && trim(col(c).cast("string")) =!= ""

          def lenExpr(c: String): Column =
          length(trim(col(c).cast("string")))

          // ── PASADA ÚNICA: estadísticos de longitud ────────────────────────────────
          val aggExprs = colsExistentes.flatMap { c =>
          Seq(
          sum(when(noVacio(c), 0).otherwise(1)).alias(s"${c}__vacios"),
          sum(when(col(c).isNull, 1).otherwise(0)).alias(s"${c}__nulls"),
          avg(when(noVacio(c), lenExpr(c))).alias(s"${c}__mean_len"),
          // cast a Long — length() devuelve IntegerType en Spark
          max(when(noVacio(c), lenExpr(c))).cast("long").alias(s"${c}__max_len"),
          min(when(noVacio(c), lenExpr(c))).cast("long").alias(s"${c}__min_len"),
          expr(s"""percentile_approx(
          CASE WHEN ${c} IS NOT NULL
          AND trim(CAST(${c} AS STRING)) != ''
          THEN length(trim(CAST(${c} AS STRING)))
          END, 0.5)""").cast("double").alias(s"${c}__median_len")
          )
          }

          val statsRow = df.select(aggExprs: _*).collect()(0)

          def getLong(field: String): Long = {
          val idx = statsRow.fieldIndex(field)
          if (statsRow.isNullAt(idx)) 0L else statsRow.getAs[Long](field)
          }

          def getDouble(field: String): Double = {
          val idx = statsRow.fieldIndex(field)
          if (statsRow.isNullAt(idx)) 0.0 else statsRow.getAs[Double](field)
          }

          // ── LÓGICA DE RECOMENDACIONES ─────────────────────────────────────────────

          def recImputacion(vacioPct: Double, nullPct: Double): String = {
          val emptyPct = vacioPct - nullPct
          if      (vacioPct == 0.0) "✅ Completa — sin imputación"
          else if (vacioPct <  5.0) "Imputar: placeholder 'desconocido' (marginal)"
          else if (vacioPct < 20.0) s"Imputar + flag_vacio auxiliar (vacíos: ${f"$emptyPct%.1f"}% + nulos: ${f"$nullPct%.1f"}%)"
          else if (vacioPct < 50.0) "⚠️  Alta ausencia — flag_vacio obligatorio"
          else                       "❌ Cobertura crítica — evaluar descarte"
          }

          def recFeature(meanLen: Double, maxLen: Long): String =
          if      (meanLen == 0.0)   "❌ Sin contenido — descartar"
          else if (meanLen <  10.0)  "Tratar como categórica (texto muy corto)"
          else if (meanLen <  50.0)  "Extracción de keywords / n-gramas"
          else if (meanLen < 200.0)  "TF-IDF / CountVectorizer"
          else                        "Embeddings (Word2Vec, BERT) — texto largo"

          def recCalidad(minLen: Long, maxLen: Long, meanLen: Double): String = {
          val rango = maxLen - minLen
          if      (meanLen == 0.0)         "❌ Sin contenido"
          else if (rango == 0)             "⚠️  Longitud constante — posible campo estructurado"
          else if (rango > meanLen * 10.0) "⚠️  Alta varianza de longitud — revisar outliers de texto"
          else                              "✅ Longitud consistente"
          }

          // ── SEPARADORES ───────────────────────────────────────────────────────────
          val sep    = "═" * 115
          val subSep = "─" * 115

          // ── BLOQUE 1: ESTADÍSTICOS ────────────────────────────────────────────────
          println(s"\n$sep")
          println("  RESUMEN ESTADÍSTICO — VARIABLES DE TEXTO")
          println(sep)
          println(f"  ${"feature"}%-25s ${"vacíos"}%-10s ${"vacío %"}%-12s ${"min len"}%-10s ${"mean len"}%-12s ${"median len"}%-12s ${"max len"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val vacios      = getLong(s"${c}__vacios")
          val vacioPct    = vacios * 100.0 / nTotal
          val minLen      = getLong(s"${c}__min_len")
          val maxLen      = getLong(s"${c}__max_len")
          val meanLen     = getDouble(s"${c}__mean_len")
          val medLen      = getDouble(s"${c}__median_len")
          val vacioPctFmt = f"$vacioPct%.2f%%"

          println(f"  $c%-25s $vacios%-10d $vacioPctFmt%-12s $minLen%-10d $meanLen%-12.2f $medLen%-12.2f $maxLen")
          }
          println(s"$sep\n")

          // ── BLOQUE 2: EJEMPLOS ────────────────────────────────────────────────────
          val dfCached = df.select(colsExistentes.map(col): _*).cache()

          println(s"\n$sep")
          println(s"  EJEMPLOS DE CONTENIDO (n=$nEjemplos por columna)")
          println(sep)

          colsExistentes.foreach { c =>
          val ejemplos = dfCached
          .filter(col(c).isNotNull && trim(col(c).cast("string")) =!= "")
          .select(trim(col(c).cast("string")))
          .limit(nEjemplos)
          .collect()
          .map(_.getString(0))
          .zipWithIndex

          println(s"  [$c]")
          if (ejemplos.isEmpty) {
          println("    - Sin ejemplos no vacíos")
          } else {
          ejemplos.foreach { case (txt, i) =>
          val corto = if (txt.length > 110) txt.take(110) + "..." else txt
          println(s"    ${i + 1}. $corto")
          }
          }
          println(subSep)
          }

          dfCached.unpersist()

          // ── BLOQUE 3a: IMPUTACIÓN + FEATURE ENGINEERING ───────────────────────────
          println(s"\n$sep")
          println("  RECOMENDACIONES (1/2) — IMPUTACIÓN Y FEATURE ENGINEERING")
          println(sep)
          println(f"  ${"feature"}%-25s ${"imputación"}%-55s ${"feature sugerida"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val vacioPct = getLong(s"${c}__vacios") * 100.0 / nTotal
          val nullPct  = getLong(s"${c}__nulls")  * 100.0 / nTotal
          val meanLen  = getDouble(s"${c}__mean_len")
          val maxLen   = getLong(s"${c}__max_len")

          val rImput   = recImputacion(vacioPct, nullPct)
          val rFeature = recFeature(meanLen, maxLen)

          println(f"  $c%-25s $rImput%-55s $rFeature")
          }
          println(s"$sep\n")

          // ── BLOQUE 3b: CALIDAD DE CONTENIDO ───────────────────────────────────────
          println(s"\n$sep")
          println("  RECOMENDACIONES (2/2) — CALIDAD Y CONSISTENCIA")
          println(sep)
          println(f"  ${"feature"}%-25s ${"mean len"}%-12s ${"max len"}%-12s ${"diagnóstico"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val minLen  = getLong(s"${c}__min_len")
          val maxLen  = getLong(s"${c}__max_len")
          val meanLen = getDouble(s"${c}__mean_len")
          val rCal    = recCalidad(minLen, maxLen, meanLen)

          println(f"  $c%-25s $meanLen%-12.2f $maxLen%-12d $rCal")
          }

          if (colsAusentes.nonEmpty)
          println(s"\n  ⚠️  Columnas no encontradas en el DF: ${colsAusentes.mkString(", ")}")

          println(subSep)
          println(s"  Total columnas analizadas: ${colsExistentes.size}  |  Filas totales: ${nTotal.toLong}")
          println(s"$sep\n")
        }


        def resumenMalTipadasTabular(df: DataFrame, cols: Seq[String], nEjemplos: Int = 3): Unit = {

          val nTotal = df.count().toDouble
          if (nTotal == 0) { println("⛔  DataFrame vacío."); return }

          val colsExistentes = cols.filter(df.columns.contains)
          val colsAusentes   = cols.filterNot(df.columns.contains)

          if (colsExistentes.isEmpty) {
          println("⛔  Ninguna de las columnas indicadas existe en el DataFrame.")
          return
          }

          // Regex: captura el número completo incluyendo decimales y signo
          val numRegex = """(-?\d+(?:[.,]\d+)?)"""

          // ── PASADA ÚNICA: conteos en una sola agregación ──────────────────────────
          val aggExprs = colsExistentes.flatMap { c =>
          val rawExpr    = trim(coalesce(col(c).cast("string"), lit("")))
          val hasNumExpr = regexp_extract(rawExpr, numRegex, 1) =!= ""
          val isVacio    = rawExpr === ""

          Seq(
          sum(when(isVacio,                        1).otherwise(0)).cast("long").alias(s"${c}__vacios"),
          sum(when(!isVacio &&  hasNumExpr,        1).otherwise(0)).cast("long").alias(s"${c}__con_num"),
          sum(when(!isVacio && !hasNumExpr,        1).otherwise(0)).cast("long").alias(s"${c}__sin_num")
          )
          }

          val statsRow = df.select(aggExprs: _*).collect()(0)

          def getLong(field: String): Long = {
          val idx = statsRow.fieldIndex(field)
          if (statsRow.isNullAt(idx)) 0L else statsRow.getAs[Long](field)
          }

          // ── LÓGICA DE RECOMENDACIONES (orientadas a regresión) ────────────────────

          /** Extraibilidad: ¿qué tan viable es convertir la columna en numérica? */
          def recExtraibilidad(pctConNum: Double, pctSinNum: Double): String =
          if      (pctConNum >= 95.0) "✅ Extraíble — castear directamente a Double para el modelo"
          else if (pctConNum >= 80.0) "Extraíble con limpieza — castear + flag_extraccion_fallida"
          else if (pctConNum >= 50.0) "⚠️  Extracción parcial — evaluar si el patrón es consistente"
          else                         "❌ No extraíble fiablemente — descartar o tratar como categórica"

          /** Imputación tras extracción: orientada a regresión (mediana > media por outliers) */
          def recImputacion(pctVacio: Double, pctSinNum: Double): String = {
          val pctProblema = pctVacio + pctSinNum
          if      (pctProblema == 0.0) "✅ Sin imputación necesaria"
          else if (pctProblema <  5.0) "Imputar con mediana tras extracción (nulos marginales)"
          else if (pctProblema < 20.0) "Imputar con mediana + flag_imputado como feature auxiliar"
          else if (pctProblema < 50.0) "⚠️  Imputar con mediana + flag_imputado obligatorio"
          else                          "❌ Cobertura crítica — evaluar si aporta señal al modelo de precio"
          }

          /** Utilidad para regresión: una columna mal tipada pero extraíble puede ser buen predictor */
          def recUtilidad(pctConNum: Double, colName: String): String =
          if      (pctConNum >= 80.0) s"Convertir a Double y tratar como numérica — incluir en pipeline de regresión"
          else if (pctConNum >= 50.0) s"Incluir con cautela — añadir flag_valor_extraido como feature binaria"
          else                         s"Bajo aporte esperado — solo incluir si correlación con price > 0.1"

          // ── SEPARADORES ───────────────────────────────────────────────────────────
          val sep    = "═" * 115
          val subSep = "─" * 115

          // ── BLOQUE 1: ESTADÍSTICOS ────────────────────────────────────────────────
          println(s"\n$sep")
          println("  RESUMEN — VARIABLES NUMÉRICAS MAL TIPADAS COMO TEXTO")
          println(sep)
          println(f"  ${"feature"}%-25s ${"vacíos"}%-10s ${"vacío %"}%-12s ${"con número"}%-14s ${"con_num %"}%-14s ${"sin número"}%-14s ${"sin_num %"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val vacios   = getLong(s"${c}__vacios")
          val conNum   = getLong(s"${c}__con_num")
          val sinNum   = getLong(s"${c}__sin_num")
          val pctVacio  = vacios * 100.0 / nTotal
          val pctConNum = conNum * 100.0 / nTotal
          val pctSinNum = sinNum * 100.0 / nTotal

          val pctVacioFmt  = f"$pctVacio%.2f%%"
          val pctConNumFmt = f"$pctConNum%.2f%%"
          val pctSinNumFmt = f"$pctSinNum%.2f%%"

          println(f"  $c%-25s $vacios%-10d $pctVacioFmt%-12s $conNum%-14d $pctConNumFmt%-14s $sinNum%-14d $pctSinNumFmt")
          }
          println(s"$sep\n")

          // ── BLOQUE 2: EJEMPLOS ────────────────────────────────────────────────────
          val dfCached = df.select(colsExistentes.map(col): _*).cache()

          println(s"\n$sep")
          println(s"  EJEMPLOS DE EXTRACCIÓN (n=$nEjemplos por columna)")
          println(sep)

          colsExistentes.foreach { c =>
          val rawExpr    = trim(coalesce(col(c).cast("string"), lit("")))
          val numExtExpr = regexp_extract(rawExpr, numRegex, 1)

          val ejemplos = dfCached
          .filter(rawExpr =!= "")
          .select(
          rawExpr.alias("_raw"),
          numExtExpr.alias("_extraido")
          )
          .limit(nEjemplos)
          .collect()

          println(s"  [$c]")
          if (ejemplos.isEmpty) {
          println("    - Sin ejemplos no vacíos")
          } else {
          ejemplos.zipWithIndex.foreach { case (row, i) =>
          val raw      = Option(row.getAs[String]("_raw")).getOrElse("")
          val extraido = Option(row.getAs[String]("_extraido")).filter(_.nonEmpty).getOrElse("NO_EXTRAIBLE")
          val rawShort = if (raw.length > 80) raw.take(80) + "..." else raw
          println(s"    ${i + 1}. original='$rawShort'  →  extraído='$extraido'")
          }
          }
          println(subSep)
          }

          dfCached.unpersist()

          // ── BLOQUE 3a: EXTRAIBILIDAD + IMPUTACIÓN ────────────────────────────────
          println(s"\n$sep")
          println("  RECOMENDACIONES (1/2) — EXTRAIBILIDAD E IMPUTACIÓN")
          println(sep)
          println(f"  ${"feature"}%-25s ${"extraibilidad"}%-50s ${"imputación tras extracción"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val vacios    = getLong(s"${c}__vacios")
          val conNum    = getLong(s"${c}__con_num")
          val sinNum    = getLong(s"${c}__sin_num")
          val pctVacio  = vacios * 100.0 / nTotal
          val pctConNum = conNum * 100.0 / nTotal
          val pctSinNum = sinNum * 100.0 / nTotal

          val rExt   = recExtraibilidad(pctConNum, pctSinNum)
          val rImput = recImputacion(pctVacio, pctSinNum)

          println(f"  $c%-25s $rExt%-50s $rImput")
          }
          println(s"$sep\n")

          // ── BLOQUE 3b: UTILIDAD PARA EL MODELO DE REGRESIÓN ──────────────────────
          println(s"\n$sep")
          println("  RECOMENDACIONES (2/2) — UTILIDAD PARA MODELO DE REGRESIÓN DE PRECIO")
          println(sep)
          println(f"  ${"feature"}%-25s ${"con_num %"}%-14s ${"diagnóstico y acción recomendada"}")
          println(subSep)

          colsExistentes.foreach { c =>
          val conNum    = getLong(s"${c}__con_num")
          val pctConNum = conNum * 100.0 / nTotal
          val pctFmt    = f"$pctConNum%.2f%%"
          val rUtil     = recUtilidad(pctConNum, c)

          println(f"  $c%-25s $pctFmt%-14s $rUtil")
          }

          if (colsAusentes.nonEmpty)
          println(s"\n  ⚠️  Columnas no encontradas en el DF: ${colsAusentes.mkString(", ")}")

          println(subSep)
          println(s"  Total columnas analizadas: ${colsExistentes.size}  |  Filas totales: ${nTotal.toLong}")
          println(s"$sep\n")
        }

        def resumenIdentificadoresTabular(df: DataFrame, cols: Seq[String]): Unit = {
          val nTotal = df.count().toDouble

          println("\n Resumen de variables identificadoras:\n")
          
          println(f"${"feature"}%-15s ${"nulls"}%-10s ${"null_pct"}%-10s ${"distinct"}%-12s ${"unique_ratio"}%-14s ${"duplicated_values"}%-18s ${"recomendacion"}%-20s")
          println("-" * 120)

          cols.foreach { c =>
            val base = df.withColumn("_id_tmp", col(c).cast("string"))

            val nulls = base.filter(col("_id_tmp").isNull || trim(col("_id_tmp")) === "").count()
            val distincts = base.select("_id_tmp").distinct().count()
            val uniqueRatio = if (nTotal > 0) distincts * 100.0 / nTotal else 0.0

            val duplicatedValues = base.filter(col("_id_tmp").isNotNull && trim(col("_id_tmp")) =!= "").groupBy("_id_tmp")
            .count().filter(col("count") > 1).count()

            val nullPct = if (nTotal > 0) nulls * 100.0 / nTotal else 0.0

            val recomendacion =
              if (uniqueRatio >= 99.0) "Eliminar"
              else if (uniqueRatio >= 20.0) "Revisar"
              else "Agrupar/Revisar"

            println(
              f"$c%-15s $nulls%-10d ${nullPct}%-10.2f $distincts%-12d ${uniqueRatio}%-14.2f $duplicatedValues%-18d $recomendacion%-20s"
            )
          }

          println("-" * 120)
        }

        
        
        def detectarParesRedundantesPorNombre(cols: Seq[String]): Seq[(String, String)] = {
          def normalizar(nombre: String): Set[String] = {
            nombre.toLowerCase.split("_").map(_.trim).filter(_.nonEmpty).toSet
          }

          val pares = for {
            c1 <- cols
            c2 <- cols
            if c1 < c2
            t1 = normalizar(c1)
            t2 = normalizar(c2)
            inter = t1.intersect(t2)
            if inter.nonEmpty
          } yield (c1, c2)

          pares.distinct
        }
        

        def validarParesRedundantes(df: DataFrame, pares: Seq[(String, String)]): Unit = {

          println("\nPosibles pares redundantes detectados:\n")
          println(
            f"${"col_1"}%-25s ${"col_2"}%-25s ${"match_pct"}%-12s ${"card_1"}%-10s ${"card_2"}%-10s ${"tipo_relacion"}%-22s ${"accion"}%-18s"
          )
          println("-" * 130)

          pares.foreach { case (c1, c2) =>
            val base = df
              .withColumn("_c1", trim(lower(coalesce(col(c1).cast("string"), lit("")))))
              .withColumn("_c2", trim(lower(coalesce(col(c2).cast("string"), lit("")))))

            val comparables = base.filter(col("_c1") =!= "" && col("_c2") =!= "")
            val nComp = comparables.count()

            val nMatch = comparables.filter(col("_c1") === col("_c2")).count()
            val matchPct = if (nComp > 0) nMatch * 100.0 / nComp else 0.0

            val card1 = base.filter(col("_c1") =!= "").select("_c1").distinct().count()
            val card2 = base.filter(col("_c2") =!= "").select("_c2").distinct().count()

            val ratioCard =
              if (math.max(card1, card2) > 0)
                math.min(card1, card2).toDouble / math.max(card1, card2).toDouble
              else 0.0

            val (tipoRelacion, accion) =
              if (matchPct >= 95.0 && ratioCard >= 0.90)
                ("Redundancia alta", "Eliminar una")
              else if (matchPct >= 70.0)
                ("Posible redundancia", "Revisar")
              else if (matchPct >= 20.0)
                ("Relacion parcial", "Analizar")
              else
                ("No redundante", "Mantener")

            println(
              f"$c1%-25s $c2%-25s ${matchPct}%-12.2f $card1%-10d $card2%-10d ${tipoRelacion}%-22s ${accion}%-18s"
            )
          }

          println("-" * 130)
        }

        

        def resumenGeografico(df: DataFrame,latCol: String = "latitude",lonCol: String = "longitude",cityCol: String = "city",zipCol:  String = "dealer_zip"
        ): Unit = {

            // ── 1. PASADA ÚNICA (Optimización de Performance) ──────────────────────────
            val stats: Row = df.select(
              count("*").as("nTotal"),
              sum(when(col(latCol).isNull, 1).otherwise(0)).as("nullLat"),
              sum(when(col(lonCol).isNull, 1).otherwise(0)).as("nullLon"),
              sum(when(col(cityCol).isNull || trim(col(cityCol)) === "", 1).otherwise(0)).as("nullCity"),
              sum(when(col(zipCol).isNull  || trim(col(zipCol))  === "", 1).otherwise(0)).as("nullZip"),
              sum(when(col(latCol).between(-90.0, 90.0) && col(lonCol).between(-180.0, 180.0), 1).otherwise(0)).as("coordsValidas"),
              sum(when(col(latCol) === 0.0 && col(lonCol) === 0.0, 1).otherwise(0)).as("islaNull"),
              sum(when(col(latCol) === 0.0 && col(lonCol) =!= 0.0, 1).otherwise(0)).as("latCero"),
              sum(when(col(latCol) =!= 0.0 && col(lonCol) === 0.0, 1).otherwise(0)).as("lonCero"),
              countDistinct(when(col(cityCol).isNotNull && trim(col(cityCol)) =!= "", col(cityCol))).as("cityCard"),
              countDistinct(when(col(zipCol).isNotNull  && trim(col(zipCol))  =!= "", col(zipCol))).as("zipCard")
            ).collect()(0)

            // ── 2. EXTRACCIÓN Y CÁLCULOS ───────────────────────────────────────────────
            val nTotal = stats.getAs[Long]("nTotal").toDouble
            if (nTotal == 0) { println("⛔ DataFrame vacío."); return }

            val coordsVal = stats.getAs[Long]("coordsValidas")
            val islaNull  = stats.getAs[Long]("islaNull")
            val cityCard  = stats.getAs[Long]("cityCard")
            val zipCard   = stats.getAs[Long]("zipCard")

            val pctNullLat  = stats.getAs[Long]("nullLat") * 100.0 / nTotal
            val pctNullLon  = stats.getAs[Long]("nullLon") * 100.0 / nTotal
            val pctNullCity = stats.getAs[Long]("nullCity") * 100.0 / nTotal
            val pctNullZip  = stats.getAs[Long]("nullZip") * 100.0 / nTotal
            val pctValidas  = coordsVal * 100.0 / nTotal
            val pctIsla     = islaNull * 100.0 / nTotal

            // ── 3. LÓGICA DE EVALUACIÓN (RECOMENDACIONES) ──────────────────────────────
            def evalCoords(pctVal: Double, pctI: Double): (String, String, String) = {
              if (pctVal >= 95.0 && pctI < 0.5) 
                ("✅ Excelente", "Usar directamente; calidad alta", "Geohashing (p6-8), dist_urban, clustering")
              else if (pctVal >= 80.0) 
                ("✅ Buena", "Limpiar anomalías menores", "Geohashing (p5-6), geo_region, clustering")
              else if (pctVal >= 50.0) 
                ("⚠️ Aceptable", "Imputar nulos (mediana/city)", "geo_region, coord_disponible (flag)")
              else 
                ("❌ Pobre", "No modelar con lat/lon", "Descartar; usar city/zip o externos")
            }

            def evalCat(nullP: Double, card: Long, name: String): (String, String, String) = {
              val estadoBase = if (nullP < 10.0) "✅ Completa" else if (nullP < 30.0) "✅ Buena" else "⚠️ Incompleta"
              val cardTag = if (card <= 100) "Baja Card." else if (card <= 5000) "Alta Card." else "Muy Alta Card."
              val estado = s"$estadoBase ($cardTag)"

              val (rec, feat) = (nullP, card) match {
                case (n, c) if n < 30 && c <= 100  => ("OHE viable", s"${name}_OHE, geo_region")
                case (n, c) if n < 30 && c <= 5000 => ("Target/Freq Encoding", s"${name}_freq, urban_level")
                case (n, c) if n < 60             => ("Agrupar + Freq Encoding", s"${name}_hash, geo_region")
                case _                            => ("Flag binario únicamente", s"flag_${name}_present")
              }
              (estado, rec, feat)
            }

            val (estLat, recLat, featLat) = evalCoords(pctValidas, pctIsla)
            val (estCit, recCit, featCit) = evalCat(pctNullCity, cityCard, cityCol)
            val (estZip, recZip, featZip) = evalCat(pctNullZip, zipCard, zipCol)

            // ── 4. REPORTE VISUAL (Ajuste de espacios) ─────────────────────────────────
            val sep1 = "═" * 155
            val sep2 = "─" * 155
            // Formatos: VARIABLE(12), NULL(9), CARD(14), ESTADO(28), RECOMENDACION(35), FEATURES(Libre)
            val rowFmt = " %-12s | %7.2f%% | %-14s | %-28s | %-35s | %s"

            println(s"\n$sep1")
            println("  EDA GEOGRÁFICO — ANÁLISIS DE CALIDAD Y RECOMENDACIONES")
            println(sep1)
            println(String.format(" %-12s | %-8s | %-14s | %-28s | %-35s | %s", 
              "VARIABLE", "NULL %", "CARDINALIDAD", "ESTADO", "RECOMENDACIÓN", "FEATURES SUGERIDAS"))
            println(sep2)

            println(rowFmt.format(latCol, pctNullLat, "-", estLat, recLat, featLat))
            println(rowFmt.format(lonCol, pctNullLon, "-", estLat, recLat, featLat))
            println(sep2)
            println(rowFmt.format(cityCol, pctNullCity, cityCard.toString, estCit, recCit, featCit))
            println(rowFmt.format(zipCol, pctNullZip, zipCard.toString, estZip, recZip, featZip))
            println(sep1)

            // ── 5. ESTRATEGIA GLOBAL ───────────────────────────────────────────────────
            val estrategia = if (pctValidas >= 80.0) 
              "GEOESPACIAL PRIMARIA: Construir features desde lat/lon (geohash, distancias). City/Zip como respaldo."
            else if (pctNullCity < 30)
              "ADMINISTRATIVA PRIMARIA: Coordenadas inconsistentes. Usar City/Zip para joins sociodemográficos."
            else
              "ENRIQUECIMIENTO REQUERIDO: Cobertura insuficiente en todas las variables geográficas."

            println(s" » ESTRATEGIA RECOMENDADA: $estrategia")
            println(s"$sep1\n")
        }

        
        def resumenTemporalTabular(df: DataFrame,listedDateCol:  String = "listed_date",yearCol:
        String = "year",daysOnMarketCol: String = "daysonmarket",nEjemplos: Int = 5): Unit = {

          val nTotal = df.count().toDouble
          if (nTotal == 0) { println("  DataFrame vacío."); return } 
          val statsRow: Row = df.select(
            // Nulos
            sum(when(col(listedDateCol).isNull, 1).otherwise(0)).as("nullDate"),
            sum(when(col(yearCol).isNull, 1).otherwise(0)).as("nullYear"),
            sum(when(col(daysOnMarketCol).isNull, 1).otherwise(0)).as("nullDays"),
            // Rangos
            min(col(listedDateCol)).as("minDate"),
            max(col(listedDateCol)).as("maxDate"),
            min(col(yearCol).cast("long")).as("minYear"),
            max(col(yearCol).cast("long")).as("maxYear"),
            avg(col(yearCol).cast("double")).as("meanYear"),
            min(col(daysOnMarketCol).cast("long")).as("minDays"),
            max(col(daysOnMarketCol).cast("long")).as("maxDays"),
            avg(col(daysOnMarketCol).cast("double")).as("meanDays"),
            // Anomalías
            sum(when(col(yearCol).isNotNull &&
              (col(yearCol) < 1900 || col(yearCol) > 2100), 1).otherwise(0)).as("invalidYear"),
            sum(when(col(daysOnMarketCol).isNotNull &&
              col(daysOnMarketCol) < 0, 1).otherwise(0)).as("negativeDays"),
            // Cardinalidad de granularidades
            countDistinct(year(col(listedDateCol))).as("yearCard"),
            countDistinct(month(col(listedDateCol))).as("monthCard")
          ).collect()(0)

          // Extracción tipada  
          def getLong(row: Row, field: String): Long =
            if (row.isNullAt(row.fieldIndex(field))) 0L else row.getAs[Long](field)
          def getDouble(row: Row, field: String): Double =
            if (row.isNullAt(row.fieldIndex(field))) 0.0 else row.getAs[Double](field)
          def getString(row: Row, field: String): String =
            if (row.isNullAt(row.fieldIndex(field))) "null" else row.get(row.fieldIndex(field)).toString

          val nullDate     = getLong(statsRow, "nullDate")
          val nullYear     = getLong(statsRow, "nullYear")
          val nullDays     = getLong(statsRow, "nullDays")
          val minDate      = getString(statsRow, "minDate")
          val maxDate      = getString(statsRow, "maxDate")
          val minYear      = getLong(statsRow, "minYear")
          val maxYear      = getLong(statsRow, "maxYear")
          val meanYear     = getDouble(statsRow, "meanYear")
          val minDays      = getLong(statsRow, "minDays")
          val maxDays      = getLong(statsRow, "maxDays")
          val meanDays     = getDouble(statsRow, "meanDays")
          val invalidYear  = getLong(statsRow, "invalidYear")
          val negativeDays = getLong(statsRow, "negativeDays")
          val yearCard     = getLong(statsRow, "yearCard")
          val monthCard    = getLong(statsRow, "monthCard")

          val pctNullDate = nullDate * 100.0 / nTotal
          val pctNullYear = nullYear * 100.0 / nTotal
          val pctNullDays = nullDays * 100.0 / nTotal

          // ── Lógica de recomendación  
          def recomendacion(colName: String, nullPct: Double, invalidos: Long): String =
            colName match {
              case c if c == listedDateCol =>
                if      (nullPct < 20.0)  "Mantener y derivar variables calendario"
                else if (nullPct < 50.0)  "Mantener con cautela"
                else                      "Uso limitado"
              case c if c == yearCol =>
                if      (nullPct < 20.0 && invalidos == 0) "Mantener y derivar antigüedad"
                else if (nullPct < 40.0)                   "Mantener con revisión"
                else                                        "Revisar o descartar"
              case c if c == daysOnMarketCol =>
                if      (nullPct < 20.0 && invalidos == 0) "Mantener"
                else if (nullPct < 40.0)                   "Mantener con revisión"
                else                                        "Uso limitado"
              case _ => "Revisar"
            }

          def features(colName: String): String = colName match {
            case c if c == listedDateCol   => "listed_year, listed_month, listed_quarter, listed_weekday"
            case c if c == yearCol         => "vehicle_age_at_listing"
            case c if c == daysOnMarketCol => "flag_alta_rotacion, flag_baja_rotacion"
            case _                         => "-"
          }

          val recDate = recomendacion(listedDateCol,  pctNullDate, 0L)
          val recYear = recomendacion(yearCol,         pctNullYear, invalidYear)
          val recDays = recomendacion(daysOnMarketCol, pctNullDays, negativeDays)

          // ── REPORTE ───────────────────────────────────────────────────────────────
          val sep    = "═" * 135
          val subSep = "─" * 135

          println(s"\n$sep")
          println(" ANÁLISIS TEMPORAL — EDA")
          println(sep)

          println("\n1. Completitud, anomalías y recomendación")
          println(subSep)
          println(f"  ${"variable"}%-20s ${"nulos"}%-8s ${"null %"}%-10s ${"incidencias"}%-14s ${"recomendación"}%-40s ${"features sugeridas"}")
          println(subSep)
          println(f"  $listedDateCol%-20s $nullDate%-8d $pctNullDate%-10.2f ${0}%-14d $recDate%-40s ${features(listedDateCol)}")
          println(f"  $yearCol%-20s $nullYear%-8d $pctNullYear%-10.2f $invalidYear%-14d $recYear%-40s ${features(yearCol)}")
          println(f"  $daysOnMarketCol%-20s $nullDays%-8d $pctNullDays%-10.2f $negativeDays%-14d $recDays%-40s ${features(daysOnMarketCol)}")

          println(s"\n2. Rango y estadísticos")
          println(subSep)
          println(f"  ${"variable"}%-20s ${"mín"}%-18s ${"máx"}%-18s ${"media"}%-18s")
          println(subSep)
          println(f"  $listedDateCol%-20s $minDate%-18s $maxDate%-18s ${"-"}%-18s")
          println(f"  $yearCol%-20s $minYear%-18d $maxYear%-18d $meanYear%-18.2f")
          println(f"  $daysOnMarketCol%-20s $minDays%-18d $maxDays%-18d $meanDays%-18.2f")

          println(s"\n3. Granularidad derivable desde $listedDateCol")
          println(s"  - Años distintos  : $yearCard")
          println(s"  - Meses distintos : $monthCard")
        
          val dfConFecha = df.filter(col(listedDateCol).isNotNull).cache()

          println(s"\n4. Distribución por $yearCol (top 10)")
          df.filter(col(yearCol).isNotNull).groupBy(yearCol).count().orderBy(desc("count")).show(10, truncate = false)

          println(s"5. Distribución por año de publicación ($listedDateCol)")
          dfConFecha.groupBy(year(col(listedDateCol)).alias("listed_year")).count().orderBy(desc("listed_year")).show(10, truncate = false)

          println("6. Distribución por mes de publicación")
          dfConFecha.groupBy(month(col(listedDateCol)).alias("listed_month")).count().orderBy(asc("listed_month")).show(12, truncate = false)

          dfConFecha.unpersist()

          println(s"7. Ejemplos con las tres variables temporales presentes (n=$nEjemplos)")
          df.select(listedDateCol, yearCol, daysOnMarketCol).filter(col(listedDateCol).isNotNull
          && col(yearCol).isNotNull && col(daysOnMarketCol).isNotNull).show(nEjemplos, truncate = false)

          //  RECOMENDACIÓN GLOBAL  
          val recGlobal = (pctNullDate, pctNullYear) match {
            case (d, y) if d < 20.0 && y < 20.0 =>
              "Mantener variables temporales y generar features derivadas."
            case (d, y) if d < 40.0 || y < 40.0 =>
              "Mantener parcialmente, priorizando year y variables robustas."
            case _ =>
              "Revisar utilidad de las variables temporales antes del modelado."
          }

          println(s"\n8. Recomendación global")
          println(s"   $recGlobal")
          println(s"   Features candidatas: vehicle_age_at_listing, listed_year, listed_month,")
          println(s"   listed_quarter, listed_weekday, flag_alta_rotacion, flag_baja_rotacion.")
          println(s"\n$sep\n")
        }


          
        def analizarGruposFuncionales(df: DataFrame): Unit = {

          val nTotal = df.count().toDouble
          if (nTotal == 0) { println("⛔  DataFrame vacío."); return }

          val grupos = ListMap(
            "TÉCNICO (Motor/Potencia)" -> Seq("engine_displacement", "horsepower", "fuel_type"),
            "USO (Desgaste/Mercado)"   -> Seq("mileage", "owner_count", "daysonmarket"),
            "ESTADO (Integridad)"      -> Seq("has_accidents", "frame_damaged", "salvage"),
            "COMERCIAL (Venta/Precio)" -> Seq("price", "seller_rating", "is_certified")
          )

          val boolCols = df.schema
            .filter(_.dataType.typeName == "boolean")
            .map(_.name)
            .toSet

          // ── PASADA ÚNICA ──────────────────────────────────────────────────────────
          val todasLasCols = grupos.values.flatten.toSeq.distinct
            .filter(df.columns.contains)

          val aggExprs = todasLasCols.flatMap { c =>
            val exprNull = sum(
              when(col(c).isNull, 1).otherwise(0)
            ).as(s"${c}__null")

            val exprZero = if (boolCols.contains(c))
              sum(when(col(c) === false, 1).otherwise(0)).as(s"${c}__zero")
            else
              sum(when(col(c).cast("double") === 0.0, 1).otherwise(0)).as(s"${c}__zero")

            Seq(exprNull, exprZero)
          }

          val statsRow: Row = df.select(aggExprs: _*).collect()(0)

          // ── REPORTE ───────────────────────────────────────────────────────────────
          val sep    = "═" * 120
          val subSep = "─" * 120

          println(s"\n$sep")
          println(s"  ANÁLISIS POR GRUPOS FUNCIONALES")
          println(sep)
          println(f"  ${"GRUPO"}%-30s | ${"VARIABLE"}%-22s | ${"NULL %"}%-14s | ${"CERO/FALSE %"}%-24s | ${"TIPO"}")
          println(sep)

          grupos.foreach { case (nombreGrupo, columnas) =>
            val colsExistentes = columnas.filter(df.columns.contains)
            val colsAusentes   = columnas.filterNot(df.columns.contains)

            if (colsExistentes.isEmpty) {
              println(f"  $nombreGrupo%-30s | [ninguna columna encontrada en el DF]")
              println(subSep)
            } else {
              val pctsNulos = colsExistentes.map { c =>
                val nulos   = statsRow.getAs[Long](s"${c}__null")
                val ceros   = statsRow.getAs[Long](s"${c}__zero")
                val pctNull = nulos * 100.0 / nTotal
                val pctZero = ceros * 100.0 / nTotal
                val tipo    = df.schema(c).dataType.typeName
                val esBool  = boolCols.contains(c)

                val etiqueta  = if (c == colsExistentes.head) nombreGrupo else ""
                val labelCero = if (esBool) "false" else "cero"

                // Pre-formatear valores con % para evitar conflicto del interpolador f
                val nullFmt = f"$pctNull%8.2f%%"
                val zeroFmt = f"$pctZero%8.2f%% ($labelCero)"

                println(f"  $etiqueta%-30s | $c%-22s | $nullFmt%-14s | $zeroFmt%-24s | $tipo")
                pctNull
              }

              // ── Salud del bloque ──
              val avgNulos    = pctsNulos.sum / colsExistentes.size
              val saludBloque = if      (avgNulos <  5.0) "✅ SÓLIDO"
                                else if (avgNulos < 20.0) "⚠️  ACEPTABLE"
                                else                       "❌ CRÍTICO"

              val ausentes = if (colsAusentes.isEmpty) ""
                            else s"  [ausentes en DF: ${colsAusentes.mkString(", ")}]"

              val avgFmt = f"$avgNulos%.1f%%"
              println(f"  ${"  → Salud bloque:"}%-30s   $saludBloque  (avg nulos: $avgFmt)$ausentes")
              println(subSep)
            }
          }
        }

    def analisisEDA(df: DataFrame): Unit = {
      println("\n==================== EDA: Análisis Exploratorio ====================\n") 

      mostrarNulosPorColumna(df)
 
      println("\n  Resumen de variables numéricas:\n")

      resumenNumericoTabular(df.drop("vin","listing_id","sp_id")) // excluimos identificadores para este resumen
      val categoricas = Seq("body_type","fuel_type","transmission","make_name","model_name","exterior_color",
      "interior_color","wheel_system")


      resumenCategoricoTabular(df, categoricas)

      val booleanas = Seq("fleet","frame_damaged","franchise_dealer","has_accidents","isCab","is_cpo","is_new","is_oemcpo","salvage","theft_title")

      println("\n  Resumen de variables booleanas:\n")
      resumenBooleanasTabular(df, booleanas)

      val textoNoEstructurado = Seq("description", "major_options")
      resumenTextoTabular(df, textoNoEstructurado)

      val malTipadas = Seq("engine_cylinders","power","torque","back_legroom","fuel_tank_volume","width","height")
      resumenMalTipadasTabular(df, malTipadas)
      
      val identificadoras = Seq("vin", "listing_id", "sp_id")
      resumenIdentificadoresTabular(df, identificadoras)
      
      val columnasString = df.schema.fields.filter(_.dataType.simpleString == "string").map(_.name).toSeq
      val candidatos = detectarParesRedundantesPorNombre(columnasString)
      validarParesRedundantes(df, candidatos)

      resumenGeografico(df)
      
      resumenTemporalTabular(df= df,listedDateCol  = "listed_date",yearCol= "year",daysOnMarketCol = "daysonmarket",nEjemplos= 10)
      
      analizarGruposFuncionales(df)
    }

        

   