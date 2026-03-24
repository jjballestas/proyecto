import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
  

object Utils { 
  
  //esta función verifica si un archivo o directorio existe en HDFS
  def hdfsExists(spark: SparkSession, path: String): Boolean = {
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  fs.exists(new Path(path))
  }

  // está función carga el CSV con opciones para manejar comillas, saltos de línea, etc. 
  def loadData(spark: SparkSession, path: String, filename: String): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true")
    .option("multiLine", "true").option("quote", "\"").option("escape", "\"")
    .option("mode", "PERMISSIVE").csv(path + filename)
  }
 
  def loadParquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

 //ESTA función guarda un DataFrame en formato Parquet, sobrescribiendo si ya existe un archivo en la ruta especificada.
  def saveParquet(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").parquet(path)
  }


  //esta función carga un DataFrame desde un archivo Parquet ubicado en la ruta especificada.
  //si la ruta no esiste intenta cargar el CSV original, procesarlo y guardarlo como Parquet para futuras cargas más rápidas.
  //si forceCreateParquet es true, se elimina Parquet existente y se creará uno nuevo a partir del CSV
def loadDataParquet( spark: SparkSession,basepath: String,rawFile: String,parquetPath: String,forceCreateParquet: Boolean
): DataFrame = {

  val csvFullPath = basepath + rawFile
  val parquetFullPath = basepath + parquetPath
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val parquetHdfsPath = new Path(parquetFullPath)

  if (forceCreateParquet) {
    println(s"Forzando recreación del Parquet en: $parquetFullPath")

    if (fs.exists(parquetHdfsPath)) {
      println("🗑 Eliminando Parquet existente...")
      fs.delete(parquetHdfsPath, true)
    }

    println(s"Leyendo CSV desde: $csvFullPath")
    val df = loadData(spark, basepath, rawFile)

    println(s" Guardando nuevo Parquet en: $parquetFullPath")
    saveParquet(df, parquetFullPath)

    return df
  }

  if (fs.exists(parquetHdfsPath)) {
    println(s"Cargando Parquet existente desde: $parquetFullPath")
    return loadParquet(spark, parquetFullPath)
  }

  println(s" No existe Parquet. Leyendo CSV desde: $csvFullPath")
  val df = loadData(spark, basepath, rawFile)

  println(s" Creando Parquet en: $parquetFullPath")
  saveParquet(df, parquetFullPath)

  df
}

  //esta funcion identifica columnas numéricas del dataframe y muestra un resumen 
  //estadístico de cada una, incluyendo conteo, media, desviación estándar, mínimo y máximo.
  def resumenNumericoTabular(df: DataFrame): Unit = {
    val numericCols = df.dtypes.filter { case (_, t) =>
        t == "IntegerType" || t == "DoubleType" || t == "FloatType" || t == "LongType"
      }.map(_._1)

    if (numericCols.isEmpty) {
      println("No hay columnas numéricas.")
      return
    }

    println(
      f"${"feature"}%-22s ${"n"}%-8s ${"min"}%-12s ${"max"}%-12s ${"mean"}%-12s ${"median"}%-12s ${"stddev"}%-12s ${"skew"}%-12s ${"kurt"}%-12s"
    )
    println("-" * 120)

    numericCols.foreach { colName =>
      val row = df.agg(
        count("*").alias("n"),
        min(col(colName)).alias("min"),
        max(col(colName)).alias("max"),
        avg(col(colName)).alias("mean"),
        expr(s"percentile_approx($colName, 0.5)").alias("median"),
        stddev(col(colName)).alias("stddev"),
        skewness(col(colName)).alias("skew"),
        kurtosis(col(colName)).alias("kurt")
      ).collect()(0)

      val nVal      = Option(row.getAs[Any]("n")).map(_.toString).getOrElse("NULL")
      val minVal    = Option(row.getAs[Any]("min")).map(_.toString).getOrElse("NULL")
      val maxVal    = Option(row.getAs[Any]("max")).map(_.toString).getOrElse("NULL")
      val meanVal   = Option(row.getAs[Any]("mean")).map(v => f"${v.toString.toDouble}%.4f").getOrElse("NULL")
      val medianVal = Option(row.getAs[Any]("median")).map(v => f"${v.toString.toDouble}%.4f").getOrElse("NULL")
      val stdVal    = Option(row.getAs[Any]("stddev")).map(v => f"${v.toString.toDouble}%.4f").getOrElse("NULL")
      val skewVal   = Option(row.getAs[Any]("skew")).map(v => f"${v.toString.toDouble}%.4f").getOrElse("NULL")
      val kurtVal   = Option(row.getAs[Any]("kurt")).map(v => f"${v.toString.toDouble}%.4f").getOrElse("NULL")

      println(
        f"$colName%-22s $nVal%-8s $minVal%-12s $maxVal%-12s $meanVal%-12s $medianVal%-12s $stdVal%-12s $skewVal%-12s $kurtVal%-12s"
      )
    }
  }

//esta función muestra el conteo de valores nulos por columna, tanto en número absoluto como en porcentaje, 
//adaptando la lógica para diferentes tipos de datos (numéricos, cadenas, booleanos).
  def anterior_mostrarNulosPorColumna(df: DataFrame): Unit = {
    val totalRows = df.count()

    val exprs = df.dtypes.map { case (colName, dataType) =>
      if (dataType == "DoubleType" || dataType == "FloatType")
        sum(when(col(colName).isNull || isnan(col(colName)), 1).otherwise(0)).alias(colName)
      else if (dataType == "StringType")
        sum(when(col(colName).isNull || trim(col(colName)) === "", 1).otherwise(0)).alias(colName)
      else
        sum(when(col(colName).isNull, 1).otherwise(0)).alias(colName)
    }

    val nullRow = df.select(exprs: _*).collect()(0)

    println("  Valores nulos por columna:")
    println(f"${"Columna"}%-25s ${"Nulos"}%12s ${"% Nulos"}%12s")
    println("-" * 52)

    df.columns.zipWithIndex.foreach { case (colName, idx) =>
      val nulls = nullRow.getLong(idx)
      val porcentaje =
        if (totalRows > 0) (nulls.toDouble / totalRows.toDouble) * 100.0
        else 0.0

      println(f"$colName%-25s ${nulls}%12d ${porcentaje}%11.2f%%")
    }

    println()
  }
    
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
  //helper para mostrar un subconjunto de columnas seleccionadas de forma bonita, con numero de filas personalizado
    def showTable(df: DataFrame, numRows: Int): Unit = {
      val colsBonitas = Seq( "vin", "make_name", "model_name", "year", "price","mileage", "body_type", "fuel_type", "transmission_display").filter(df.columns.contains)
      df.select(colsBonitas.head, colsBonitas.tail: _*).show(numRows, truncate = 25)
      }

//esta función realiza un análisis exploratorio de datos (EDA) específico para la variable objetivo "price",
  def analisisEDA(df: DataFrame): Unit = {
    println("\n==================== EDA: Análisis Exploratorio ====================\n")

    println("  Valores nulos por columna:")
    mostrarNulosPorColumna(df)
    println()
   
    println("  Resumen de variables numéricas:")
    resumenNumericoTabular(df)


    println("ANALISIS DE LA VARIABLE OBJETIVO PRICE:")
    Utils.showDF("Resumen global de price", Utils.analyzePriceGlobalStats(df))
    Utils.showDF("Percentiles de price", Utils.analyzePricePercentiles(df))
    Utils.showDF("Comparación price vs log(price)", Utils.analyzePriceVsLogPrice(df))
    Utils.showDF("Precio por is_new", Utils.analyzePriceByCategory(df, "is_new"))
    Utils.showDF("Precio por body_type", Utils.analyzePriceByCategory(df, "body_type", topN = 10))
    Utils.showDF("Precio por make_name", Utils.analyzePriceByTopCategories(df, "make_name", topCategories = 10))
    Utils.showDF("Precio por year", Utils.analyzePriceByYear(df), n = 20)
    Utils.showDF("Top vehículos más caros", Utils.getTopExpensiveVehicles(df, topN = 30), n = 10)
    Utils.showDF("Top 5 precios por marca", Utils.getTopKPriceByCategory(df, "make_name", topCategories = 15, k = 5), n = 50)
    
    val dfGeo = agregarFeaturesUrbanasHaversine(df)
    Utils.showDF("price por geo_region",analyzePriceByGeoRegion(dfGeo),n = 10)
    Utils.showDF("price por urban_level",analyzePriceByUrbanProximity(dfGeo),n = 10)
    println("ANALISIS DE OUTLIERS EN PRICE:")
    Utils.showDF("Identificar errores de datos vs precios reales de lujo",
    Utils.getGlobalIQROutlierBounds(df, "price", positiveOnly = true).withColumnRenamed("median", "median_price"))


    Utils.showDF("Resumen para decidir transformación de price", Utils.summarizePriceTransformationDecision(df))

 
    println("ANALISIS DE LA VARIABLE MILEAGE:") 
    
    Utils.showDF("mileage stats",   Utils.analyzeNumericGlobalStats(df, "mileage"))
   Utils.showDF("mileage pctiles",Utils.analyzeNumericPercentiles(df,"mileage",
        positiveOnly = true,probs = Seq(0.25, 0.50, 0.75, 0.95, 0.99, 0.995, 0.999)))

     
    Utils.showDF("mileage por is_new", Utils.getNumericSegmentStats(df, "mileage", Seq("is_new")))

    println("ANALISIS DE LA VARIABLE DAYSONMARKET:")  
    Utils.showDF("dom stats",   Utils.analyzeNumericGlobalStats(df, "daysonmarket"))
 
    Utils.showDF("dom pctiles",Utils.analyzeNumericPercentiles(df,"daysonmarket",
        positiveOnly = true,probs = Seq(0.25, 0.50, 0.75, 0.95, 0.99, 0.995, 0.999)))
    Utils.showDF("dom por body_type", Utils.getNumericSegmentStats(df, "daysonmarket", Seq("body_type")))

     println("ANALISIS DE CORRELACION:")  
 
    Utils.analyzeCorrelationWithPrice(df)

    val corrEngine = df.stat.corr("horsepower", "engine_displacement")
    val corrFuel = df.stat.corr("city_fuel_economy", "highway_fuel_economy")
    println(f"  Correlación entre horsepower y engine_displacement: $corrEngine%.4f")
    println(f"  Correlación entre city_fuel_economy y highway_fuel_economy: $corrFuel%.4f")
    println("\n")
    println("ANALISIS DE VARIABLES CATEGÓRICAS:")
    Utils.analyzeStringColumnsContent(  df,  Seq("power", "torque", "engine_cylinders","back_legroom",
    "front_legroom","fuel_tank_volume","height","length","maximum_seating","wheelbase","width"))
    
    println("\n TOP CATEGORIAS POR COLUMNA CATEGORICA:")
    val categoricasUtiles = Seq("body_type","fuel_type","make_name","model_name","transmission","transmission_display",
    "wheel_system","wheel_system_display","listing_color","exterior_color","interior_color","trim_name"
    ).filter(df.columns.contains)

    categoricasUtiles.foreach { colName =>
      println(s"\n - $colName:")
      df.groupBy(colName).count().orderBy(desc("count")).show(10, false)
    }

    
  }

//esta función analiza la correlación entre variables numéricas y la variable objetivo "price", 
//calcula el coeficiente de correlación de Pearson para cada variable numérica en relación con "price".
 def analyzeCorrelationWithPrice(df: DataFrame,targetCol: String = "price",excludeCols: Seq[String] = Seq()
): Unit = {

  
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
 
//esta función transforma la variable objetivo "price" aplicando el logaritmo natural,
// creando una nueva columna "log_price" y eliminando la columna original "price".
def transformTargetToLog(df: DataFrame): DataFrame = {
  df.withColumn("log_price", log(col("price"))).drop("price")    
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
  def analyzeNumericGlobalStats(df: DataFrame,numericCol: String,positiveOnly: Boolean = false
  ): DataFrame = {
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
    val dfNum = getValidNumericDF(df, numericCol, positiveOnly)
    val probsStr = probs.mkString(", ")

    dfNum.selectExpr(
      s"percentile_approx($numericCol, array($probsStr)) as percentiles"
    )
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

  //esta función calcula los límites de outliers basados en el rango intercuartílico (IQR) para una variable numérica .
  def getGlobalIQROutlierBounds(df: DataFrame,numericCol: String,positiveOnly: Boolean = false): DataFrame = {
    val dfNum = getValidNumericDF(df, numericCol, positiveOnly)

    dfNum.selectExpr(
      s"percentile_approx($numericCol, 0.25) as q1",
      s"percentile_approx($numericCol, 0.50) as median",
      s"percentile_approx($numericCol, 0.75) as q3"
    )
    .withColumn("iqr", col("q3") - col("q1"))
    .withColumn("lower_bound_iqr_1_5", col("q1") - lit(1.5) * col("iqr"))
    .withColumn("upper_bound_iqr_1_5", col("q3") + lit(1.5) * col("iqr"))
    .withColumn("lower_bound_iqr_3_0", col("q1") - lit(3.0) * col("iqr"))
    .withColumn("upper_bound_iqr_3_0", col("q3") + lit(3.0) * col("iqr"))
  }

//esta función calcula estadísticas de una variable numérica segmentada por una o más columnas categóricas,
  def getNumericSegmentStats(df: DataFrame,numericCol: String,segmentCols: Seq[String],positiveOnly: Boolean = false,minGroupSize: Int = 30
  ): DataFrame = {
    val dfNum = getValidNumericDF(df, numericCol, positiveOnly)

    dfNum.groupBy(segmentCols.map(col): _*)
      .agg(
        count("*").alias("group_n"),
        min(numericCol).alias("min_value"),
        expr(s"percentile_approx($numericCol, 0.25)").alias("q1"),
        expr(s"percentile_approx($numericCol, 0.5)").alias("median_value"),
        expr(s"percentile_approx($numericCol, 0.75)").alias("q3"),
        expr(s"percentile_approx($numericCol, 0.95)").alias("p95"),
        expr(s"percentile_approx($numericCol, 0.99)").alias("p99"),
        avg(numericCol).alias("mean_value"),
        max(numericCol).alias("max_value"),
        skewness(numericCol).alias("skew_value")
      )
      .withColumn("iqr", col("q3") - col("q1"))
      .withColumn("lower_bound_iqr_1_5", col("q1") - lit(1.5) * col("iqr"))
      .withColumn("upper_bound_iqr_1_5", col("q3") + lit(1.5) * col("iqr"))
      .withColumn("lower_bound_iqr_3_0", col("q1") - lit(3.0) * col("iqr"))
      .withColumn("upper_bound_iqr_3_0", col("q3") + lit(3.0) * col("iqr"))
      .filter(col("group_n") >= minGroupSize)
  }

//esta función identifica outliers sospechosos en una variable numérica específica, segmentada por una o más columnas categóricas,  utilizando límites basados en el rango intercuartílico (IQR) y permitiendo filtrar solo valores positivos y establecer un valor mínimo absoluto para considerar un outlier como sospechoso.
  def detectSuspiciousNumericOutliersBySegment(
      df: DataFrame,
      numericCol: String,
      segmentCols: Seq[String],
      positiveOnly: Boolean = false,
      minGroupSize: Int = 30,
      iqrMultiplier: Double = 3.0,
      minAbsoluteValue: Double = Double.MinValue,
      extraCols: Seq[String] = Seq()
  ): DataFrame = {
    val dfNum = getValidNumericDF(df, numericCol, positiveOnly)

    val validSegmentCols = segmentCols.filter(df.columns.contains)

    val stats = dfNum.groupBy(validSegmentCols.map(col): _*)
      .agg(
        count("*").alias("group_n"),
        expr(s"percentile_approx($numericCol, 0.25)").alias("q1"),
        expr(s"percentile_approx($numericCol, 0.5)").alias("segment_median_value"),
        expr(s"percentile_approx($numericCol, 0.75)").alias("q3"),
        expr(s"percentile_approx($numericCol, 0.95)").alias("segment_p95"),
        expr(s"percentile_approx($numericCol, 0.99)").alias("segment_p99")
      )
      .withColumn("iqr", col("q3") - col("q1"))
      .withColumn("segment_upper_bound", col("q3") + lit(iqrMultiplier) * col("iqr"))
      .withColumn("segment_lower_bound", col("q1") - lit(iqrMultiplier) * col("iqr"))
      .filter(col("group_n") >= minGroupSize)

    val selectedExtraCols = extraCols.filter(df.columns.contains).distinct

    dfNum.join(stats, validSegmentCols, "inner")
      .withColumn(s"${numericCol}_to_median_ratio", col(numericCol) / col("segment_median_value"))
      .withColumn(s"${numericCol}_to_p95_ratio", col(numericCol) / col("segment_p95"))
      .withColumn(s"${numericCol}_to_p99_ratio", col(numericCol) / col("segment_p99"))
      .filter(
        (col(numericCol) > col("segment_upper_bound") || col(numericCol) < col("segment_lower_bound")) &&
        col(numericCol) >= lit(minAbsoluteValue)
      )
      .select(
        validSegmentCols.map(col) ++
        Seq(
          col("group_n"),
          col(numericCol),
          col("segment_median_value"),
          col("segment_p95"),
          col("segment_p99"),
          col("segment_lower_bound"),
          col("segment_upper_bound"),
          col(s"${numericCol}_to_median_ratio"),
          col(s"${numericCol}_to_p95_ratio"),
          col(s"${numericCol}_to_p99_ratio")
        ) ++
        selectedExtraCols.map(col): _*
      )
      .orderBy(desc(numericCol))
  }

  // ---------------------------------------------------------
  // Resumen de outliers sospechosos por segmento
  // ---------------------------------------------------------
  def summarizeSuspiciousNumericOutliers(
      suspiciousDF: DataFrame,
      numericCol: String,
      segmentCols: Seq[String]
  ): DataFrame = {
    suspiciousDF.groupBy(segmentCols.map(col): _*)
      .agg(
        count("*").alias("n_suspicious"),
        min(numericCol).alias("min_suspicious_value"),
        expr(s"percentile_approx($numericCol, 0.5)").alias("median_suspicious_value"),
        max(numericCol).alias("max_suspicious_value")
      )
      .orderBy(desc("n_suspicious"), desc("max_suspicious_value"))
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

  def analyzePricePercentiles(
      df: DataFrame,
      priceCol: String = "price",
      probs: Seq[Double] = Seq(0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.999)
  ): DataFrame = {
    analyzeNumericPercentiles(df, priceCol, positiveOnly = true, probs = probs).withColumnRenamed("percentiles", "price_percentiles")
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
    
  
    
  def addGeoRegion(   df: DataFrame,    latitudeCol: String = "latitude",    longitudeCol: String = "longitude",    regionCol: String = "geo_region"): DataFrame = {
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

  // ---------------------------------------------------------
  // Precio por región geográfica
  // ---------------------------------------------------------
  def analyzePriceByGeoRegion(    df: DataFrame,    latitudeCol: String = "latitude",    
  longitudeCol: String = "longitude",    regionCol: String = "geo_region"): DataFrame = {
    val dfRegion = addGeoRegion(df, latitudeCol, longitudeCol, regionCol)
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

  // ---------------------------------------------------------
  // Distancia Haversine en millas
  // ---------------------------------------------------------
  def haversineMiles(lat1: Column, lon1: Column, lat2: Double, lon2: Double): Column = {
    val r = 3958.8

    val dLat = radians(lit(lat2) - lat1)
    val dLon = radians(lit(lon2) - lon1)

    val a =
      pow(sin(dLat / 2), 2) +
        cos(radians(lat1)) * cos(radians(lit(lat2))) * pow(sin(dLon / 2), 2)

    lit(r) * lit(2) * asin(sqrt(a))
  }

  // ---------------------------------------------------------
  // Añadir proximidad urbana a partir de grandes ciudades
  // ---------------------------------------------------------
  def agregarFeaturesUrbanasHaversine(df: DataFrame): DataFrame = {

    val ciudades = Seq(
      (40.7128, -74.0060),   // New York
      (34.0522, -118.2437),  // Los Angeles
      (41.8781, -87.6298),   // Chicago
      (29.7604, -95.3698),   // Houston
      (33.4484, -112.0740),  // Phoenix
      (39.9526, -75.1652),   // Philadelphia
      (29.4241, -98.4936),   // San Antonio
      (32.7157, -117.1611),  // San Diego
      (32.7767, -96.7970),   // Dallas
      (37.3382, -121.8863),  // San Jose
      (25.7617, -80.1918),   // Miami
      (33.7490, -84.3880),   // Atlanta
      (47.6062, -122.3321),  // Seattle
      (39.7392, -104.9903),  // Denver
      (42.3601, -71.0589)    // Boston
    )

    val distExprs = ciudades.map { case (lat, lon) =>
      haversineMiles(col("latitude"), col("longitude"), lat, lon)
    }

    val minDistExpr = least(distExprs: _*)

    df.withColumn("dist_to_major_city_miles", minDistExpr)
      .withColumn(
        "urban_level",
        when(col("dist_to_major_city_miles") <= 25, "urban")
          .when(col("dist_to_major_city_miles") <= 100, "suburban")
          .otherwise("rural")
      )
  }
  // ---------------------------------------------------------
  // Precio según proximidad urbana
  // ---------------------------------------------------------
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


  def getPriceThresholds(df: DataFrame): (Double, Double) = {
    val quantiles = df.stat.approxQuantile("price", Array(0.90, 0.95), 0.01)
    (quantiles(0), quantiles(1)) // p90, p95
  }

  // ---------------------------------------------------------
  // Analizar contenido de columnas string antes de procesarlas
  // ---------------------------------------------------------
  def analyzeStringColumnsContent(df: DataFrame,colsToAnalyze: Seq[String],sampleTopN: Int = 20
  ): Unit = {

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

  def addGamaAlta(df: DataFrame): DataFrame = {

    val dfClean = df
      .withColumn("make_name_clean", trim(lower(col("make_name"))))
      .withColumn("body_type_clean", trim(lower(col("body_type"))))

    val (p90, p95) = getPriceThresholds(dfClean.filter(col("price") > 4000))

    dfClean.withColumn(
      "gama_alta",
      when(col("price") <= 4000, 0)
        .when(col("price") >= p95, 1)
        .when(
          col("price") >= p90 &&
          (
            col("horsepower") > 300 ||
            col("make_name_clean").isin(
              "bmw", "mercedes-benz", "audi", "porsche", "lexus", "tesla",
              "ferrari", "mclaren", "lamborghini", "bentley", "rolls-royce",
              "aston martin", "maserati", "bugatti", "pagani", "koenigsegg"
            ) ||
            col("body_type_clean").isin("coupe", "convertible")
          ),
          1
        )
        .otherwise(0)
    ).drop("make_name_clean", "body_type_clean")
  }

  def treatMileageOutliers(df: DataFrame): DataFrame = {
  val sentinel = 99000000.0
  val capUsados = df.filter(col("is_new") === false && col("mileage").isNotNull && col("mileage") < sentinel)
    .selectExpr("percentile_approx(mileage, 0.99) as p99").first().getDouble(0)

  println(f"  [treatMileageOutliers] cap is_new=false: $capUsados%.0f mi")

  df.withColumn("mileage",when(col("is_new") === true && col("mileage") > 500,
        lit(null).cast("double")).when(col("is_new") === false && col("mileage") > capUsados,lit(capUsados)).otherwise(col("mileage"))
    )
}

def treatDaysOnMarket(df: DataFrame): DataFrame = {
  val p99 = df.filter(col("daysonmarket").isNotNull).selectExpr("percentile_approx(daysonmarket, 0.99) as p99").first().getInt(0)
  println(s"  [treatDaysOnMarket] cap p99 = $p99 días")
  df.withColumn("daysonmarket",when(col("daysonmarket") > p99, p99).otherwise(col("daysonmarket")))
}

def treatSavingsAmount(df: DataFrame, mode: String = "binary"): DataFrame =
  mode match {
    case "drop"   => df.drop("savings_amount")
    case "binary" =>
      df.withColumn("has_savings",when(col("savings_amount") > 0, 1).otherwise(0)).drop("savings_amount")
    case "keep_both" =>
      df.withColumn("has_savings",when(col("savings_amount") > 0, 1).otherwise(0))
    case _ => df
  }

  // esta función extrae características numéricas de columnas string que contienen números mezclados con texto, 
  // como "200 hp" o "5.0 L", creando nuevas columnas numéricas y eliminando las originales para facilitar el análisis y modelado posterior
  def extractStringNumericFeatures(df: DataFrame): DataFrame = {

    def extractNumber(colName: String): Column = {
      regexp_extract(col(colName), "([0-9]+\\.?[0-9]*)", 1).cast("double")
    }

    def extractCylinders(colName: String): Column = {
      regexp_extract(col(colName), "(\\d+)", 1).cast("double")
    }

    val df2 = df
      .withColumn("power_num", extractNumber("power"))
      .withColumn("torque_num", extractNumber("torque"))
      .withColumn("engine_cylinders_num", extractCylinders("engine_cylinders"))
      .withColumn("back_legroom_num", extractNumber("back_legroom"))
      .withColumn("front_legroom_num", extractNumber("front_legroom"))
      .withColumn("fuel_tank_volume_num", extractNumber("fuel_tank_volume"))
      .withColumn("height_num", extractNumber("height"))
      .withColumn("length_num", extractNumber("length"))
      .withColumn("maximum_seating_num", extractNumber("maximum_seating"))
      .withColumn("wheelbase_num", extractNumber("wheelbase"))
      .withColumn("width_num", extractNumber("width"))

    val colsToDrop = Seq(
      "power",
      "torque",
      "engine_cylinders",
      "back_legroom",
      "front_legroom",
      "fuel_tank_volume",
      "height",
      "length",
      "maximum_seating",
      "wheelbase",
      "width"
    ).filter(df2.columns.contains)

    df2.drop(colsToDrop: _*)
  }

  def addGeographicFeatures(df: DataFrame): DataFrame = {

    val df1 = addGeoRegion(df)
    val df2 = agregarFeaturesUrbanasHaversine(df1)

    val colsToDrop = Seq("latitude", "longitude").filter(df2.columns.contains)
    df2.drop(colsToDrop: _*)
  }

  // ---------------------------------------------------------
  // Crear is_pickup y limpiar variables específicas de pickup
  // ---------------------------------------------------------


  def addIsPickupAndClean(df: DataFrame): DataFrame = {
    val pickupCols = Seq("bed", "bed_height", "bed_length", "cabin")
      .filter(df.columns.contains)

    val dfWithPickup = df.withColumn(
      "is_pickup",
      when(col("body_type") === "Pickup Truck", 1).otherwise(0)
    )

    pickupCols.foldLeft(dfWithPickup) { (acc, c) =>
      acc.withColumn(
        c,
        when(
          col(c).isNull || trim(col(c)).isin("", "--", "NULL", "None", "null"),
          lit(null).cast("string")
        ).otherwise(col(c))
      ).withColumn(
        c,
        when(col("is_pickup") === 1, col(c))
          .otherwise(lit(null).cast("string"))
      )
    }
  }


  def fillBooleanAsCategory(df: DataFrame, cols: Seq[String]): DataFrame = {
    val validCols = cols.filter(df.columns.contains)

    validCols.foldLeft(df) { (acc, c) =>
      acc.withColumn(
        c,
        when(col(c).isNull, "unknown")
          .otherwise(col(c).cast("string"))
      )
    }
  }
  //esta función trata la variable owner_count, que tiene un alto porcentaje de valores nulos, 
  //creando una nueva columna que indica si el valor original estaba ausente y rellenando los nulos con -1 
  //para mantener la información de ausencia sin perder registros
  def treatOwnerCount(df: DataFrame): DataFrame = {
    df.withColumn("owner_count_missing", col("owner_count").isNull.cast("int"))
      .withColumn("owner_count", when(col("owner_count").isNull, -1).otherwise(col("owner_count")))
  }
  def addTemporalFeatures(df: DataFrame): DataFrame = {
    df.withColumn("listed_year", year(col("listed_date")))
      .withColumn("listed_month", month(col("listed_date")))
      .withColumn("vehicle_age_at_listing", year(col("listed_date")) - col("year"))
  }

  def cleanFuelType(df: DataFrame): DataFrame = {
    df.withColumn(
      "fuel_type_clean",
      when(lower(col("fuel_type")).contains("gas"), "gasoline")
        .when(lower(col("fuel_type")).contains("diesel"), "diesel")
        .when(lower(col("fuel_type")).contains("hybrid"), "hybrid")
        .when(lower(col("fuel_type")).contains("electric"), "electric")
        .when(lower(col("fuel_type")).contains("flex"), "flex_fuel")
        .when(col("fuel_type").isNull, "unknown")
        .otherwise("other")
    ).drop("fuel_type")
  }

 
  def addMissingDimensionsFlag(df: DataFrame): DataFrame = {
    df.withColumn(
      "missing_dimensions",
      when(
        col("height_num").isNull &&
        col("length_num").isNull &&
        col("width_num").isNull &&
        col("wheelbase_num").isNull,
        1
      ).otherwise(0)
    )
  }

  def addMissingFlags(df: DataFrame, cols: Seq[String]): DataFrame = {
    val validCols = cols.filter(df.columns.contains)

    validCols.foldLeft(df) { (acc, c) =>
      acc.withColumn(s"${c}_missing", col(c).isNull.cast("int"))
    }
  }


def imputeNumericBlockByGroupWithGlobalFallback(df: DataFrame,colsToImpute: Seq[String],groupCols: Seq[String]): DataFrame = {

  val validGroupCols = groupCols.filter(df.columns.contains)
  val validCols = colsToImpute.filter(df.columns.contains)

  val globalMedians: Map[String, Any] = validCols.map { c =>
    val med = df.selectExpr(s"percentile_approx($c, 0.5) as median").first().get(0)
    c -> med
  }.toMap

  val aggExprs = validCols.map { c =>
    expr(s"percentile_approx($c, 0.5)").alias(s"${c}_group_median")
  }

  val groupedMedians = df.groupBy(validGroupCols.map(col): _*).agg(aggExprs.head, aggExprs.tail: _*)

  val joined = df.join(groupedMedians, validGroupCols, "left")

  val imputed = validCols.foldLeft(joined) { (acc, c) =>
    acc.withColumn(c,when(col(c).isNull && col(s"${c}_group_median").isNotNull, col(s"${c}_group_median"))
    .when(col(c).isNull, lit(globalMedians(c))).otherwise(col(c))
    )
  }

  imputed.drop(validCols.map(c => s"${c}_group_median"): _*)
}



  def imputeDimensions(df: DataFrame): DataFrame = {
    //este bloque corresponde a las columnas de dimensiones del vehículo,
    // que tienen un alto porcentaje de valores nulos y se imputarán utilizando 
    //la mediana por grupo de body_type, is_pickup  
    //con una imputación global como fallback para casos sin grupo definido o con todos los valores nulos en el grupo
    val dimCols = Seq("height_num","length_num","width_num","wheelbase_num","maximum_seating_num","front_legroom_num","back_legroom_num")

    val df1 = addMissingFlags(df, dimCols)
    val df2 = addMissingDimensionsFlag(df1)

    imputeNumericBlockByGroupWithGlobalFallback(df2,dimCols,Seq("body_type", "is_pickup" ))
  }

  def imputeEngineSpecs(df: DataFrame): DataFrame = {
    //este bloque corresponde a las columnas relacionadas con potencia, torque, cilindros y volumen del motor,
    //que también tienen un alto porcentaje de valores nulos y se imputarán utilizando 
    //la misma estrategia de mediana por grupo de body_type, fuel_type_clean y gama_alta, con fallback
    val engineCols = Seq("power_num","torque_num","engine_cylinders_num","engine_displacement","fuel_tank_volume_num")

    val df1 = addMissingFlags(df, engineCols)

    imputeNumericBlockByGroupWithGlobalFallback(df1,engineCols,Seq("body_type", "fuel_type_clean", "gama_alta"))
  }

  def imputeFuelEconomy(df: DataFrame): DataFrame = {
    //este bloque corresponde a las columnas de economía de combustible, que también tienen un alto porcentaje de valores nulos 
    //y se imputarán utilizando la misma estrategia de mediana por grupo de fuel_type_clean, body_type y is_new, con fallback
    val econCols = Seq("city_fuel_economy","highway_fuel_economy")

    val df1 = addMissingFlags(df, econCols)

    imputeNumericBlockByGroupWithGlobalFallback(
      df1,
      econCols,
      Seq("fuel_type_clean", "body_type", "is_new")
    )
  }
    
  def imputeRemainingNumeric(df: DataFrame): DataFrame = {
    
    val engineCols =  Seq("horsepower", "mileage", "seller_rating")

    val df1 = addMissingFlags(df, engineCols)

    imputeNumericBlockByGroupWithGlobalFallback(df1,engineCols,Seq("body_type", "fuel_type_clean", "is_new"))
  }

 
def fillCategoricalUnknown(df: DataFrame, cols: Seq[String]): DataFrame = {
  val validCols = cols.filter(df.columns.contains)

  validCols.foldLeft(df) { (acc, c) =>
    acc.withColumn(c,when(col(c).isNull, "unknown").otherwise(col(c)))
  }
}

def dropColumns(df: DataFrame, colsToDrop: Seq[String]): DataFrame = {
  val validCols = colsToDrop.filter(df.columns.contains)
  df.drop(validCols: _*)
}
    
// Añadir en Utils.scala
def addPowerDensity(df: DataFrame): DataFrame = {
  df.withColumn(
    "power_density",
    when(
      col("engine_displacement").isNotNull && col("engine_displacement") > 0 &&
      col("horsepower").isNotNull,
      col("horsepower") / col("engine_displacement")
    ).otherwise(lit(null).cast("double"))
  )
}
    
  
def filterPriceErrors(df: DataFrame): DataFrame = {
  val marcasMasivas = Seq("Ford","Chevrolet","Nissan","Dodge","Toyota","Honda","Hyundai","Kia","GMC","Jeep","RAM","Buick")
  // IQR upper bound = 97,510 — calculado antes
  // Coches masivos no deberían superar ~200K salvo rarísimas excepciones
  val capMarcasMasivas = 200000.0
  df.filter(!col("make_name").isin(marcasMasivas: _*) ||col("price") <= capMarcasMasivas)
}
// Coches pre-1980 tienen lógica de precio diferente por su valor histórico y coleccionista, 
// así que añadimos una flag para identificarlos y tratarlos aparte 
def addIsClassicFlag(df: DataFrame): DataFrame = {
  df.withColumn("is_classic", when(col("year") < 1980, 1).otherwise(0))
}
}
