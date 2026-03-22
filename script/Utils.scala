import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
  

object Utils { 
  
  // está función carga el CSV con opciones para manejar comillas, saltos de línea, etc. 
def loadData(spark: SparkSession, path: String, filename: String): DataFrame = {
  spark.read.option("header", "true").option("inferSchema", "true")
  .option("multiLine", "true").option("quote", "\"").option("escape", "\"")
  .option("mode", "PERMISSIVE").csv(path + filename)
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

def mostrarNulosPorColumna(df: DataFrame): Unit = {
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

  println("📌 Valores nulos por columna:")
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
def analisisEDA(df: DataFrame): Unit = {
  println("\n==================== EDA: Análisis Exploratorio ====================\n")

  println("📌 Valores nulos por columna:")
  mostrarNulosPorColumna(df)
  println()
 
  println("📌 Resumen de variables numéricas:")
  resumenNumericoTabular(df)
  println("analisis de la variable objetivo price:")
  Utils.showDF("Resumen global de price", Utils.analyzePriceGlobalStats(df))
  Utils.showDF("Percentiles de price", Utils.analyzePricePercentiles(df))
  Utils.showDF("Comparación price vs log(price)", Utils.analyzePriceVsLogPrice(df))
  Utils.showDF("Precio por is_new", Utils.analyzePriceByCategory(df, "is_new"))
  Utils.showDF("Precio por body_type", Utils.analyzePriceByCategory(df, "body_type", topN = 20))
  Utils.showDF("Precio por make_name", Utils.analyzePriceByTopCategories(df, "make_name", topCategories = 20))
  Utils.showDF("Precio por year", Utils.analyzePriceByYear(df), n = 50)
  Utils.showDF("Top vehículos más caros", Utils.getTopExpensiveVehicles(df, topN = 30), n = 30)
  Utils.showDF("Top 5 precios por marca", Utils.getTopKPriceByCategory(df, "make_name", topCategories = 15, k = 5), n = 200)
  
  Utils.showDF("Resumen para decidir transformación de price", Utils.summarizePriceTransformationDecision(df))
val dfGeo = agregarFeaturesUrbanasHaversine(df)

Utils.showDF(
  "price por geo_region",
  analyzePriceByGeoRegion(dfGeo),
  n = 10
)

Utils.showDF(
  "price por urban_level",
  analyzePriceByUrbanProximity(dfGeo),
  n = 10
)


  println("\n📌 Top categorías por columna categórica:")
  val categoricasUtiles = Seq(
    "body_type",
    "fuel_type",
    "make_name",
    "model_name",
    "transmission",
    "transmission_display",
    "wheel_system",
    "wheel_system_display",
    "listing_color",
    "exterior_color",
    "interior_color",
    "trim_name"
  ).filter(df.columns.contains)

  categoricasUtiles.foreach { colName =>
    println(s"\n - $colName:")
    df.groupBy(colName)
      .count()
      .orderBy(desc("count"))
      .show(10, false)
  }

  
}

 
  // ---------------------------------------------------------
  // Verificar si existe archivo/carpeta
  // ---------------------------------------------------------
  def hdfsExists(spark: SparkSession, path: String): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.exists(new Path(path))
  }

  // ---------------------------------------------------------
  // Guardar parquet
  // ---------------------------------------------------------
  def saveParquet(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").parquet(path)
  }

  // ---------------------------------------------------------
  // Cargar parquet
  // ---------------------------------------------------------
  def loadParquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

// ---------------------------------------------------------
// Cargar datos asegurando existencia de Parquet
// ---------------------------------------------------------
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

  // ---------------------------------------------------------
  // Pipeline: cargar CSV o parquet, procesar si es necesario
  // ---------------------------------------------------------
  def loadOrProcessData( spark: SparkSession, basepath: String, rawFile: String, parquetPath: String, realizarEda: Boolean ): DataFrame = {

    val parquetFullPath = basepath + parquetPath

    if (realizarEda) {
      println("🔍 Cargando CSV crudo para EDA")
      return loadData(spark, basepath, rawFile)
    }

    if (hdfsExists(spark, parquetFullPath)) {
      println(s"📦 Cargando datos procesados desde $parquetFullPath")
      return loadParquet(spark, parquetFullPath)
    }

    println(s"⚙ Procesando datos desde $rawFile y guardando en $parquetFullPath")

    val rawDf = loadData(spark, basepath, rawFile)
    println("Realizando preprocesamiento básico...")

    val processedDf = preprocesadoBasico(rawDf)

    println("Guardando datos procesados en formato Parquet...")
    saveParquet(processedDf, parquetFullPath)

    processedDf
  } 

  // ---------------------------------------------------------
  // Preprocesado básico
  // ---------------------------------------------------------
  def preprocesadoBasico(df: DataFrame): DataFrame = {
    df.filter(col("price") > 0).withColumn("log_price", log(col("price"))).na.drop("all")
  }

 

  // ---------------------------------------------------------
  // Mostrar tabla bonita
  // ---------------------------------------------------------
  def showTable(df: DataFrame, numRows: Int): Unit = {
    val colsBonitas = Seq( "vin", "make_name", "model_name", "year", "price","mileage", "body_type", "fuel_type", "transmission_display").filter(df.columns.contains)
    df.select(colsBonitas.head, colsBonitas.tail: _*).show(numRows, truncate = 25)
    }
  // =========================================================
  // EDA GENÉRICO PARA VARIABLES NUMÉRICAS
  // =========================================================

  // ---------------------------------------------------------
  // esta funcion filtra el DataFrame para obtener solo filas válidas de una variable numérica, manteniendo solo valores positivos
  // ---------------------------------------------------------
  def getValidNumericDF(df: DataFrame,numericCol: String,positiveOnly: Boolean = false): DataFrame = {
    val base = df.filter(col(numericCol).isNotNull)

    if (positiveOnly) base.filter(col(numericCol) > 0)
    else base
  }

  // ---------------------------------------------------------
  // Resumen global de una variable numérica
  // ---------------------------------------------------------
  def analyzeNumericGlobalStats(
      df: DataFrame,
      numericCol: String,
      positiveOnly: Boolean = false
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

  // ---------------------------------------------------------
  // Percentiles de una variable numérica
  // ---------------------------------------------------------
  def analyzeNumericPercentiles(
      df: DataFrame,
      numericCol: String,
      positiveOnly: Boolean = false,
      probs: Seq[Double] = Seq(0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.999)
  ): DataFrame = {
    val dfNum = getValidNumericDF(df, numericCol, positiveOnly)
    val probsStr = probs.mkString(", ")

    dfNum.selectExpr(
      s"percentile_approx($numericCol, array($probsStr)) as percentiles"
    )
  }

  // ---------------------------------------------------------
  // Comparar variable numérica vs log1p(variable)
  // Solo recomendable cuando la variable es no negativa o positiva
  // ---------------------------------------------------------
  def analyzeNumericVsLog(
      df: DataFrame,
      numericCol: String,
      positiveOnly: Boolean = true
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

  // ---------------------------------------------------------
  // Top valores extremos de una variable numérica
  // extraCols permite añadir columnas de contexto
  // ---------------------------------------------------------
  def getTopExtremeValues(
      df: DataFrame,
      numericCol: String,
      positiveOnly: Boolean = false,
      topN: Int = 30,
      extraCols: Seq[String] = Seq()
  ): DataFrame = {
    val dfNum = getValidNumericDF(df, numericCol, positiveOnly)
    val selectedCols = (Seq(numericCol) ++ extraCols.filter(df.columns.contains)).distinct

    dfNum.select(selectedCols.map(col): _*)
      .orderBy(desc(numericCol))
      .limit(topN)
  }

  // ---------------------------------------------------------
  // Límites IQR globales para una variable numérica
  // ---------------------------------------------------------
  def getGlobalIQROutlierBounds(
      df: DataFrame,
      numericCol: String,
      positiveOnly: Boolean = false
  ): DataFrame = {
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

  // ---------------------------------------------------------
  // Estadísticos de variable numérica por segmento
  // ---------------------------------------------------------
  def getNumericSegmentStats(
      df: DataFrame,
      numericCol: String,
      segmentCols: Seq[String],
      positiveOnly: Boolean = false,
      minGroupSize: Int = 30
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

  // ---------------------------------------------------------
  // Detectar outliers sospechosos de una variable numérica por segmento
  // No elimina; solo devuelve registros sospechosos
  // ---------------------------------------------------------
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

  // =========================================================
  // WRAPPERS ESPECÍFICOS PARA PRICE
  // =========================================================

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
    analyzeNumericPercentiles(df, priceCol, positiveOnly = true, probs = probs)
      .withColumnRenamed("percentiles", "price_percentiles")
  }

  def analyzePriceVsLogPrice(df: DataFrame, priceCol: String = "price"): DataFrame = {
    analyzeNumericVsLog(df, priceCol, positiveOnly = true)
      .withColumnRenamed("skew_value", "skew_price")
      .withColumnRenamed("kurt_value", "kurt_price")
      .withColumnRenamed("skew_log_value", "skew_log_price")
      .withColumnRenamed("kurt_log_value", "kurt_log_price")
  }

  def analyzePriceByCategory(
      df: DataFrame,
      categoryCol: String,
      priceCol: String = "price",
      topN: Int = 50,
      minCount: Int = 1
  ): DataFrame = {
    val dfPrice = getValidPriceDF(df, priceCol)

    dfPrice.groupBy(categoryCol)
      .agg(
        count("*").alias("n"),
        min(priceCol).alias("min_price"),
        expr(s"percentile_approx($priceCol, 0.25)").alias("p25"),
        expr(s"percentile_approx($priceCol, 0.5)").alias("median_price"),
        expr(s"percentile_approx($priceCol, 0.75)").alias("p75"),
        expr(s"percentile_approx($priceCol, 0.95)").alias("p95"),
        max(priceCol).alias("max_price"),
        avg(priceCol).alias("mean_price"),
        skewness(priceCol).alias("skew_price")
      )
      .filter(col("n") >= minCount)
      .orderBy(desc("median_price"))
      .limit(topN)
  }

  def analyzePriceByTopCategories(
      df: DataFrame,
      categoryCol: String,
      priceCol: String = "price",
      topCategories: Int = 20
  ): DataFrame = {
    val dfPrice = getValidPriceDF(df, priceCol)

    val topCats = dfPrice.groupBy(categoryCol)
      .count()
      .orderBy(desc("count"))
      .limit(topCategories)
      .select(categoryCol)

    dfPrice.join(topCats, Seq(categoryCol))
      .groupBy(categoryCol)
      .agg(
        count("*").alias("n"),
        min(priceCol).alias("min_price"),
        expr(s"percentile_approx($priceCol, 0.25)").alias("p25"),
        expr(s"percentile_approx($priceCol, 0.5)").alias("median_price"),
        expr(s"percentile_approx($priceCol, 0.75)").alias("p75"),
        expr(s"percentile_approx($priceCol, 0.95)").alias("p95"),
        max(priceCol).alias("max_price"),
        avg(priceCol).alias("mean_price")
      )
      .orderBy(desc("median_price"))
  }

  def analyzePriceByYear(df: DataFrame, priceCol: String = "price"): DataFrame = {
    val dfPrice = getValidPriceDF(df, priceCol)

    dfPrice.groupBy("year")
      .agg(
        count("*").alias("n"),
        expr(s"percentile_approx($priceCol, 0.5)").alias("median_price"),
        expr(s"percentile_approx($priceCol, 0.95)").alias("p95"),
        max(priceCol).alias("max_price"),
        avg(priceCol).alias("mean_price"),
        skewness(priceCol).alias("skew_price")
      )
      .orderBy(desc("year"))
  }

  def getTopExpensiveVehicles(
      df: DataFrame,
      priceCol: String = "price",
      topN: Int = 30
  ): DataFrame = {
    getTopExtremeValues(
      df = df,
      numericCol = priceCol,
      positiveOnly = true,
      topN = topN,
      extraCols = Seq("make_name", "model_name", "trim_name", "year", "body_type", "fuel_type", "is_new", "mileage", "horsepower")
    )
  }

  def getTopKPriceByCategory(
      df: DataFrame,
      categoryCol: String,
      priceCol: String = "price",
      topCategories: Int = 15,
      k: Int = 5
  ): DataFrame = {
    val dfPrice = getValidPriceDF(df, priceCol)

    val topCats = dfPrice.groupBy(categoryCol)
      .count()
      .orderBy(desc("count"))
      .limit(topCategories)
      .select(categoryCol)

    val w = org.apache.spark.sql.expressions.Window
      .partitionBy(categoryCol)
      .orderBy(desc(priceCol))

    dfPrice.join(topCats, Seq(categoryCol))
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") <= k)
      .select(
        col(categoryCol),
        col("model_name"),
        col("trim_name"),
        col("year"),
        col("body_type"),
        col("is_new"),
        col("mileage"),
        col("horsepower"),
        col(priceCol),
        col("rn")
      )
      .orderBy(col(categoryCol), desc(priceCol))
  }

  def summarizePriceTransformationDecision(
      df: DataFrame,
      priceCol: String = "price"
  ): DataFrame = {
    val stats = analyzeNumericGlobalStats(df, priceCol, positiveOnly = true)
    val vsLog = analyzeNumericVsLog(df, priceCol, positiveOnly = true)

    stats.crossJoin(vsLog.select(
      col("skew_log_value"),
      col("kurt_log_value")
    ))
    .withColumnRenamed("min_value", "min_price")
    .withColumnRenamed("max_value", "max_price")
    .withColumnRenamed("mean_value", "mean_price")
    .withColumnRenamed("median_value", "median_price")
    .withColumnRenamed("std_value", "std_price")
    .withColumnRenamed("skew_value", "skew_price")
    .withColumnRenamed("kurt_value", "kurt_price")
    .withColumnRenamed("skew_log_value", "skew_log_price")
    .withColumnRenamed("kurt_log_value", "kurt_log_price")
  }

  def getGlobalPricePercentiles(df: DataFrame, priceCol: String = "price"): DataFrame = {
    analyzeNumericPercentiles(
      df,
      numericCol = priceCol,
      positiveOnly = true,
      probs = Seq(0.25, 0.50, 0.75, 0.95, 0.99, 0.995, 0.999)
    ).withColumnRenamed("percentiles", "price_percentiles")
  }

  def getGlobalPriceIQRBounds(df: DataFrame, priceCol: String = "price"): DataFrame = {
    getGlobalIQROutlierBounds(df, priceCol, positiveOnly = true)
      .withColumnRenamed("median", "median_price")
  }

  def getPriceSegmentStats(
      df: DataFrame,
      segmentCols: Seq[String],
      priceCol: String = "price",
      minGroupSize: Int = 30
  ): DataFrame = {
    getNumericSegmentStats(
      df = df,
      numericCol = priceCol,
      segmentCols = segmentCols,
      positiveOnly = true,
      minGroupSize = minGroupSize
    )
    .withColumnRenamed("min_value", "min_price")
    .withColumnRenamed("median_value", "median_price")
    .withColumnRenamed("mean_value", "mean_price")
    .withColumnRenamed("max_value", "max_price")
    .withColumnRenamed("skew_value", "skew_price")
  }

  def detectSuspiciousPriceOutliersBySegment(
      df: DataFrame,
      segmentCols: Seq[String],
      priceCol: String = "price",
      minGroupSize: Int = 30,
      iqrMultiplier: Double = 3.0,
      minAbsolutePrice: Double = 100000.0
  ): DataFrame = {
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
    .withColumnRenamed("segment_median_value", "segment_median_price")
    .withColumnRenamed("price_to_median_ratio", "price_to_median_ratio")
    .withColumnRenamed("price_to_p95_ratio", "price_to_p95_ratio")
    .withColumnRenamed("price_to_p99_ratio", "price_to_p99_ratio")
  }

  def detectSuspiciousPriceOutliersHierarchical(
      df: DataFrame,
      priceCol: String = "price",
      minGroupSize: Int = 30,
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
// Añadir región geográfica fina usando latitude + longitude
// ---------------------------------------------------------
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
def analyzePriceByGeoRegion(    df: DataFrame,    latitudeCol: String = "latitude",    longitudeCol: String = "longitude",    regionCol: String = "geo_region"): DataFrame = {
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

  dfPrice
    .filter(col("urban_level").isNotNull)
    .groupBy("urban_level")
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
    .orderBy(
      when(col("urban_level") === "urban", 1)
        .when(col("urban_level") === "suburban", 2)
        .otherwise(3)
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
    base.groupBy(col(c))
      .count()
      .orderBy(desc("count"))
      .show(sampleTopN, truncate = false)

    println("---- Ejemplos con número extraíble ----")
    base.filter(regexp_extract(col(c), "([0-9]+\\.?[0-9]*)", 1) =!= "")
      .groupBy(col(c))
      .count()
      .orderBy(desc("count"))
      .show(sampleTopN, truncate = false)

    println("---- Ejemplos sin número extraíble ----")
    base.filter(
      col(c).isNotNull &&
      trim(col(c)) =!= "" &&
      !trim(lower(col(c))).isin("null", "none", "--", "n/a", "na") &&
      regexp_extract(col(c), "([0-9]+\\.?[0-9]*)", 1) === ""
    )
      .groupBy(col(c))
      .count()
      .orderBy(desc("count"))
      .show(sampleTopN, truncate = false)
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
def imputeDimensionsByGroup(df: DataFrame): DataFrame = {

  val groupCols = Seq("body_type", "is_pickup")

  val cols = Seq(
    "height_num",
    "length_num",
    "width_num",
    "wheelbase_num",
    "maximum_seating_num"
  )

  cols.foldLeft(df) { (acc, c) =>
    acc.withColumn(
      c,
      when(col(c).isNull,
        expr(s"""
          percentile_approx($c, 0.5)
          OVER (PARTITION BY ${groupCols.mkString(",")})
        """)
      ).otherwise(col(c))
    )
  }
}


//esta función elimina columnas que se consideran irrelevantes para el modelo, 
//ya sea por falta de información, alta cardinalidad, redundancia o baja cobertura
def dropIrrelevantColumns(df: DataFrame): DataFrame = {

  val colsToDrop = Seq(

    // Variables sin información (100% nulos → no aportan varianza)
    "combine_fuel_economy",
    "is_certified",
    "vehicle_damage_category","bed_height","bed_length",

    // Identificadores únicos o casi únicos → no generalizan
    "vin",
    "listing_id",
    "sp_id",

    // Variables de tipo multimedia o texto libre no estructurado
    // (no se utilizarán en este modelo tabular)
    "main_picture_url",
    "description",

    // Variables duplicadas semánticamente
    // Se mantiene la versión más compacta/estandarizada
    "transmission_display",     // redundante con transmission
    "wheel_system_display",     // redundante con wheel_system

    // Variables con alta cardinalidad o ruido categórico
    // sin aportar señal clara adicional frente a otras variables
    "listing_color",
    "dealer_zip",
    "sp_name",

    // Variables específicas de pickups con muy baja cobertura global
    // y que no aportan suficiente señal tras la creación de is_pickup
    "bed",
    "cabin",

    // Variables con >90% de valores nulos → muy baja capacidad predictiva
    "is_cpo",
    "is_oemcpo",

    // Variable inconsistente como indicador de pickup
    // (se sustituye por la feature derivada is_pickup)
    "isCab"
    //columnas relacionadas con potencia, torque y cilindros del motor que son string con formato inconsistente 
    //y que se procesaron por separado para extraer su parte numérica
    ,"power", "torque", "engine_cylinders",
    // Variable con alto porcentaje de nulos que se habtratado y transformado en una nueva feature owner_count_missing 
    //  por lo que se elimina la original para evitar redundancia
    "owner_count","trimId","listed_date"

  ).filter(df.columns.contains)

  df.drop(colsToDrop: _*)
}

}
