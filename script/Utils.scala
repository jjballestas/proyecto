import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)
 


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
 
 
def resumenCategoricoTabular(df: DataFrame, cols: Seq[String]): Unit = {
  val nTotal = df.count().toDouble

  val resumen = cols.map { c =>
    val distinctCount = df.select(col(c)).distinct().count()

    val nullCount = df.filter(col(c).isNull).count()
    val nullPct = if (nTotal > 0) (nullCount * 100.0) / nTotal else 0.0

    val topRow = df.groupBy(col(c)).count().orderBy(desc("count")).first()

    val topValue =
      if (topRow.isNullAt(0)) "null"
      else Option(topRow.get(0)).map(_.toString).getOrElse("null")

    val topCount = topRow.getLong(1)
    val topPct = if (nTotal > 0) (topCount * 100.0) / nTotal else 0.0

    (
      c,distinctCount,nullCount,nullPct,topValue,topCount,topPct
    )
  }

  val resumenDF = resumen.toDF("feature","cardinality","nulls","null_pct","top_category","top_count","top_pct")

  println("\nResumen de variables categóricas:\n")
  println("-" * 130)
  println(f"${"feature"}%-20s ${"cardinality"}%-12s ${"nulls"}%-10s ${"null_pct"}%-10s ${"top_category"}%-30s ${"top_count"}%-12s ${"top_pct"}%-10s")
  println("-" * 130)
  resumenDF.collect().foreach { row =>
    val feature      = row.getAs[String]("feature")
    val cardinality  = row.getAs[Long]("cardinality")
    val nulls        = row.getAs[Long]("nulls")
    val nullPct      = row.getAs[Double]("null_pct")
    val topCategory  = row.getAs[String]("top_category")
    val topCount     = row.getAs[Long]("top_count")
    val topPct       = row.getAs[Double]("top_pct")

    println(
      f"$feature%-20s $cardinality%-12d $nulls%-10d ${nullPct}%-10.2f ${topCategory.take(28)}%-30s $topCount%-12d ${topPct}%-10.2f"
    )
  }

    println("-" * 130)
}
 
def resumenBooleanasTabular(df: DataFrame, cols: Seq[String]): Unit = {
  val nTotal = df.count().toDouble

  println(f"${"feature"}%-20s ${"nulls"}%-10s ${"null_pct"}%-10s ${"true_count"}%-12s ${"true_pct"}%-10s ${"false_count"}%-12s ${"false_pct"}%-10s")
  println("-" * 130)

  cols.foreach { c =>
    val nulls = df.filter(col(c).isNull).count()
    val trueCount = df.filter(col(c) === true).count()
    val falseCount = df.filter(col(c) === false).count()

    val nullPct = if (nTotal > 0) nulls * 100.0 / nTotal else 0.0
    val truePct = if (nTotal > 0) trueCount * 100.0 / nTotal else 0.0
    val falsePct = if (nTotal > 0) falseCount * 100.0 / nTotal else 0.0

    println(
      f"$c%-20s $nulls%-10d ${nullPct}%-10.2f $trueCount%-12d ${truePct}%-10.2f $falseCount%-12d ${falsePct}%-10.2f"
    )
  }

  println("-" * 130)
}

 
def resumenTextoTabular(df: DataFrame, cols: Seq[String], nEjemplos: Int = 3): Unit = {
  val nTotal = df.count().toDouble

  println("\n Resumen de variables de texto no estructurado:\n")
   
  println(f"${"feature"}%-20s ${"nulls"}%-10s ${"null_pct"}%-10s ${"mean_len"}%-12s ${"median_len"}%-12s ${"max_len"}%-10s")
  println("-" * 130)

  cols.foreach { c =>
    val dfTexto = df.withColumn(
      "_texto_limpio",
      trim(coalesce(col(c).cast("string"), lit("")))
    )

    val nulls = dfTexto.filter(col("_texto_limpio") === "").count()
    val nullPct = if (nTotal > 0) nulls * 100.0 / nTotal else 0.0

    val dfNoVacio = dfTexto.filter(col("_texto_limpio") =!= "").withColumn("_len", length(col("_texto_limpio")))

    val stats = dfNoVacio.agg(avg(col("_len")).alias("mean_len"),max(col("_len")).alias("max_len")).first()

    val meanLen =if (stats == null || stats.isNullAt(0)) 0.0 else stats.getAs[Double]("mean_len")

    val maxLen =if (stats == null || stats.isNullAt(1)) 0 else stats.getAs[Int]("max_len")

    val medianArr = dfNoVacio.stat.approxQuantile("_len", Array(0.5), 0.01)
    val medianLen = if (medianArr.nonEmpty) medianArr(0) else 0.0

    println(
      f"$c%-20s $nulls%-10d ${nullPct}%-10.2f ${meanLen}%-12.2f ${medianLen}%-12.2f $maxLen%-10d"
    )

    val ejemplos = dfNoVacio.select("_texto_limpio").limit(nEjemplos).collect().map(_.getString(0)).zipWithIndex

    println(s"  Ejemplos de $c:")
    if (ejemplos.isEmpty) {
      println("    - Sin ejemplos no vacíos")
    } else {
      ejemplos.foreach { case (txt, i) =>
        val corto = if (txt.length > 120) txt.take(120) + "..." else txt
        println(s"    ${i + 1}. $corto")
      }
    }
    println("-" * 130)
  }
}


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

def resumenMalTipadasTabular(df: DataFrame, cols: Seq[String], nEjemplos: Int = 3): Unit = {
  val nTotal = df.count().toDouble

  println("\n Resumen de variables numéricas mal tipadas como texto:\n")
  println("-" * 120)
  println(f"${"feature"}%-20s ${"nulls"}%-10s ${"null_pct"}%-10s ${"con_num"}%-10s ${"con_num_pct"}%-14s ${"sin_num"}%-10s ${"sin_num_pct"}%-14s")
  println("-" * 120)

  cols.foreach { c =>
    val base = df.withColumn("_raw", trim(coalesce(col(c).cast("string"), lit(""))))
    .withColumn("_num_txt", regexp_extract(col("_raw"), """(-?\d+(?:\.\d+)?)""", 1))
    .withColumn("_has_num", col("_num_txt") =!= "")

    val nNulls = base.filter(col("_raw") === "").count()
    val nConNum = base.filter(col("_has_num")).count()
    val nSinNum = nTotal.toLong - nNulls - nConNum

    val pctNulls = if (nTotal > 0) nNulls * 100.0 / nTotal else 0.0
    val pctConNum = if (nTotal > 0) nConNum * 100.0 / nTotal else 0.0
    val pctSinNum = if (nTotal > 0) nSinNum * 100.0 / nTotal else 0.0

    println(
      f"$c%-20s $nNulls%-10d ${pctNulls}%-10.2f $nConNum%-10d ${pctConNum}%-14.2f $nSinNum%-10d ${pctSinNum}%-14.2f"
    )

    val ejemplos = base.filter(col("_raw") =!= "").select("_raw", "_num_txt").limit(nEjemplos).collect()

    println(s"  Ejemplos de $c:")
    if (ejemplos.isEmpty) {
      println("    - Sin ejemplos no vacíos")
    } else {
      ejemplos.zipWithIndex.foreach { case (row, i) =>
        val raw = row.getAs[String]("_raw")
        val numTxt = row.getAs[String]("_num_txt")
        val rawShort = if (raw.length > 80) raw.take(80) + "..." else raw
        val numShow = if (numTxt == null || numTxt.isEmpty) "NO_EXTRAIBLE" else numTxt
        println(s"    ${i + 1}. original='$rawShort'  ->  extraido='$numShow'")
      }
    }

    println("-" * 120)
  }
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


  import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

def resumenGeografico(
    df: DataFrame,
    latCol: String = "latitude",
    lonCol: String = "longitude",
    cityCol: String = "city",
    zipCol:  String = "dealer_zip"
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

//esta funcion muestra una tabla con un número específico de filas, seleccionando columnas relevantes para un dataset de autos usados, como "vin", "make_name", "model_name", "year", "price", "mileage", "body_type
    def showTable(df: DataFrame, numRows: Int): Unit = {
      val colsBonitas = Seq( "vin", "make_name", "model_name", "year", "price","mileage", "body_type", "fuel_type", "transmission_display").filter(df.columns.contains)
      df.select(colsBonitas.head, colsBonitas.tail: _*).show(numRows, truncate = 25)
      }

//esta función realiza un análisis exploratorio de datos (EDA) específico para la variable objetivo "price",
 
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
  def analyzeNumericPercentiles(
    df: DataFrame,
    numericCol: String,
    positiveOnly: Boolean = false,
    probs: Seq[Double] = Seq(0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.999)
): DataFrame = {
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
    (quantiles(0), quantiles(1))  
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

def filterPriceErrors(df: DataFrame): DataFrame = {
  val marcasMasivas = Seq("Ford","Chevrolet","Nissan","Dodge","Toyota","Honda","Hyundai","Kia","GMC","Jeep","RAM","Buick")

  // p99 por marca — solo sobre marcas masivas
  val p99PorMarca = df.filter(col("make_name").isin(marcasMasivas: _*)).groupBy("make_name").agg(expr("percentile_approx(price, 0.99) as p99_price"))

  val dfFiltrado = df.join(p99PorMarca, Seq("make_name"), "left").filter(col("p99_price").isNull ||col("price") <= col("p99_price")).drop("p99_price")

  val eliminadas = df.count() - dfFiltrado.count()
  println(s"  [filterPriceErrors] registros eliminados: $eliminadas")

  dfFiltrado
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
  //creando una nueva columna que indica si el valor original estaba ausente y rellenando los nulos con 0 
  //para mantener la información de ausencia sin perder registros
  def treatOwnerCount(df: DataFrame): DataFrame = {
    df.withColumn("owner_count_missing", col("owner_count").isNull.cast("int"))
      .withColumn("owner_count", when(col("owner_count").isNull, 0).otherwise(col("owner_count")))
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

    imputeNumericBlockByGroupWithGlobalFallback(df1,econCols,Seq("fuel_type_clean", "body_type", "is_new")
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
    
//esta función crea una nueva columna de densidad de potencia (horsepower por litro de desplazamiento) 
//a partir de las columnas de potencia
// y desplazamiento del motor.
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
    
  

// Coches pre-1980 tienen lógica de precio diferente por su valor histórico y coleccionista, 
// así que añadimos una flag para identificarlos y tratarlos aparte 
def addIsClassicFlag(df: DataFrame): DataFrame = {
  df.withColumn("is_classic", when(col("year") < 1980, 1).otherwise(0))
}


def prepararDataset(spark: SparkSession,df: DataFrame,path: String,forcePreprocess: Boolean = false
): DataFrame = {

  val fs              = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val finalPath       = path + "dataset/parquet/dataset_final"
  val preImputPath    = path + "dataset/parquet/pre_imputation"
  val finalHdfsPath   = new org.apache.hadoop.fs.Path(finalPath)

  // ── Si no se fuerza y existe el parquet final → carga directa ──
  if (!forcePreprocess && fs.exists(finalHdfsPath)) {
    println("  ✅ Cargando dataset_final existente desde disco...")
    val dfCargado = spark.read.parquet(finalPath)
    println(s"  ✅ Dataset cargado: ${dfCargado.count()} registros | ${dfCargado.columns.length} columnas")
    return dfCargado
  }

  // ── Preprocesado completo ───────────────────────────────────────
  if (forcePreprocess)
    println("  🔄 forcePreprocess=true — regenerando dataset completo...")
  else
    println("  🔄 dataset_final no encontrado — ejecutando preprocesado...")

  // Fase 1: limpieza y feature engineering
  val colsToDrop = Seq("combine_fuel_economy","is_certified","vehicle_damage_category",
    "vin","listing_id","sp_id","main_picture_url","description","transmission_display",
    "wheel_system_display","listing_color","dealer_zip","sp_name",
    "bed","cabin","is_cpo","is_oemcpo","isCab")

  var dfWork = dropColumns(df, colsToDrop)
  dfWork = addGamaAlta(dfWork)
  dfWork = addIsPickupAndClean(dfWork)
  dfWork = cleanFuelType(dfWork)
  dfWork = extractStringNumericFeatures(dfWork)
  dfWork = addGeographicFeatures(dfWork)
  dfWork = addTemporalFeatures(dfWork)
  dfWork = fillBooleanAsCategory(dfWork, Seq("fleet","frame_damaged","has_accidents","salvage","theft_title"))
  dfWork = treatOwnerCount(dfWork)
  dfWork = treatMileageOutliers(dfWork)
  dfWork = treatDaysOnMarket(dfWork)
  dfWork = treatSavingsAmount(dfWork, mode = "drop")

  // Fase 2: corte de linaje
  dfWork.write.mode("overwrite").parquet(preImputPath)
  var dfImp = spark.read.parquet(preImputPath)

  // Fase 3: imputación y features finales
  dfImp = imputeDimensions(dfImp)
  dfImp = imputeEngineSpecs(dfImp)
  dfImp = imputeFuelEconomy(dfImp)
  dfImp = imputeRemainingNumeric(dfImp)
  dfImp = addPowerDensity(dfImp)
  dfImp = filterPriceErrors(dfImp)
  dfImp = addIsClassicFlag(dfImp)
  dfImp = fillCategoricalUnknown(dfImp, Seq("interior_color","body_type","engine_type",
    "franchise_make","transmission","trim_name","wheel_system"))

  val colsToDropPost = Seq("engine_displacement","city_fuel_economy","power","torque",
    "engine_cylinders","trimId","listed_date","bed_height","bed_length","major_options")
  dfImp = dropColumns(dfImp, colsToDropPost)
  dfImp = transformTargetToLog(dfImp)

  // Fase 4: filtrar clásicos y persistir
  val dfFinal = dfImp.filter(col("is_classic") === 0)
  dfFinal.write.mode("overwrite").parquet(finalPath)
  println(s"  ✅ Dataset final guardado: ${dfFinal.count()} registros | ${dfFinal.columns.length} columnas")

  // Fase 5: eliminar pre_imputation
  val preImputHdfsPath = new org.apache.hadoop.fs.Path(preImputPath)
  if (fs.exists(preImputHdfsPath)) {
    fs.delete(preImputHdfsPath, true)
    println("  🗑  pre_imputation eliminado")
  }

  dfFinal
}


def mostrarResumenFinal(df: DataFrame): Unit = {
  val total     = df.count()
  val sepAncho  = 70

  // esta función auxiliar imprime una secuencia de strings en columnas formateadas 
  // mejora la legibilidad del resumen final
def imprimirEnColumnas(cols: Seq[String], colWidth: Int = 30, colsPorFila: Int = 3): Unit = {
  cols.grouped(colsPorFila).foreach { grupo =>
    println("     " + grupo.map(c => c.padTo(colWidth, ' ')).mkString("  "))
  }
}

  println("\n" + "=" * sepAncho)
  println("  RESUMEN DATASET FINAL")
  println("=" * sepAncho)

  // ── Dimensiones ───────────────────────────────────────────
  println(f"\n  📌 Registros : $total%,d")
  println(f"  📌 Columnas  : ${df.columns.length}%d")

  // ── Tipos de columnas ─────────────────────────────────────
  val porTipo = df.dtypes.groupBy(_._2).mapValues(_.length)
  println(s"\n  📌 Tipos:")
  porTipo.toSeq.sortBy(_._1).foreach { case (tipo, n) =>
    val tipoCorto = tipo.replace("Type", "")
    println(f"     $tipoCorto%-15s $n%d columnas")
  }

  // ── Nulos ─────────────────────────────────────────────────
  val nullExprs = df.dtypes.map { case (colName, dataType) =>
    val cond = dataType match {
      case "DoubleType" | "FloatType" => col(colName).isNull || col(colName).isNaN
      case "StringType"               => col(colName).isNull || trim(col(colName)) === ""
      case _                          => col(colName).isNull
    }
    sum(when(cond, 1).otherwise(0)).alias(colName)
  }
  val nullRow     = df.select(nullExprs: _*).head()
  val colsConNull = df.columns.zipWithIndex
    .map { case (c, i) => (c, nullRow.getLong(i)) }
    .filter(_._2 > 0)

  if (colsConNull.isEmpty)
    println("\n  ✅ Nulls: 0 en todas las columnas")
  else {
    println(s"\n  ⚠️  Columnas con nulls: ${colsConNull.length}")
    colsConNull.foreach { case (c, n) =>
      val pct = n.toDouble / total * 100
      println(f"     $c%-35s $n%,d ($pct%.2f%%)")
    }
  }

  // ── Variable objetivo ─────────────────────────────────────
  if (df.columns.contains("log_price")) {
    val row = df.select(
      min("log_price"), max("log_price"),
      avg("log_price"),
      expr("percentile_approx(log_price, 0.5)"),
      skewness("log_price")
    ).head()
    println("\n  📌 Target — log_price:")
    println(f"     min    : ${row.getDouble(0)}%.4f")
    println(f"     max    : ${row.getDouble(1)}%.4f")
    println(f"     media  : ${row.getDouble(2)}%.4f")
    println(f"     mediana: ${row.getDouble(3)}%.4f")
    println(f"     skew   : ${row.getDouble(4)}%.4f")
  }

  // ── Columnas categóricas ──────────────────────────────────
  val catCols = df.dtypes.filter(_._2 == "StringType").map(_._1).sorted
  println(s"\n  📌 Variables categóricas (${catCols.length}):")
  println("     " + "-" * (sepAncho - 5))
  imprimirEnColumnas(catCols, colWidth = 25, colsPorFila = 3)

  // ── Columnas numéricas — separadas por grupo ──────────────
  val numCols = df.dtypes
    .filter { case (_, t) => t == "DoubleType" || t == "IntegerType" || t == "FloatType" }
    .map(_._1)
    .filterNot(_ == "log_price")

  // Separar por sufijo para agrupar visualmente
  val missingCols  = numCols.filter(_.endsWith("_missing")).sorted
  val featureCols  = numCols.filterNot(_.endsWith("_missing")).sorted

  println(s"\n  📌 Variables numéricas — features (${featureCols.length}):")
  println("     " + "-" * (sepAncho - 5))
  imprimirEnColumnas(featureCols, colWidth = 28, colsPorFila = 3)

  println(s"\n  📌 Variables numéricas — flags missing (${missingCols.length}):")
  println("     " + "-" * (sepAncho - 5))
  imprimirEnColumnas(missingCols, colWidth = 32, colsPorFila = 2)

  println("\n" + "=" * sepAncho + "\n")
}
//*******************************************************************************************
//  FIN DE LA FASE DE PREPROCESADO COMPLETO
//*******************************************************************************************

def cargarOPrepararDataset(spark: SparkSession,path: String,rawData: String,
rawParquet: String,forceCreateParquet: Boolean = false,forcePreprocess: Boolean = false): DataFrame = {

  val fs            = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val finalPath     = path + "dataset/parquet/dataset_final"
  val finalHdfsPath = new org.apache.hadoop.fs.Path(finalPath)

  if (!forcePreprocess && fs.exists(finalHdfsPath)) { 
    println(s"  ✅ Cargando dataset_final desde disco...")
    val df = spark.read.parquet(finalPath)
    println(s"  ✅ Dataset cargado: ${df.count()} registros | ${df.columns.length} columnas")
    df

  } else {
     
    if (forcePreprocess)
      println("  🔄 forcePreprocess=true — regenerando desde raw data...")
    else
      println("  🔄 dataset_final no encontrado — ejecutando pipeline completo...")

    val dfRaw = loadDataParquet(spark, path, rawData, rawParquet, forceCreateParquet)
    prepararDataset(spark, dfRaw, path, forcePreprocess)
  }
}


def crearSubconjuntoControlado(
    df: DataFrame,
    targetSize: Int = 400000,
    seed: Long = 42L,
    minRowsPerStratum: Int = 500
): DataFrame = {

  require(df.columns.contains("year"), "La columna 'year' es obligatoria.")
  require(df.columns.contains("body_type"), "La columna 'body_type' es obligatoria.")
  require(df.columns.contains("log_price"), "La columna 'log_price' es obligatoria.")

  println("\n==================== SUBCONJUNTO CONTROLADO ====================\n")
  println(s"📌 Tamaño objetivo aproximado : $targetSize")
  println(s"📌 Seed                       : $seed")
  println(s"📌 Mínimo por estrato         : $minRowsPerStratum")

  // ---------------------------------------------------------
  // 1. Filtro básico de seguridad
  // ---------------------------------------------------------
  val dfBase = df
    .filter(col("log_price").isNotNull)
    .filter(col("year").isNotNull)

  val totalRows = dfBase.count()
  println(s"📌 Filas válidas de entrada   : $totalRows")

  if (targetSize >= totalRows) {
    println("✅ El tamaño objetivo es mayor o igual que el dataset. Se devuelve el dataset completo.\n")
    return dfBase
  }

  // ---------------------------------------------------------
  // 2. Crear bins temporales y estrato
  // ---------------------------------------------------------
  val dfStrata = dfBase
    .withColumn(
      "year_bin",
      when(col("year") >= 2019, "recent")
        .when(col("year") >= 2014, "mid")
        .otherwise("old")
    )
    .withColumn(
      "body_type_stratum",
      coalesce(trim(col("body_type")), lit("unknown"))
    )
    .withColumn(
      "stratum",
      concat_ws("_", col("year_bin"), col("body_type_stratum"))
    )

  // ---------------------------------------------------------
  // 3. Recuento por estrato
  // ---------------------------------------------------------
  val strataCounts = dfStrata
    .groupBy("stratum")
    .count()
    .cache()

  val numStrata = strataCounts.count()
  val baseFraction = targetSize.toDouble / totalRows.toDouble

  println(s"📌 Número de estratos         : $numStrata")
  println(f"📌 Fracción base              : $baseFraction%.6f")

  // ---------------------------------------------------------
  // 4. Construir fracciones por estrato
  //    Regla:
  //    - estratos grandes: fracción base
  //    - estratos pequeños: intentar preservar al menos minRowsPerStratum
  // ---------------------------------------------------------
  val fractions: Map[String, Double] = strataCounts
    .collect()
    .map { row =>
      val stratum = row.getAs[String]("stratum")
      val count = row.getAs[Long]("count")

      val fraction =
        if (count <= minRowsPerStratum) 1.0
        else math.min(1.0, math.max(baseFraction, minRowsPerStratum.toDouble / count.toDouble))

      stratum -> fraction
    }
    .toMap

  println(s"📌 Estratos con fracción calc.: ${fractions.size}")

  // ---------------------------------------------------------
  // 5. Muestreo estratificado
  // ---------------------------------------------------------
  val sampled = dfStrata
    .stat
    .sampleBy("stratum", fractions, seed)

  val sampledCount = sampled.count()
  println(s"📌 Filas tras sampleBy        : $sampledCount")

  // ---------------------------------------------------------
  // 6. Ajuste fino del tamaño
  //    Si se pasa del objetivo, recorte aleatorio reproducible.
  // ---------------------------------------------------------
  val dfFinal =
    if (sampledCount > targetSize) {
      val ratio = targetSize.toDouble / sampledCount.toDouble
      println(f"📌 Ajuste fino adicional      : ratio = $ratio%.6f")
      sampled.sample(withReplacement = false, ratio, seed + 1L)
    } else {
      sampled
    }

  val result = dfFinal
    .drop("year_bin", "body_type_stratum", "stratum")

  val finalCount = result.count()
  println(s"✅ Tamaño final aproximado    : $finalCount")
  println("\n===============================================================\n")

  strataCounts.unpersist()

  result
}
def crearOCargarSplit(
    spark: SparkSession,
    df: DataFrame,
    trainPath: String,
    testPath: String,
    trainExiste: Boolean,
    testExiste: Boolean,
    forceSplit: Boolean = false,
    trainRatio: Double = 0.8,
    seed: Long = 42L
): (DataFrame, DataFrame) = {

  val (dfTrain, dfTest) = if (!forceSplit && trainExiste && testExiste) {
    println("  ✅ Cargando train/test desde disco...")
    (spark.read.parquet(trainPath), spark.read.parquet(testPath))
  } else {
    println("  🔄 Generando split train/test...")
    val Array(tr, te) = df.randomSplit(Array(trainRatio, 1 - trainRatio), seed = seed)
    tr.write.mode("overwrite").parquet(trainPath)
    te.write.mode("overwrite").parquet(testPath)
    println("  ✅ Train y Test guardados en parquet")
    (spark.read.parquet(trainPath), spark.read.parquet(testPath))
  }

  val nTrain = dfTrain.count()
  val nTest  = dfTest.count()
  val nTotal = nTrain + nTest

  println(f"\n  📌 Train : $nTrain%,d registros (${nTrain.toDouble / nTotal * 100}%.1f%%)")
  println(f"  📌 Test  : $nTest%,d registros (${nTest.toDouble / nTotal * 100}%.1f%%)")

  (dfTrain, dfTest)
}

def evaluarModelo(nombre: String,modelo: org.apache.spark.ml.PipelineModel,dfTe: DataFrame,
evaluador: org.apache.spark.ml.evaluation.RegressionEvaluator,dfTr: Option[DataFrame] = None
): Unit = {

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

 
def mostrarResultadosCV(
    nombre: String,
    cvModel: org.apache.spark.ml.tuning.CrossValidatorModel,
    paramGrid: Array[org.apache.spark.ml.param.ParamMap]
): Unit = {
  val avg = cvModel.avgMetrics

  // Limpiar parámetros: eliminar UID, formatear floats, poner en una línea
  def limpiarParams(pm: org.apache.spark.ml.param.ParamMap): String = {
    pm.toSeq
      .map { pair =>
        val nombre = pair.param.name  // solo el nombre sin UID
        val valor  = pair.value match {
          case d: Double => f"$d%.4f".replaceAll("\\.?0+$", "")  // quitar ceros trailing
          case other     => other.toString
        }
        s"$nombre=$valor"
      }
      .mkString("  ")  // separar parámetros con dos espacios
  }

  println(s"\n  ══════════════════════════════════════════════════════════")
  println(s"  RESULTADOS CV — $nombre")
  println(s"  ══════════════════════════════════════════════════════════")
  println(f"  ${"#"}%-4s ${"Parámetros"}%-45s ${"RMSE_CV"}%10s")
  println("  " + "-" * 62)

  paramGrid.zip(avg)
    .sortBy(_._2)
    .zipWithIndex
    .foreach { case ((pm, rmse), idx) =>
      println(f"  ${idx + 1}%-4d ${limpiarParams(pm)}%-45s $rmse%10.4f")
    }

  println(f"\n  ✅ Mejor RMSE CV : ${avg.min}%.4f")
  println(f"  ⚠️  Peor  RMSE CV : ${avg.max}%.4f")
  println(f"  📌 Mejora vs peor : ${avg.max - avg.min}%.4f")
}



// ── Guardar resultados CV a CSV ───────────────────────────────
def guardarResultadosCV(spark: SparkSession,nombre: String,cvModel: org.apache.spark.ml.tuning.CrossValidatorModel,
paramGrid: Array[org.apache.spark.ml.param.ParamMap],path: String): Unit = {
  import spark.implicits._

  val rows = paramGrid.zip(cvModel.avgMetrics).map { case (pm, rmse) =>
      (pm.toString.replaceAll("[{}]","").trim, rmse)
    }.toSeq

  spark.createDataFrame(rows).toDF("params","rmse_cv").orderBy(col("rmse_cv").asc).coalesce(1)
  .write.mode("overwrite").option("header","true").csv(path)

  println(s"  ✅ Resultados CV $nombre guardados en: $path")
}


def ejecutarBusquedaCV(spark: SparkSession,nombre: String,pipeline: org.apache.spark.ml.Pipeline,
paramGrid: Array[org.apache.spark.ml.param.ParamMap],dfTrain: DataFrame,evaluador: org.apache.spark.ml.evaluation.RegressionEvaluator,
numFolds: Int = 5,seed: Long = 42L,resultadosPath: String): org.apache.spark.ml.tuning.CrossValidatorModel = {

  val cv = new org.apache.spark.ml.tuning.CrossValidator().setEstimator(pipeline).setEvaluator(evaluador.setMetricName("rmse"))
  .setEstimatorParamMaps(paramGrid).setNumFolds(numFolds).setSeed(seed)

  println(s"\n  🔄 $nombre (${paramGrid.length} combinaciones x $numFolds folds)...")
  val cvModel = cv.fit(dfTrain)
  println(s"  ✅ $nombre completada")

  mostrarResultadosCV(nombre, cvModel, paramGrid)
  guardarResultadosCV(spark, nombre, cvModel, paramGrid, resultadosPath)

  cvModel
}


}
