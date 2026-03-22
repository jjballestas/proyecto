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
  Utils.showDF("price por geo_region", Utils.analyzeNumericByGeoRegion(df, "price", positiveOnly = true), n = 10)
  Utils.showDF("Resumen para decidir transformación de price", Utils.summarizePriceTransformationDecision(df))



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

 def imprimirDiccionarioAnalisis(df: DataFrame): Unit = {

  case class InfoColumna(
    significado: String,
    analizar: String,
    motivo: String,
    tratamiento: String
  )

  val meta = Map(
    "vin" -> InfoColumna(
      "Identificador único del vehículo (VIN).",
      "NO",
      "Es un identificador único; sirve para control de duplicados, no para explicar el precio.",
      "Usar para detectar duplicados y excluir del modelado."
    ),
    "back_legroom" -> InfoColumna(
      "Espacio para piernas en la parte trasera.",
      "SI",
      "Puede reflejar tamaño y segmento del vehículo y presenta un porcentaje de nulos relativamente moderado.",
      "Extraer valor numérico en pulgadas y revisar imputación."
    ),
    "bed" -> InfoColumna(
      "Tipo o tamaño de caja de carga en pickups.",
      "AMPLIAR",
      "Presenta un porcentaje altísimo de nulos y solo aplica a pickups.",
      "Mantener solo si se trabaja específicamente con pickups; en otro caso eliminar o recodificar como no aplica."
    ),
    "bed_height" -> InfoColumna(
      "Altura de la caja de carga.",
      "AMPLIAR",
      "Tiene demasiados nulos y solo aporta información en pickups.",
      "Extraer numérico y conservar solo si se mantiene un subanálisis específico para pickups."
    ),
    "bed_length" -> InfoColumna(
      "Longitud de la caja de carga.",
      "AMPLIAR",
      "Tiene demasiados nulos y solo es útil en pickups.",
      "Extraer numérico y conservar solo si aporta valor en ese segmento."
    ),
    "body_type" -> InfoColumna(
      "Tipo de carrocería del vehículo.",
      "SI",
      "Es una de las variables estructurales más importantes y tiene muy pocos nulos.",
      "Analizar distribución y relación con el precio; codificar como categórica."
    ),
    "cabin" -> InfoColumna(
      "Tipo de cabina en pickups.",
      "AMPLIAR",
      "Solo aplica a ciertos vehículos y presenta una ausencia masiva.",
      "Tratar como variable específica de pickups o eliminar en un modelado general."
    ),
    "city" -> InfoColumna(
      "Ciudad donde está anunciado el vehículo.",
      "AMPLIAR",
      "Puede capturar diferencias regionales, pero tendrá alta cardinalidad y posible ruido.",
      "Analizar cardinalidad; agrupar categorías frecuentes o eliminar si no aporta."
    ),
    "city_fuel_economy" -> InfoColumna(
      "Consumo en ciudad.",
      "SI",
      "Variable técnica útil para caracterizar el vehículo y con un volumen de datos válido suficiente.",
      "Analizar distribución, nulos y outliers; imputar si se mantiene."
    ),
    "combine_fuel_economy" -> InfoColumna(
      "Consumo combinado.",
      "NO",
      "La columna está completamente vacía en el dataset actual.",
      "Eliminar del análisis y del modelado."
    ),
    "daysonmarket" -> InfoColumna(
      "Días que lleva publicado el anuncio.",
      "SI",
      "Puede relacionarse con el precio, la demanda y el ajuste comercial del vehículo.",
      "Analizar distribución, outliers y relación con price."
    ),
    "dealer_zip" -> InfoColumna(
      "Código postal del concesionario.",
      "AMPLIAR",
      "Es una variable geográfica, no una magnitud continua, y en el esquema real aparece como string.",
      "Usar solo para agregación geográfica o eliminar del modelado base."
    ),
    "description" -> InfoColumna(
      "Descripción libre del anuncio.",
      "AMPLIAR",
      "Es texto libre con potencial valor, pero no conviene incluirla en un EDA tabular base.",
      "Excluir del modelado base y reservar para NLP o ingeniería de variables derivadas."
    ),
    "engine_cylinders" -> InfoColumna(
      "Configuración del motor por cilindros.",
      "SI",
      "Variable mecánica relevante con pocos nulos relativos.",
      "Normalizar etiquetas y analizar como categórica."
    ),
    "engine_displacement" -> InfoColumna(
      "Cilindrada del motor.",
      "SI",
      "Variable técnica importante y potencialmente muy relacionada con el precio.",
      "Analizar distribución, nulos y outliers; imputar si se mantiene."
    ),
    "engine_type" -> InfoColumna(
      "Tipo o configuración del motor.",
      "SI",
      "Aporta información mecánica útil y puede complementar o redundar con engine_cylinders.",
      "Analizar como categórica y revisar redundancia."
    ),
    "exterior_color" -> InfoColumna(
      "Color exterior detallado.",
      "AMPLIAR",
      "Puede tener mucha cardinalidad y nombres comerciales muy dispersos.",
      "Normalizar categorías o preferir listing_color para un modelado más simple."
    ),
    "fleet" -> InfoColumna(
      "Indica si el vehículo perteneció a una flota.",
      "AMPLIAR",
      "Puede ser relevante, pero tiene un volumen muy alto de nulos.",
      "Imputar como desconocido o evaluar eliminación según el enfoque del modelo."
    ),
    "frame_damaged" -> InfoColumna(
      "Indica si el chasis está dañado.",
      "AMPLIAR",
      "Es relevante para el estado del vehículo, pero presenta muchos valores nulos.",
      "Imputar como desconocido o mantener como categoría separada."
    ),
    "franchise_dealer" -> InfoColumna(
      "Indica si el vendedor es concesionario oficial.",
      "SI",
      "No tiene nulos y puede influir en confianza, garantía y precio.",
      "Usar como booleana."
    ),
    "franchise_make" -> InfoColumna(
      "Marca asociada a la franquicia del concesionario.",
      "AMPLIAR",
      "Puede resultar redundante con make_name y tiene un número importante de nulos.",
      "Revisar redundancia antes de decidir si se conserva."
    ),
    "front_legroom" -> InfoColumna(
      "Espacio para piernas delantero.",
      "SI",
      "Puede reflejar tamaño interior y categoría del vehículo.",
      "Extraer valor numérico en pulgadas."
    ),
    "fuel_tank_volume" -> InfoColumna(
      "Capacidad del tanque de combustible.",
      "SI",
      "Es una variable física útil para caracterizar el vehículo.",
      "Extraer valor numérico e imputar si procede."
    ),
    "fuel_type" -> InfoColumna(
      "Tipo de combustible.",
      "SI",
      "Muy relevante para segmentación y precio, con distribución interpretable.",
      "Analizar como categórica; agrupar categorías muy raras si hace falta."
    ),
    "has_accidents" -> InfoColumna(
      "Indica si el vehículo tiene accidentes registrados.",
      "AMPLIAR",
      "Es una variable muy importante, pero con gran cantidad de nulos.",
      "Imputar como desconocido o mantener categoría separada."
    ),
    "height" -> InfoColumna(
      "Altura del vehículo.",
      "AMPLIAR",
      "Puede aportar valor físico, pero requiere limpieza textual.",
      "Extraer numérico y evaluar su utilidad real."
    ),
    "highway_fuel_economy" -> InfoColumna(
      "Consumo en carretera.",
      "SI",
      "Variable técnica relevante con volumen de datos suficiente.",
      "Analizar distribución, nulos y outliers."
    ),
    "horsepower" -> InfoColumna(
      "Potencia del motor en HP.",
      "SI",
      "Variable clave para prestaciones y precio.",
      "Analizar distribución, outliers y relación con price."
    ),
    "interior_color" -> InfoColumna(
      "Color interior.",
      "AMPLIAR",
      "Tiene nulos casi inexistentes, pero mantiene alta cardinalidad y variantes inconsistentes.",
      "Normalizar etiquetas y agrupar categorías raras."
    ),
    "isCab" -> InfoColumna(
      "Indica si fue taxi o cab.",
      "AMPLIAR",
      "Puede reflejar un uso intensivo, pero presenta muchos nulos.",
      "Imputar como desconocido o evaluar si compensa conservarla."
    ),
    "is_certified" -> InfoColumna(
      "Indica si el vehículo está certificado.",
      "NO",
      "La columna está completamente vacía en el dataset actual.",
      "Eliminar del análisis y del modelado."
    ),
    "is_cpo" -> InfoColumna(
      "Indica si es certified pre-owned por concesionario.",
      "AMPLIAR",
      "Puede influir en confianza y precio, pero presenta una ausencia muy alta.",
      "Mantener solo si se decide imputar como desconocido; en otro caso evaluar eliminación."
    ),
    "is_new" -> InfoColumna(
      "Indica si el vehículo es nuevo o muy reciente.",
      "SI",
      "No tiene nulos y puede alterar de forma importante la distribución del precio.",
      "Usar como booleana y analizar su efecto sobre price."
    ),
    "is_oemcpo" -> InfoColumna(
      "Indica si está certificado por el fabricante.",
      "AMPLIAR",
      "Podría ser útil, pero presenta demasiados nulos.",
      "Imputar como desconocido o eliminar si no aporta."
    ),
    "latitude" -> InfoColumna(
      "Latitud del concesionario.",
      "AMPLIAR",
      "Útil para información geográfica, pero no suele aportar valor directo por sí sola.",
      "Usar junto con longitude o derivar región/estado."
    ),
    "length" -> InfoColumna(
      "Longitud del vehículo.",
      "SI",
      "Medida física relevante para segmentación del vehículo.",
      "Extraer valor numérico."
    ),
    "listed_date" -> InfoColumna(
      "Fecha en que el anuncio fue publicado.",
      "SI",
      "Puede aportar estacionalidad y antigüedad del anuncio.",
      "Derivar variables temporales como año, mes o antigüedad."
    ),
    "listing_color" -> InfoColumna(
      "Grupo dominante del color exterior.",
      "SI",
      "Es una versión más compacta y tratable que exterior_color.",
      "Analizar como categórica y preferirla sobre exterior_color en el modelado base."
    ),
    "listing_id" -> InfoColumna(
      "Identificador único del anuncio.",
      "NO",
      "Es un identificador técnico.",
      "Usar solo para control de registros; excluir del modelado."
    ),
    "longitude" -> InfoColumna(
      "Longitud del concesionario.",
      "AMPLIAR",
      "Útil para geolocalización, pero no suele aportar señal directa por sí sola.",
      "Usar junto con latitude o derivar región."
    ),
    "main_picture_url" -> InfoColumna(
      "URL de la imagen principal del anuncio.",
      "NO",
      "No aporta valor en un EDA tabular ni en un modelado base.",
      "Eliminar del análisis y del modelado."
    ),
    "major_options" -> InfoColumna(
      "Listado de equipamientos u opciones principales.",
      "AMPLIAR",
      "Puede ser útil, pero suele venir como texto o lista compleja.",
      "Contar opciones o extraer indicadores binarios si se decide explotar esta información."
    ),
    "make_name" -> InfoColumna(
      "Marca del vehículo.",
      "SI",
      "Variable comercial clave con gran poder explicativo sobre el precio.",
      "Analizar distribución y codificar como categórica."
    ),
    "maximum_seating" -> InfoColumna(
      "Número máximo de plazas.",
      "SI",
      "Puede reflejar tamaño y tipo de vehículo.",
      "Extraer valor numérico."
    ),
    "mileage" -> InfoColumna(
      "Kilometraje del vehículo.",
      "SI",
      "Es una de las variables más importantes para explicar el precio.",
      "Analizar distribución, outliers y relación con price."
    ),
    "model_name" -> InfoColumna(
      "Modelo del vehículo.",
      "SI",
      "Variable comercial importante, aunque con alta cardinalidad.",
      "Analizar frecuencia y valorar agrupación o recorte de cardinalidad."
    ),
    "owner_count" -> InfoColumna(
      "Número de propietarios anteriores.",
      "AMPLIAR",
      "Puede afectar depreciación y confianza, pero presenta muchos nulos.",
      "Imputar o tratar como discreta si se conserva."
    ),
    "power" -> InfoColumna(
      "Potencia expresada en texto.",
      "AMPLIAR",
      "Puede duplicar información de horsepower y requiere limpieza textual.",
      "Extraer valor numérico o eliminar si resulta redundante."
    ),
    "price" -> InfoColumna(
      "Precio del vehículo.",
      "SI",
      "Es la variable objetivo principal del problema de regresión.",
      "Analizar distribución, sesgo, outliers y posible log-transform."
    ),
    "salvage" -> InfoColumna(
      "Indica si el vehículo tiene título salvage.",
      "AMPLIAR",
      "Es relevante a nivel legal y comercial, pero presenta muchos nulos.",
      "Imputar como desconocido o evaluar conservación según la estrategia."
    ),
    "savings_amount" -> InfoColumna(
      "Ahorro estimado respecto a una referencia.",
      "AMPLIAR",
      "Puede ser útil, pero depende fuertemente de cómo se haya calculado.",
      "Revisar definición y posible riesgo de fuga antes de usar."
    ),
    "seller_rating" -> InfoColumna(
      "Valoración del vendedor.",
      "SI",
      "Tiene muy pocos nulos y puede influir en confianza y precio percibido.",
      "Analizar distribución e imputar los pocos faltantes si se mantiene."
    ),
    "sp_id" -> InfoColumna(
      "Identificador del vendedor o proveedor.",
      "NO",
      "Es un identificador técnico aunque en el esquema aparezca como double.",
      "Usar solo para trazabilidad y excluir del modelado."
    ),
    "sp_name" -> InfoColumna(
      "Nombre del vendedor o proveedor.",
      "AMPLIAR",
      "Puede tener cardinalidad alta y riesgo de fuga de información.",
      "Analizar frecuencia y decidir si agrupar o excluir."
    ),
    "theft_title" -> InfoColumna(
      "Indica si el título está asociado a robo.",
      "AMPLIAR",
      "Es relevante a nivel legal, pero presenta un gran volumen de nulos.",
      "Imputar como desconocido o evaluar eliminación."
    ),
    "torque" -> InfoColumna(
      "Par motor.",
      "AMPLIAR",
      "Es útil, pero suele venir en texto y requiere parsing.",
      "Extraer valor numérico si es viable; en caso contrario eliminar."
    ),
    "transmission" -> InfoColumna(
      "Tipo de transmisión.",
      "SI",
      "Variable técnica y comercial importante.",
      "Analizar como categórica."
    ),
    "transmission_display" -> InfoColumna(
      "Descripción mostrada de la transmisión.",
      "AMPLIAR",
      "Puede ser redundante con transmission, aunque aporta mayor detalle.",
      "Comparar ambas y conservar la más útil."
    ),
    "trimId" -> InfoColumna(
      "Identificador del acabado o versión.",
      "NO",
      "Parece identificador técnico.",
      "Excluir del modelado y usar solo para trazabilidad."
    ),
    "trim_name" -> InfoColumna(
      "Nombre del acabado o versión.",
      "AMPLIAR",
      "Puede aportar detalle comercial, pero tiene cardinalidad elevada.",
      "Analizar frecuencia y decidir si agrupar o conservar."
    ),
    "vehicle_damage_category" -> InfoColumna(
      "Categoría de daño del vehículo.",
      "NO",
      "La columna está completamente vacía en el dataset actual.",
      "Eliminar del análisis y del modelado."
    ),
    "wheel_system" -> InfoColumna(
      "Sistema de tracción o ruedas.",
      "SI",
      "Variable técnica relevante para segmentación del vehículo.",
      "Analizar como categórica."
    ),
    "wheel_system_display" -> InfoColumna(
      "Descripción del sistema de tracción.",
      "AMPLIAR",
      "Parece muy próxima a wheel_system y puede ser redundante.",
      "Comparar ambas y conservar la más útil."
    ),
    "wheelbase" -> InfoColumna(
      "Distancia entre ejes.",
      "SI",
      "Medida física relevante para caracterizar el vehículo.",
      "Extraer valor numérico."
    ),
    "width" -> InfoColumna(
      "Anchura del vehículo.",
      "SI",
      "Medida física útil para segmentación del vehículo.",
      "Extraer valor numérico."
    ),
    "year" -> InfoColumna(
      "Año del vehículo.",
      "SI",
      "Variable clave para explicar depreciación y precio.",
      "Analizar distribución y relación con price."
    )
  )

  def wrapText(text: String, width: Int): Seq[String] = {
    if (text == null || text.isEmpty) return Seq("")
    val words = text.split("\\s+")
    val lines = scala.collection.mutable.ArrayBuffer[String]()
    var current = ""

    for (word <- words) {
      if ((current + " " + word).trim.length <= width) {
        current = (current + " " + word).trim
      } else {
        lines += current
        current = word
      }
    }
    if (current.nonEmpty) lines += current
    lines.toSeq
  }

  def printField(label: String, value: String, width: Int = 88): Unit = {
    val wrapped = wrapText(value, width)
    if (wrapped.nonEmpty) {
      println(f"$label%-14s ${wrapped.head}")
      wrapped.tail.foreach(line => println(f"${""}%-14s $line"))
    } else {
      println(f"$label%-14s ")
    }
  }

  println("\n==================== ANÁLISIS DEL DATASET ====================\n")

  df.dtypes.foreach { case (colName, tipoReal) =>
    val info = meta.getOrElse(
      colName,
      InfoColumna(
        "Pendiente de documentar.",
        "AMPLIAR",
        "No hay definición suficiente para decidir su uso.",
        "Revisar valores reales y decidir tratamiento."
      )
    )

    println("=" * 100)
    printField("Columna:", colName)
    printField("Tipo:", tipoReal)
    printField("Significado:", info.significado)
    printField("¿Analizar?:", info.analizar)
    printField("Motivo:", info.motivo)
    printField("Tratamiento:", info.tratamiento)
  }

  println("=" * 100)
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
    // =========================================================
  // REGIÓN GEOGRÁFICA FINA Y ANÁLISIS POR REGIÓN
  // =========================================================

  // ---------------------------------------------------------
  // Añadir región geográfica fina usando latitude + longitude
  // ---------------------------------------------------------
  def addGeoRegion(
      df: DataFrame,
      latitudeCol: String = "latitude",
      longitudeCol: String = "longitude",
      regionCol: String = "geo_region"
  ): DataFrame = {
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
  // esta función analiza una variable numérica por región geográfica, 
  // devuelve estadísticas las descriptivas 
  // ---------------------------------------------------------
  def analyzeNumericByGeoRegion( df: DataFrame,numericCol: String,positiveOnly: Boolean = false,latitudeCol: String = "latitude",
  longitudeCol: String = "longitude",regionCol: String = "geo_region" ): DataFrame = {
    val dfRegion = addGeoRegion(df, latitudeCol, longitudeCol, regionCol)
    val dfNum = getValidNumericDF(dfRegion, numericCol, positiveOnly)

    dfNum.groupBy(regionCol)
      .agg(
        count("*").alias("n"),
        min(numericCol).alias("min_value"),
        expr(s"percentile_approx($numericCol, 0.25)").alias("p25"),
        expr(s"percentile_approx($numericCol, 0.5)").alias("median_value"),
        expr(s"percentile_approx($numericCol, 0.75)").alias("p75"),
        expr(s"percentile_approx($numericCol, 0.95)").alias("p95"),
        max(numericCol).alias("max_value"),
        round(avg(numericCol), 2).alias("mean_value"),
        skewness(numericCol).alias("skew_value")
      )
      .orderBy(desc("median_value"))
  }

  // ---------------------------------------------------------
  // Analizar price por geo_region
  // ---------------------------------------------------------
  def analyzePriceByGeoRegion(
      df: DataFrame,
      priceCol: String = "price",
      latitudeCol: String = "latitude",
      longitudeCol: String = "longitude",
      regionCol: String = "geo_region"
  ): DataFrame = {
    analyzeNumericByGeoRegion(
      df = df,
      numericCol = priceCol,
      positiveOnly = true,
      latitudeCol = latitudeCol,
      longitudeCol = longitudeCol,
      regionCol = regionCol
    )
    .withColumnRenamed("min_value", "min_price")
    .withColumnRenamed("median_value", "median_price")
    .withColumnRenamed("mean_value", "mean_price")
    .withColumnRenamed("max_value", "max_price")
    .withColumnRenamed("skew_value", "skew_price")
  }


def haversineMiles(lat1: Column, lon1: Column, lat2: Double, lon2: Double): Column = {
  val r = 3958.8 // radio Tierra en millas

  val dLat = radians(lit(lat2) - lat1)
  val dLon = radians(lit(lon2) - lon1)

  val a =
    pow(sin(dLat / 2), 2) +
    cos(radians(lat1)) * cos(radians(lit(lat2))) * pow(sin(dLon / 2), 2)

  lit(r) * lit(2) * asin(sqrt(a))
}
def agregarFeaturesUrbanasHaversine(df: DataFrame): DataFrame = {

  val ciudades = Seq(
    ("New_York", 40.7128, -74.0060),
    ("Los_Angeles", 34.0522, -118.2437),
    ("Chicago", 41.8781, -87.6298),
    ("Houston", 29.7604, -95.3698),
    ("Phoenix", 33.4484, -112.0740),
    ("Philadelphia", 39.9526, -75.1652),
    ("San_Antonio", 29.4241, -98.4936),
    ("San_Diego", 32.7157, -117.1611),
    ("Dallas", 32.7767, -96.7970),
    ("San_Jose", 37.3382, -121.8863),
    ("Miami", 25.7617, -80.1918),
    ("Atlanta", 33.7490, -84.3880),
    ("Seattle", 47.6062, -122.3321),
    ("Denver", 39.7392, -104.9903),
    ("Boston", 42.3601, -71.0589)
  )

  val distExprs = ciudades.map { case (nombre, lat, lon) =>
    haversineMiles(col("latitude"), col("longitude"), lat, lon).alias(s"dist_$nombre")
  }

  val dfDist = df.select(col("*") +: distExprs: _*)

  val distCols = ciudades.map { case (nombre, _, _) => col(s"dist_$nombre") }
  val minDistExpr = least(distCols: _*)

  val nearestCityExpr =
    when(col("dist_New_York") === minDistExpr, "New_York")
      .when(col("dist_Los_Angeles") === minDistExpr, "Los_Angeles")
      .when(col("dist_Chicago") === minDistExpr, "Chicago")
      .when(col("dist_Houston") === minDistExpr, "Houston")
      .when(col("dist_Phoenix") === minDistExpr, "Phoenix")
      .when(col("dist_Philadelphia") === minDistExpr, "Philadelphia")
      .when(col("dist_San_Antonio") === minDistExpr, "San_Antonio")
      .when(col("dist_San_Diego") === minDistExpr, "San_Diego")
      .when(col("dist_Dallas") === minDistExpr, "Dallas")
      .when(col("dist_San_Jose") === minDistExpr, "San_Jose")
      .when(col("dist_Miami") === minDistExpr, "Miami")
      .when(col("dist_Atlanta") === minDistExpr, "Atlanta")
      .when(col("dist_Seattle") === minDistExpr, "Seattle")
      .when(col("dist_Denver") === minDistExpr, "Denver")
      .otherwise("Boston")

  dfDist.withColumn("dist_to_major_city_miles", minDistExpr).withColumn("nearest_major_city", nearestCityExpr).withColumn("urban_level", 
  when(col("dist_to_major_city_miles") <= 25, "urban").when(col("dist_to_major_city_miles") <= 100, "suburban").otherwise("rural"))
  .withColumn("log_dist_to_major_city", log1p(col("dist_to_major_city_miles")))
}
}
