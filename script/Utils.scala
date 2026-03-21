import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
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

  /**
   * Filtra filas válidas para análisis de precio.
   */
  def getValidPriceDF(df: DataFrame, priceCol: String = "price"): DataFrame = {
    df.filter(col(priceCol).isNotNull && col(priceCol) > 0)
  }

  
  /**
   * Percentiles globales de price.
   */
  def analyzePricePercentiles(
      df: DataFrame,
      priceCol: String = "price",
      probs: Seq[Double] = Seq(0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.999)
  ): DataFrame = {
    val dfPrice = getValidPriceDF(df, priceCol)
    val probsStr = probs.mkString(", ")

    dfPrice.selectExpr(
      s"percentile_approx($priceCol, array($probsStr)) as price_percentiles"
    )
  }

  /**
   * Compara estadísticamente price vs log(price).
   */
  def analyzePriceVsLogPrice(df: DataFrame, priceCol: String = "price"): DataFrame = {
    val dfPrice = getValidPriceDF(df, priceCol)
      .withColumn("log_price", log1p(col(priceCol)))

    dfPrice.select(
      skewness(priceCol).alias("skew_price"),
      kurtosis(priceCol).alias("kurt_price"),
      skewness("log_price").alias("skew_log_price"),
      kurtosis("log_price").alias("kurt_log_price")
    )
  }

  /**
   * Devuelve los vehículos más caros para inspección manual.
   */
  def getTopExpensiveVehicles(
      df: DataFrame,
      priceCol: String = "price",
      topN: Int = 30
  ): DataFrame = {
    val dfPrice = getValidPriceDF(df, priceCol)

    dfPrice.select(
      col(priceCol),
      col("make_name"),
      col("model_name"),
      col("trim_name"),
      col("year"),
      col("body_type"),
      col("fuel_type"),
      col("is_new"),
      col("mileage"),
      col("horsepower")
    ).orderBy(desc(priceCol))
      .limit(topN)
  }

  /**
   * Analiza price por una variable categórica.
   * Muy útil para make_name, body_type, is_new, fuel_type, etc.
   */
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

  /**
   * Analiza price por las categorías más frecuentes de una columna.
   * Ideal para make_name o model_name cuando hay mucha cardinalidad.
   */
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

  /**
   * Analiza price por year.
   */
  def analyzePriceByYear(
      df: DataFrame,
      priceCol: String = "price"
  ): DataFrame = {
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

  /**
   * Muestra los top K precios dentro de cada categoría.
   * Muy útil para comprobar si los extremos son coherentes por marca.
   */
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

    val w = Window.partitionBy(categoryCol).orderBy(desc(priceCol))

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

  /**
   * Resumen final para decidir si log(price) puede ser útil.
   */
  def summarizePriceTransformationDecision(
      df: DataFrame,
      priceCol: String = "price"
  ): DataFrame = {
    val dfPrice = getValidPriceDF(df, priceCol)
      .withColumn("log_price", log1p(col(priceCol)))

    dfPrice.select(
      count("*").alias("n"),
      avg(priceCol).alias("mean_price"),
      expr(s"percentile_approx($priceCol, 0.5)").alias("median_price"),
      stddev(priceCol).alias("std_price"),
      skewness(priceCol).alias("skew_price"),
      kurtosis(priceCol).alias("kurt_price"),
      skewness("log_price").alias("skew_log_price"),
      kurtosis("log_price").alias("kurt_log_price")
    )
  }

  /**
   * Función de impresión rápida para no repetir show(false).
   */
  def showDF(title: String, df: DataFrame, n: Int = 50): Unit = {
    println(s"\n=== $title ===")
    df.show(n, truncate = false)
  }
  def runFullPriceEDA(df: DataFrame): Unit = {
 
  showDF("Percentiles de price", analyzePricePercentiles(df))
  showDF("Comparación price vs log(price)", analyzePriceVsLogPrice(df))
  showDF("Precio por is_new", analyzePriceByCategory(df, "is_new"))
  showDF("Precio por body_type", analyzePriceByCategory(df, "body_type", topN = 20))
  showDF("Precio por make_name", analyzePriceByTopCategories(df, "make_name", topCategories = 20))
  showDF("Precio por year", analyzePriceByYear(df), n = 50)
  showDF("Top vehículos más caros", getTopExpensiveVehicles(df, topN = 30), n = 30)
  showDF(
    "Top 5 precios por marca",
    getTopKPriceByCategory(df, categoryCol = "make_name", topCategories = 15, k = 5),
    n = 200
  )
  showDF(
    "Resumen para decidir transformación de price",
    summarizePriceTransformationDecision(df)
  )
}

}
