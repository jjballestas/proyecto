import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
 

object Utils {

  // ---------------------------------------------------------
  // Cargar CSVclear
  // ---------------------------------------------------------
def loadData(spark: SparkSession, path: String, filename: String): DataFrame = {
  spark.read.option("header", "true").option("inferSchema", "true")
  .option("multiLine", "true").option("quote", "\"").option("escape", "\"")
  .option("mode", "PERMISSIVE").csv(path + filename)
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
      "Puede reflejar tamaño y segmento del vehículo.",
      "Extraer valor numérico en pulgadas y revisar nulos."
    ),
    "bed" -> InfoColumna(
      "Tipo o tamaño de caja de carga en pickups.",
      "SI",
      "Es relevante en pickups y puede ayudar a segmentar este tipo de vehículo.",
      "Mantener como categórica; si hay muchos nulos interpretarlos como no aplica."
    ),
    "bed_height" -> InfoColumna(
      "Altura de la caja de carga.",
      "AMPLIAR",
      "Solo aporta valor en pickups y suele tener muchos nulos.",
      "Extraer numérico y conservar solo si el porcentaje de datos válidos lo justifica."
    ),
    "bed_length" -> InfoColumna(
      "Longitud de la caja de carga.",
      "AMPLIAR",
      "Solo es útil para pickups y puede presentar alta ausencia.",
      "Extraer numérico y evaluar utilidad real."
    ),
    "body_type" -> InfoColumna(
      "Tipo de carrocería del vehículo.",
      "SI",
      "Es una de las variables estructurales más importantes.",
      "Analizar distribución y relación con el precio."
    ),
    "cabin" -> InfoColumna(
      "Tipo de cabina en pickups.",
      "AMPLIAR",
      "Solo aplica a ciertos vehículos.",
      "Tratar como categórica y considerar nulos como no aplica."
    ),
    "city" -> InfoColumna(
      "Ciudad donde está anunciado el vehículo.",
      "SI",
      "Puede capturar diferencias regionales del mercado.",
      "Analizar cardinalidad y agrupar categorías raras si hace falta."
    ),
    "city_fuel_economy" -> InfoColumna(
      "Consumo en ciudad.",
      "SI",
      "Variable técnica útil para caracterizar el vehículo.",
      "Analizar distribución, nulos y outliers."
    ),
    "combine_fuel_economy" -> InfoColumna(
      "Consumo combinado.",
      "AMPLIAR",
      "Según la documentación debería ser numérica, pero en tu esquema aparece como string.",
      "Revisar parsing y convertir a numérico si es correcto."
    ),
    "daysonmarket" -> InfoColumna(
      "Días que lleva publicado el anuncio.",
      "SI",
      "Puede relacionarse con el nivel de precio y la demanda.",
      "Analizar distribución y relación con price."
    ),
    "dealer_zip" -> InfoColumna(
      "Código postal del concesionario.",
      "AMPLIAR",
      "Puede ser útil geográficamente, pero no debe tratarse como variable numérica continua.",
      "Usarlo para agregación geográfica o descartarlo del modelado base."
    ),
    "description" -> InfoColumna(
      "Descripción libre del anuncio.",
      "AMPLIAR",
      "Es texto libre; puede aportar mucho, pero no en un EDA tabular básico.",
      "Excluir del análisis simple y usar NLP o variables derivadas si se necesita."
    ),
    "engine_cylinders" -> InfoColumna(
      "Configuración del motor por cilindros.",
      "SI",
      "Variable mecánica relevante para segmentación y precio.",
      "Normalizar etiquetas y analizar como categórica."
    ),
    "engine_displacement" -> InfoColumna(
      "Cilindrada del motor.",
      "SI",
      "Variable técnica importante y potencialmente correlacionada con precio.",
      "Analizar distribución, nulos y correlación."
    ),
    "engine_type" -> InfoColumna(
      "Tipo o configuración del motor.",
      "SI",
      "Aporta información mecánica útil.",
      "Analizar como categórica y revisar redundancia con engine_cylinders."
    ),
    "exterior_color" -> InfoColumna(
      "Color exterior detallado.",
      "AMPLIAR",
      "Puede tener mucha cardinalidad y nombres comerciales.",
      "Normalizar categorías o usar listing_color."
    ),
    "fleet" -> InfoColumna(
      "Indica si el vehículo perteneció a una flota.",
      "SI",
      "Puede reflejar mayor uso y afectar la depreciación.",
      "Analizar proporciones y relación con price."
    ),
    "frame_damaged" -> InfoColumna(
      "Indica si el chasis está dañado.",
      "SI",
      "Es una variable crítica sobre el estado del vehículo.",
      "Analizar frecuencia e impacto en precio."
    ),
    "franchise_dealer" -> InfoColumna(
      "Indica si el vendedor es concesionario oficial.",
      "SI",
      "Puede influir en confianza, garantía y precio.",
      "Analizar proporciones y diferencias de precio."
    ),
    "franchise_make" -> InfoColumna(
      "Marca asociada a la franquicia del concesionario.",
      "AMPLIAR",
      "Podría ser redundante con make_name.",
      "Revisar redundancia antes de usar."
    ),
    "front_legroom" -> InfoColumna(
      "Espacio para piernas delantero.",
      "SI",
      "Puede reflejar tamaño interior del coche.",
      "Extraer numérico y analizar."
    ),
    "fuel_tank_volume" -> InfoColumna(
      "Capacidad del tanque de combustible.",
      "SI",
      "Variable física que puede ayudar a caracterizar el vehículo.",
      "Extraer numérico."
    ),
    "fuel_type" -> InfoColumna(
      "Tipo de combustible.",
      "SI",
      "Muy relevante para segmentación y precio.",
      "Analizar como categórica."
    ),
    "has_accidents" -> InfoColumna(
      "Indica si el vehículo tiene accidentes registrados.",
      "SI",
      "Es una de las variables más importantes de estado.",
      "Analizar frecuencia e impacto en precio."
    ),
    "height" -> InfoColumna(
      "Altura del vehículo.",
      "AMPLIAR",
      "Puede ser útil, pero requiere limpieza y no siempre aporta mucho.",
      "Extraer numérico y evaluar."
    ),
    "highway_fuel_economy" -> InfoColumna(
      "Consumo en carretera.",
      "SI",
      "Variable técnica relevante.",
      "Analizar distribución y outliers."
    ),
    "horsepower" -> InfoColumna(
      "Potencia del motor en HP.",
      "SI",
      "Variable clave para prestaciones y precio.",
      "Analizar distribución, outliers y correlación."
    ),
    "interior_color" -> InfoColumna(
      "Color interior.",
      "AMPLIAR",
      "Puede tener impacto limitado y mucha cardinalidad.",
      "Normalizar o agrupar categorías raras."
    ),
    "isCab" -> InfoColumna(
      "Indica si fue taxi o cab.",
      "SI",
      "Puede reflejar un uso intensivo.",
      "Analizar frecuencia e impacto sobre el precio."
    ),
    "is_certified" -> InfoColumna(
      "Indica si el vehículo está certificado.",
      "AMPLIAR",
      "Según la documentación debería ser booleano, pero en el esquema aparece string.",
      "Revisar valores y convertir a booleano si procede."
    ),
    "is_cpo" -> InfoColumna(
      "Indica si es certified pre-owned por concesionario.",
      "SI",
      "Puede influir en confianza y precio.",
      "Analizar proporciones y diferencias de precio."
    ),
    "is_new" -> InfoColumna(
      "Indica si el vehículo es nuevo o muy reciente.",
      "SI",
      "Puede alterar mucho la distribución del precio.",
      "Analizar separado si mezcla nuevos y usados."
    ),
    "is_oemcpo" -> InfoColumna(
      "Indica si está certificado por el fabricante.",
      "SI",
      "Puede influir en garantía y valor percibido.",
      "Analizar proporciones y precio."
    ),
    "latitude" -> InfoColumna(
      "Latitud del concesionario.",
      "AMPLIAR",
      "Útil para información geográfica, no para EDA tabular básico.",
      "Usar junto con longitude en variables derivadas."
    ),
    "length" -> InfoColumna(
      "Longitud del vehículo.",
      "SI",
      "Medida física relevante para segmentación.",
      "Extraer numérico."
    ),
    "listed_date" -> InfoColumna(
      "Fecha en que el anuncio fue publicado.",
      "SI",
      "Puede aportar estacionalidad y antigüedad.",
      "Derivar año, mes o antigüedad si interesa."
    ),
    "listing_color" -> InfoColumna(
      "Grupo dominante del color exterior.",
      "SI",
      "Versión más tratable que exterior_color.",
      "Analizar como categórica."
    ),
    "listing_id" -> InfoColumna(
      "Identificador único del anuncio.",
      "NO",
      "Es un identificador técnico.",
      "Usar solo para control de registros."
    ),
    "longitude" -> InfoColumna(
      "Longitud del concesionario.",
      "AMPLIAR",
      "Útil para geolocalización, no para análisis directo.",
      "Usar junto con latitude."
    ),
    "main_picture_url" -> InfoColumna(
      "URL de la imagen principal del anuncio.",
      "NO",
      "No aporta valor en un EDA tabular.",
      "Excluir del análisis."
    ),
    "major_options" -> InfoColumna(
      "Listado de equipamientos u opciones principales.",
      "AMPLIAR",
      "Puede ser útil, pero suele venir como texto o lista compleja.",
      "Contar opciones o extraer indicadores binarios."
    ),
    "make_name" -> InfoColumna(
      "Marca del vehículo.",
      "SI",
      "Variable clave del problema.",
      "Analizar distribución y relación con price."
    ),
    "maximum_seating" -> InfoColumna(
      "Número máximo de plazas.",
      "SI",
      "Puede reflejar tamaño y tipo de vehículo.",
      "Convertir a numérico."
    ),
    "mileage" -> InfoColumna(
      "Kilometraje del vehículo.",
      "SI",
      "Es una de las variables más importantes para el precio.",
      "Analizar distribución, outliers y relación con price."
    ),
    "model_name" -> InfoColumna(
      "Modelo del vehículo.",
      "SI",
      "Variable comercial importante.",
      "Analizar frecuencia y cardinalidad."
    ),
    "owner_count" -> InfoColumna(
      "Número de propietarios anteriores.",
      "SI",
      "Puede afectar confianza y depreciación.",
      "Analizar distribución y quizá tratarla como discreta."
    ),
    "power" -> InfoColumna(
      "Potencia expresada en texto.",
      "AMPLIAR",
      "Puede duplicar información de horsepower.",
      "Extraer numérico o excluir si es redundante."
    ),
    "price" -> InfoColumna(
      "Precio del vehículo.",
      "SI",
      "Es la variable objetivo principal.",
      "Analizar distribución, sesgo, outliers y log-transform si hace falta."
    ),
    "salvage" -> InfoColumna(
      "Indica si el vehículo tiene título salvage.",
      "SI",
      "Variable muy importante de estado legal y comercial.",
      "Analizar frecuencia e impacto en price."
    ),
    "savings_amount" -> InfoColumna(
      "Ahorro estimado respecto a una referencia.",
      "AMPLIAR",
      "Puede depender de cómo fue calculado.",
      "Revisar definición antes de usar."
    ),
    "seller_rating" -> InfoColumna(
      "Valoración del vendedor.",
      "SI",
      "Puede influir en confianza y precio percibido.",
      "Verificar integridad de la columna y analizar distribución."
    ),
    "sp_id" -> InfoColumna(
      "Identificador del vendedor o proveedor.",
      "NO",
      "Es un identificador técnico.",
      "Usar solo para trazabilidad."
    ),
    "sp_name" -> InfoColumna(
      "Nombre del vendedor o proveedor.",
      "AMPLIAR",
      "Puede tener cardinalidad alta y riesgo de fuga de información.",
      "Analizar frecuencia y decidir si agrupar o excluir."
    ),
    "theft_title" -> InfoColumna(
      "Indica si el título está asociado a robo.",
      "SI",
      "Variable crítica del estado legal.",
      "Analizar frecuencia e impacto."
    ),
    "torque" -> InfoColumna(
      "Par motor.",
      "AMPLIAR",
      "Es útil, pero suele venir en texto y requiere parsing.",
      "Extraer numérico si es posible."
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
      "Puede ser redundante con transmission.",
      "Comparar y conservar la más útil."
    ),
    "trimId" -> InfoColumna(
      "Identificador del acabado o versión.",
      "NO",
      "Parece identificador técnico.",
      "Usar solo para trazabilidad."
    ),
    "trim_name" -> InfoColumna(
      "Nombre del acabado o versión.",
      "SI",
      "Puede capturar diferencias dentro del mismo modelo.",
      "Analizar frecuencia y cardinalidad."
    ),
    "vehicle_damage_category" -> InfoColumna(
      "Categoría de daño del vehículo.",
      "SI",
      "Aporta información relevante sobre el estado.",
      "Analizar como categórica."
    ),
    "wheel_system" -> InfoColumna(
      "Sistema de tracción o ruedas.",
      "SI",
      "Variable técnica relevante.",
      "Analizar como categórica."
    ),
    "wheel_system_display" -> InfoColumna(
      "Descripción del sistema de tracción.",
      "AMPLIAR",
      "Puede ser redundante con wheel_system.",
      "Evaluar redundancia."
    ),
    "wheelbase" -> InfoColumna(
      "Distancia entre ejes.",
      "SI",
      "Medida física relevante.",
      "Extraer numérico."
    ),
    "width" -> InfoColumna(
      "Anchura del vehículo.",
      "SI",
      "Medida física útil para segmentación.",
      "Extraer numérico."
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

  println("\n ANÁLISIS DEL DATASET n")

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
  // Pipeline: cargar CSV o parquet, procesar si es necesario
  // ---------------------------------------------------------
  def loadOrProcessData(
      spark: SparkSession,
      basepath: String,
      rawFile: String,
      parquetPath: String,
      realizarEda: Boolean
  ): DataFrame = {

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
    df
      .filter(col("price") > 0)
      .withColumn("log_price", log(col("price")))
      .na.drop("all")
  }

  // ---------------------------------------------------------
  // EDA completo
  // ---------------------------------------------------------
  def analisisEDA(df: DataFrame): Unit = {
    println("\n==================== EDA: Análisis Exploratorio ====================\n")



    println("📌 Valores nulos por columna:")
    df.columns.foreach { colName =>
      val nulls = df.filter(col(colName).isNull || col(colName) === "" || col(colName).isNaN).count()
      println(f" - $colName%-25s : $nulls")
    }
    println("\n")

    println("📌 Estadísticos descriptivos:")
    df.describe().show(false)

    val dup = df.count() - df.dropDuplicates().count()
    println(s"\n📌 Filas duplicadas: $dup\n")

    println("📌 Distribución de variables numéricas:")
    df.dtypes
      .filter(t => t._2 == "IntegerType" || t._2 == "DoubleType")
      .foreach { case (colName, _) =>
        println(s"\n - Distribución de $colName:")
        df.select(colName).describe().show()
      }

    println("\n📌 Top categorías por columna categórica:")
    df.dtypes
      .filter(_._2 == "StringType")
      .foreach { case (colName, _) =>
        println(s"\n - $colName:")
        df.groupBy(colName).count().orderBy(desc("count")).show(10, truncate = false)
      }

    println("\n==================== Fin del EDA ====================\n")
  }

  // ---------------------------------------------------------
  // Mostrar tabla bonita
  // ---------------------------------------------------------
  def showTable(df: DataFrame, numRows: Int): Unit = {
  
  val colsBonitas = Seq(
    "vin", "make_name", "model_name", "year", "price",
    "mileage", "body_type", "fuel_type", "transmission_display"
  ).filter(df.columns.contains)

  df.select(colsBonitas.head, colsBonitas.tail: _*).show(numRows, truncate = 25)
 
}

}
