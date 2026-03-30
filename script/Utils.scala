import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import scala.util.Try 
import scala.collection.immutable.ListMap
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
 
 
import org.apache.spark.sql.expressions.Window

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)
Logger.getLogger("org.apache.spark.scheduler.DAGScheduler").setLevel(Level.ERROR)
Logger.getLogger("org.apache.spark.util.SizeEstimator").setLevel(Level.OFF)
// ── Suprimir warnings de reflection ilegal de la JVM ─────────
val originalErr = System.err
val filteredErr = new java.io.PrintStream(originalErr) {
  override def println(x: String): Unit = {
    if (x != null && x.contains("illegal reflective access")) return
    if (x != null && x.contains("WARNING: An illegal")) return
    if (x != null && x.contains("WARNING: Please consider")) return
    if (x != null && x.contains("WARNING: Use --illegal")) return
    if (x != null && x.contains("WARNING: All illegal")) return
    super.println(x)
  }
  override def println(x: Object): Unit = println(if (x == null) "null" else x.toString)
}
System.setErr(filteredErr)

 
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


    //esta funcion muestra una tabla con un número específico de filas, seleccionando columnas relevantes para un dataset de autos usados, como "vin", "make_name", "model_name", "year", "price", "mileage", "body_type
    def showTable(df: DataFrame, numRows: Int): Unit = {
    val colsBonitas = Seq( "vin", "make_name", "model_name", "year", "price","mileage", "body_type", "fuel_type", "transmission_display").filter(df.columns.contains)
    df.select(colsBonitas.head, colsBonitas.tail: _*).show(numRows, truncate = 25)
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
  

  def getPriceThresholds(df: DataFrame): (Double, Double) = {
    val quantiles = df.stat.approxQuantile("price", Array(0.90, 0.95), 0.01)
    (quantiles(0), quantiles(1))  
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


  def addMajorOptionsFeaturesFromData(df: DataFrame,optionsCol: String     = "major_options",
  descriptionCol: String = "description",
  opcionesRelevantes: Seq[String] = Seq("offroadpackage", "navigationsystem", "thirdrowseating", 
  "sunroof/moonroof", "parkingsensors", "heatedseats","adaptivecruisecontrol", "blindspotmonitoring",
  "backupcamera", "leatherseats", "multizoneclimatecontrol")): DataFrame = {

      val dfDesc = df.withColumn("description_length",when(col(descriptionCol).isNull, 0)
      .otherwise(length(col(descriptionCol)))).drop(descriptionCol)

      val dfNorm = dfDesc.withColumn("options_clean",when(col(optionsCol).isNull, "no_options_reported")
      .otherwise(regexp_replace(lower(col(optionsCol)), "[\\[\\]'\"\\s]", "")))

      val dfCount = dfNorm.withColumn("option_count",when(col("options_clean") === "no_options_reported", 0)
      .otherwise(size(split(regexp_replace(lower(col(optionsCol)), "[\\[\\]'\\\"\\s]", ""), ","))))
      
      println(s"\n  📌 Generando ${opcionesRelevantes.length} variables binarias desde $optionsCol:")
      opcionesRelevantes.foreach(t =>
        println(s"     → has_${t.replaceAll("[^a-z0-9]", "_")}"))
        
      val dfWithFlags = opcionesRelevantes.foldLeft(dfCount) { (acc, termino) =>
      val colName = s"has_${termino.replaceAll("[^a-z0-9]", "_")}"
      acc.withColumn(colName, col("options_clean").contains(termino).cast("int"))
      }
      dfWithFlags.drop("options_clean", optionsCol)
  }

  def resolverColumnas(df: DataFrame,numColsCandidatas: Array[String],
  strColsCandidatas: Array[String],boolColsCandidatas: Array[String]): (Array[String], Array[String], Array[String]) = {

      val colsDisponibles = df.columns.toSet

      val numCols  = numColsCandidatas.filter(colsDisponibles.contains)
      val strCols  = strColsCandidatas.filter(colsDisponibles.contains)
      val boolCols = boolColsCandidatas.filter(colsDisponibles.contains)

      val numFaltantes  = numColsCandidatas.filterNot(colsDisponibles.contains)
      val strFaltantes  = strColsCandidatas.filterNot(colsDisponibles.contains)
      val boolFaltantes = boolColsCandidatas.filterNot(colsDisponibles.contains)

    

      if (numFaltantes.nonEmpty)
        println(s"  ⚠️  numCols  faltantes: ${numFaltantes.mkString(", ")}")
      if (strFaltantes.nonEmpty)
        println(s"  ⚠️  strCols  faltantes: ${strFaltantes.mkString(", ")}")
      if (boolFaltantes.nonEmpty)
        println(s"  ⚠️  boolCols faltantes: ${boolFaltantes.mkString(", ")}")

      println(s"  ══════════════════════════════════════════\n")

      (numCols, strCols, boolCols)
 }



  def addGeographicFeatures(df: DataFrame): DataFrame = {

    val df1 = addGeoRegion(df)
    val df2 = agregarFeaturesUrbanasHaversine(df1)
     df2 
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
    val rawAge = year(col("listed_date")) - col("year")
    df.withColumn("listed_year",  year(col("listed_date"))).withColumn("listed_month", month(col("listed_date")))
    .withColumn("vehicle_age_at_listing",when(rawAge.between(0, 30), rawAge).otherwise(lit(null).cast("double")))
  }
 

    

  def imputeVehicleAge(df: DataFrame): DataFrame = {

    val medianAge = df.filter(col("vehicle_age_at_listing").isNotNull)
    .selectExpr("percentile_approx(vehicle_age_at_listing, 0.5) as median_age").first().get(0)

    val medianVal = if (medianAge == null) 5.0 else medianAge.toString.toDouble

    val nullCount = df.filter(col("vehicle_age_at_listing").isNull).count()
    println(f"  [imputeVehicleAge] nulls encontrados : $nullCount")
    println(f"  [imputeVehicleAge] imputando con mediana: $medianVal%.1f años")

    df.withColumn("vehicle_age_at_listing",
      when(col("vehicle_age_at_listing").isNull, lit(medianVal)).otherwise(col("vehicle_age_at_listing")))
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
   //esta función transforma la variable objetivo "price" aplicando el logaritmo natural,
    // creando una nueva columna "log_price" y eliminando la columna original "price".
def transformTargetToLog(df: DataFrame): DataFrame = {
  df.withColumn("log_price", log(col("price"))).drop("price")    
}

  

def prepararDataset(spark: SparkSession,df: DataFrame,path: String,forcePreprocess: Boolean = false
): DataFrame = {

  val fs              = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val finalPath       = path + "dataset/parquet/dataset_final"
  val preImputPath    = path + "dataset/parquet/pre_imputation"
  val finalHdfsPath   = new org.apache.hadoop.fs.Path(finalPath)

 // Si el dataset final ya existe y no se fuerza el preprocesado, lo cargamos para ahorrar tiempo
  if (!forcePreprocess && fs.exists(finalHdfsPath)) {
    println("  ✅ Cargando dataset_final existente desde disco...")
    val dfCargado = spark.read.parquet(finalPath)
    println(s"  ✅ Dataset cargado: ${dfCargado.count()} registros | ${dfCargado.columns.length} columnas")
    return dfCargado
  }

  // cuando no existe el dataset final o se fuerza el preprocesado, 
  //ejecutamos todas las fases de limpieza, feature engineering e imputación
  if (forcePreprocess)
    println("  🔄 forcePreprocess=true — regenerando dataset completo...")
  else
    println("  🔄 dataset_final no encontrado — ejecutando preprocesado...")

  // Fase 1: limpieza y feature engineering
  val colsToDrop = Seq("combine_fuel_economy","is_certified","vehicle_damage_category",
    "vin","listing_id","sp_id","main_picture_url","transmission_display",
    "wheel_system_display","listing_color","dealer_zip","sp_name",
    "bed","cabin","is_cpo","is_oemcpo","isCab","franchise_make" ,"frame_damaged", "salvage", "theft_title")

  var dfWork = dropColumns(df, colsToDrop)
  dfWork = addGamaAlta(dfWork)
  dfWork = addIsPickupAndClean(dfWork)
  dfWork = cleanFuelType(dfWork)
  dfWork = extractStringNumericFeatures(dfWork)
  dfWork = addGeographicFeatures(dfWork)
  dfWork = addTemporalFeatures(dfWork)
  dfWork = fillBooleanAsCategory(dfWork, Seq("fleet","has_accidents"))
  dfWork = treatOwnerCount(dfWork)
  dfWork = treatMileageOutliers(dfWork)
  dfWork = treatDaysOnMarket(dfWork)
  dfWork = treatSavingsAmount(dfWork, mode = "drop")

  // Fase 2: corte de linaje
  dfWork.write.mode("overwrite").parquet(preImputPath)
  var dfImp = spark.read.parquet(preImputPath) 
 
  dfImp = addMajorOptionsFeaturesFromData(dfImp)   
 
 // Fase 3: imputación y features finales
  dfImp = imputeDimensions(dfImp)
  dfImp = imputeEngineSpecs(dfImp)
  dfImp = imputeFuelEconomy(dfImp)
  dfImp = imputeRemainingNumeric(dfImp)
  dfImp = addPowerDensity(dfImp)
  dfImp = filterPriceErrors(dfImp)
  dfImp = addIsClassicFlag(dfImp)
  dfImp = fillCategoricalUnknown(dfImp, Seq("interior_color","body_type","engine_type" 
  ,"transmission","trim_name","wheel_system"))

  
  val colsToDropPost = Seq(
    "engine_displacement", "city_fuel_economy", "power", "torque",
    "engine_cylinders", "trimId", "listed_date", "bed_height", 
    "bed_length", "major_options", "description",
    "geo_region", "urban_level", "dist_to_major_city_miles",   
    "is_classic",                                               
    "listed_year", "listed_month"                              
  )

  dfImp = dropColumns(dfImp, colsToDropPost)
  dfImp = imputeVehicleAge(dfImp) 
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

  spark.read.parquet(finalPath) 
}


def mostrarResumenFinal(df: DataFrame): Unit = {
    val total     = df.count()
    val sepAncho  = 70


    if (total == 0) {
      println("\n  ⚠️  DataFrame vacío — sin resumen disponible")
      return
    }
    // esta función auxiliar imprime una secuencia de strings en columnas formateadas 
    // mejora la legibilidad del resumen final
  def imprimirEnColumnas(cols: Seq[String], colWidth: Int = 30, colsPorFila: Int = 3): Unit = {
    cols.grouped(colsPorFila).foreach { grupo =>
      println("     " + grupo.map(c => c.padTo(colWidth, ' ')).mkString("  "))
    }
  }

    

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
      println("  RESUMEN DATASET RAW DATA")

       mostrarResumenFinal(dfRaw)

    prepararDataset(spark, dfRaw, path, forcePreprocess)
  }
}


def crearSubconjuntoControlado(df: DataFrame,targetSize: Int = 400000,seed: Long = 42L,minRowsPerStratum: Int = 500): DataFrame = {

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
  val dfBase = df.filter(col("log_price").isNotNull).filter(col("year").isNotNull)

  val totalRows = dfBase.count()
  println(s"📌 Filas válidas de entrada   : $totalRows")

  if (targetSize >= totalRows) {
    println("✅ El tamaño objetivo es mayor o igual que el dataset. Se devuelve el dataset completo.\n")
    return dfBase
  }
 
  val dfStrata = dfBase.withColumn("year_bin",when(col("year") >= 2019, "recent")
  .when(col("year") >= 2014, "mid").otherwise("old")).withColumn("body_type_stratum",
  coalesce(trim(col("body_type")), lit("unknown"))).withColumn("stratum",concat_ws("_", col("year_bin"), col("body_type_stratum")))

  val strataCounts = dfStrata.groupBy("stratum").count().cache()

  val numStrata = strataCounts.count()
  val baseFraction = targetSize.toDouble / totalRows.toDouble

  println(s"📌 Número de estratos         : $numStrata")
  println(f"📌 Fracción base              : $baseFraction%.6f")


  val fractions: Map[String, Double] = strataCounts.collect().map { row =>
      val stratum = row.getAs[String]("stratum")
      val count = row.getAs[Long]("count")

      val fraction =
        if (count <= minRowsPerStratum) 1.0
        else math.min(1.0, math.max(baseFraction, minRowsPerStratum.toDouble / count.toDouble))

      stratum -> fraction
    }.toMap

  println(s"📌 Estratos con fracción calc.: ${fractions.size}")

  val sampled = dfStrata.stat.sampleBy("stratum", fractions, seed) 

  val sampledCount = sampled.count()
  println(s"📌 Filas tras sampleBy        : $sampledCount")
 
  val dfFinal =
    if (sampledCount > targetSize) {
      val ratio = targetSize.toDouble / sampledCount.toDouble
      println(f"📌 Ajuste fino adicional      : ratio = $ratio%.6f")
      sampled.sample(withReplacement = false, ratio, seed + 1L)
    } else {
      sampled
    }

  val result = dfFinal.drop("year_bin", "body_type_stratum", "stratum")

  val finalCount = result.count()
  println(s"✅ Tamaño final aproximado    : $finalCount")
  println("\n===============================================================\n")

  strataCounts.unpersist()

  result
}

def crearOCargarSplit(spark: SparkSession,df: DataFrame,trainPath: String,testPath: String,trainExiste: Boolean
,testExiste: Boolean,forceSplit: Boolean = false,trainRatio: Double = 0.8,seed: Long = 42L): (DataFrame, DataFrame) = {

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

 
def mostrarResultadosCV(nombre: String,cvModel: org.apache.spark.ml.tuning.CrossValidatorModel,
paramGrid: Array[org.apache.spark.ml.param.ParamMap]): Unit = {
  val avg = cvModel.avgMetrics

  
  def limpiarParams(pm: org.apache.spark.ml.param.ParamMap): String = {
    pm.toSeq.map { pair =>
        val nombre = pair.param.name  // solo el nombre sin UID
        val valor  = pair.value match {
          case d: Double => f"$d%.4f".replaceAll("\\.?0+$", "")  // quitar ceros trailing
          case other     => other.toString
        }
        s"$nombre=$valor"
      }.mkString("  ")   
  }

  println(s"\n  ══════════════════════════════════════════════════════════")
  println(s"  RESULTADOS CV — $nombre")
  println(s"  ══════════════════════════════════════════════════════════")
  println(f"  ${"#"}%-4s ${"Parámetros"}%-45s ${"RMSE_CV"}%10s")
  println("  " + "-" * 62)

  paramGrid.zip(avg).sortBy(_._2).zipWithIndex.foreach { case ((pm, rmse), idx) =>
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

 

