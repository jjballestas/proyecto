import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Utils {

  // ---------------------------------------------------------
  // Cargar CSV
  // ---------------------------------------------------------
  def loadData(spark: SparkSession, path: String, filename: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path + filename)
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

    println(s"📌 Filas: ${df.count()}  |  Columnas: ${df.columns.length}\n")

    println("📌 Esquema del DataFrame:")
    df.printSchema()
    println("\n")

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
    println(s"\n---------------------- TABLA ($numRows filas) ----------------------")
    df.show(numRows, truncate = false)
    println("-------------------------------------------------------------------\n")
  }

}
