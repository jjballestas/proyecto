:load Utils.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.debug.maxToStringFields", 200)
val PATH = "/home/usuario/regresion/proyecto/"
val RAWDATA = "dataset/used_cars_data.csv"

val RAWPARQUET = "dataset/parquet/raw_data"
val REALIZAR_EDA = true    
val FORCE_CREATE_PARQUET = false
val FORCE_PREPROCESS =false
val spark = SparkSession.builder().appName("EDA Regresion").master("local[*]").getOrCreate()

 def analisisEDA(df: DataFrame): Unit = {
    println("\n==================== EDA: Análisis Exploratorio ====================\n") 
    /*
    Utils.mostrarNulosPorColumna(df)
    println()
    println("  Resumen de variables numéricas:\n")
      
    Utils.resumenNumericoTabular(df)
    val categoricas = Seq("body_type","fuel_type","transmission","make_name","model_name","exterior_color",
    "interior_color","wheel_system")

     
    Utils.resumenCategoricoTabular(df, categoricas)
    
    val booleanas = Seq("fleet","frame_damaged","franchise_dealer","has_accidents","isCab","is_cpo","is_new","is_oemcpo","salvage","theft_title")

    println("\n  Resumen de variables booleanas:\n")
    Utils.resumenBooleanasTabular(df, booleanas)
    
    val textoNoEstructurado = Seq("description", "major_options")
    Utils.resumenTextoTabular(df, textoNoEstructurado)

    val malTipadas = Seq("engine_cylinders","power","torque","back_legroom","fuel_tank_volume","width","height")
    Utils.resumenMalTipadasTabular(df, malTipadas)

    val identificadoras = Seq("vin", "listing_id", "sp_id")
    Utils.resumenIdentificadoresTabular(df, identificadoras)
 

    val columnasString = df.schema.fields.filter(_.dataType.simpleString == "string").map(_.name).toSeq
    val candidatos = Utils.detectarParesRedundantesPorNombre(columnasString)

    Utils.validarParesRedundantes(df, candidatos)
*/
    Utils.resumenGeografico(df)
 


    println("ANALISIS DE LA VARIABLE OBJETIVO PRICE:")
    Utils.showDF("Resumen global de price", Utils.analyzePriceGlobalStats(df)) 
    Utils.showDF("Percentiles de price", Utils.analyzePricePercentiles(df))
    Utils.showDF("Comparación price vs log(price)", Utils.analyzePriceVsLogPrice(df))
    Utils.showDF("Precio por is_new", Utils.analyzePriceByCategory(df, "is_new"))
    Utils.showDF("Precio por body_type", Utils.analyzePriceByCategory(df, "body_type", topN = 10))
    Utils.showDF("Precio por make_name", Utils.analyzePriceByTopCategories(df, "make_name", topCategories = 10))

    Utils.showDF("Precio por fuel_type", Utils.analyzePriceByTopCategories(df, "fuel_type", topCategories = 10))



    Utils.showDF("Precio por year", Utils.analyzePriceByYear(df), n = 20)
    Utils.showDF("Top vehículos más caros", Utils.getTopExpensiveVehicles(df, topN = 30), n = 10)
    Utils.showDF("Top 5 precios por marca", Utils.getTopKPriceByCategory(df, "make_name", topCategories = 15, k = 5), n = 50)
    
    val dfGeo = Utils.agregarFeaturesUrbanasHaversine(df)
    Utils.showDF("price por geo_region",Utils.analyzePriceByGeoRegion(dfGeo),n = 10)
    Utils.showDF("price por urban_level",Utils.analyzePriceByUrbanProximity(dfGeo),n = 10)
 
    

    
    println("ANALISIS DE OUTLIERS EN PRICE:")

    // 1) Límites globales de outliers para price
    Utils.showDF("Límites globales IQR para price",Utils.getGlobalIQROutlierBounds(df, "price", positiveOnly = true)
    .withColumnRenamed("median", "median_price"),n = 1)

    // 2) Estadísticas segmentadas por marca
    Utils.showDF("Comportamiento del precio dentro de cada marca,",Utils.getNumericSegmentStats(df,numericCol = "price"
    ,segmentCols = Seq("make_name"),positiveOnly = true,minGroupSize = 100).orderBy(desc("median_value")),n = 20)

    // 3) Detección de outliers sospechosos dentro de cada marca
    val suspiciousPriceByMake = Utils.detectSuspiciousNumericOutliersBySegment(df,numericCol = "price"
    ,segmentCols = Seq("make_name"),positiveOnly = true,minGroupSize = 100,iqrMultiplier = 3.0,minAbsoluteValue = 4000.0,
    extraCols = Seq("year", "model_name", "trim_name", "mileage", "horsepower", "body_type"))

    Utils.showDF("Outliers sospechosos de price por marca",suspiciousPriceByMake,n = 20)

    // 4) Resumen de outliers sospechosos por marca
    Utils.showDF("Resumen de outliers sospechosos de price por marca",
    Utils.summarizeSuspiciousNumericOutliers(suspiciousPriceByMake,numericCol = "price",segmentCols = Seq("make_name")),n = 20)




    
    Utils.showDF(  "Top mileage positivos",  Utils.getTopExtremeValues(df, "price",topN = 20, 
    positiveOnly = true, extraCols = Seq("make_name", "model_name", "year", "mileage", "body_type")))

        Utils.showDF("Resumen para decidir transformación de price", Utils.summarizePriceTransformationDecision(df))

    
        println("ANALISIS DE LA VARIABLE MILEAGE:") 
        
        Utils.showDF("mileage stats",   Utils.analyzeNumericGlobalStats(df, "mileage"))

      Utils.showDF("mileage pctiles", Utils.analyzeNumericPercentiles(df,"mileage",positiveOnly = true,
      probs = Seq(0.25, 0.50, 0.75, 0.95, 0.99, 0.995, 0.999)
      )
)
     
    Utils.showDF("mileage por is_new", Utils.getNumericSegmentStats(df, "mileage", Seq("is_new")))

    println("ANALISIS DE LA VARIABLE DAYSONMARKET:")  
    Utils.showDF("dom stats",   Utils.analyzeNumericGlobalStats(df, "daysonmarket"))
 
 

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

println("[EDA] Cargando dataset completo desde CSV o  parquet ...")
val dfcarsdataFull=Utils.loadDataParquet(spark, PATH, RAWDATA, RAWPARQUET, FORCE_CREATE_PARQUET)
val nFilasOriginal = dfcarsdataFull.count()
val nColumnas = dfcarsdataFull.columns.length

 

println("[EDA] Esquema del DataFrame:")
dfcarsdataFull.printSchema()

println("[EDA]Dimensiones del dataset:")
println(s"📌 Filas: $nFilasOriginal  |  Columnas: $nColumnas\n")
 
println("\n")
if (REALIZAR_EDA) {
  

  analisisEDA(dfcarsdataFull)
  //Utils.showTable(dfcarsdataFull, 10)
} else {
  println("=== Datos procesados cargados ===")
   Utils.showTable(dfcarsdataFull, 10)
}
 

val dfFinal =  Utils.prepararDataset(spark, dfcarsdataFull, PATH, FORCE_PREPROCESS)
 println(s"[EDA] Tratados: ${nFilasOriginal - dfFinal.count()}")
Utils.mostrarResumenFinal(dfFinal)
 