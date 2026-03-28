:load Utils.scala
  
    //**********************************************************************************
   // ************************************************************************************
 

  def corrSafe(df: DataFrame, colA: String, colB: String): Double = {
    val valid = Seq(colA, colB).forall(df.columns.contains)
    if (!valid) Double.NaN
    else {
      val base = df.filter(col(colA).isNotNull && col(colB).isNotNull)
      if (base.take(1).isEmpty) Double.NaN
      else df.stat.corr(colA, colB)
    }
  }

  def corrLabel(v: Double): String = {
    if (v.isNaN) "sin datos"
    else {
      math.abs(v) match {
        case x if x >= 0.6 => "fuerte"
        case x if x >= 0.3 => "moderada"
        case x if x >= 0.1 => "débil"
        case _             => "nula"
      }
    }
  }

  def printSingleCorrelationWithTarget(df: DataFrame,featureCol: String,targetCol: String = "price"): Unit = {
    val c = corrSafe(df, featureCol, targetCol)
    println(f"   - $featureCol vs $targetCol: $c%+.4f (${corrLabel(c)})")
  }

  def printPairwiseCorrelations(df: DataFrame, cols: Seq[String]): Unit = {
    val validCols = cols.filter(df.columns.contains).distinct

    if (validCols.length < 2) {
      println("   - No hay suficientes columnas válidas para calcular correlaciones cruzadas.")
      return
    }

    validCols.combinations(2).foreach {
      case Seq(c1, c2) =>
        val c = corrSafe(df, c1, c2)
        println(f"   - $c1 vs $c2: $c%+.4f (${corrLabel(c)})")
      case _ =>
    }
  }

  def printFeatureCreationInterpretation(featureName: String,semanticNote: String = "",expectedBenefit: String = ""): Unit = {
    println("\n Interpretación semántica:")
    if (semanticNote.trim.nonEmpty) println(s"   - $semanticNote")
    else println(s"   - Revisar si '$featureName' tiene sentido físico, lógico o de negocio.")

    println("\n Mejora potencial del modelo:")
    if (expectedBenefit.trim.nonEmpty) println(s"   - $expectedBenefit")
    else println(s"   - Evaluar si '$featureName' reduce ruido, resume información o captura mejor el fenómeno.")
  }

 
  def analyzeFeatureCreation(df: DataFrame,featureName: String,baseNumericCols: Seq[String],targetCol: String = "price",
  positiveOnlyForBaseStats: Boolean = false,segmentColsForTarget: Seq[String] = Seq(),minGroupSize: Int = 30,
  semanticNote: String = "",expectedBenefit: String = "",topNCategoryStats: Int = 10): Unit = {

    val validBaseCols = baseNumericCols.filter(df.columns.contains).distinct
    val validSegmentCols = segmentColsForTarget.filter(df.columns.contains).distinct

    println(s"\n                      JUSTIFICACIÓN FEATURE: $featureName \n")

    if (validBaseCols.isEmpty) {
      println("No hay columnas base válidas para analizar.")
      return
    }

    println("Relación con el target:")
    validBaseCols.foreach { c =>
      printSingleCorrelationWithTarget(df, c, targetCol)
    }

    println("\n Relación entre variables base:")
    printPairwiseCorrelations(df, validBaseCols)

    println("\n Distribución de variables base:")
    validBaseCols.foreach { c => Utils.showDF(s"Stats de $c",Utils.analyzeNumericGlobalStats(df, c, positiveOnly = positiveOnlyForBaseStats),n = 1)
    Utils.showDF(s"$c vs log($c)",Utils.analyzeNumericVsLog(df, c, positiveOnly = positiveOnlyForBaseStats),n = 1)
    }

    if (validSegmentCols.nonEmpty) {
      println("\n Comportamiento por segmentos relevantes:")
      validBaseCols.foreach { c =>
        Utils.showDF(s"$c por ${validSegmentCols.mkString(" + ")}",Utils.getNumericSegmentStats(df,numericCol = c,segmentCols = validSegmentCols,
		positiveOnly = positiveOnlyForBaseStats,minGroupSize = minGroupSize),n = topNCategoryStats
        )
      }
    }

    printFeatureCreationInterpretation(featureName = featureName,semanticNote = semanticNote,expectedBenefit = expectedBenefit)
  }

 
  def evaluateNewFeatureImpact(dfBefore: DataFrame,dfAfter: DataFrame,newCol: String,
  originalCols: Seq[String],targetCol: String = "price",positiveOnlyForNewCol: Boolean = false,
  segmentColsForNewCol: Seq[String] = Seq(),minGroupSize: Int = 30,analyzeOutliersBySegment: Boolean = false,
  outlierMinAbsoluteValue: Double = Double.MinValue,outlierIqrMultiplier: Double = 3.0,semanticConclusion: String = ""
  ): Unit = {

    val validOriginalColsBefore = originalCols.filter(dfBefore.columns.contains).distinct
    val validOriginalColsAfter  = originalCols.filter(dfAfter.columns.contains).distinct
    val validSegmentCols = segmentColsForNewCol.filter(dfAfter.columns.contains).distinct

    println(s"\n                    EVALUACIÓN FEATURE: $newCol \n")

    if (!dfAfter.columns.contains(newCol)) {
      println(s" La columna '$newCol' no existe en dfAfter.")
      return
    }

    println("Correlación con el target (antes vs después):")
    validOriginalColsBefore.foreach { c => val corrOld = corrSafe(dfBefore, c, targetCol)
	println(f"   - Base $c vs $targetCol: $corrOld%+.4f (${corrLabel(corrOld)})")}

    val corrNew = corrSafe(dfAfter, newCol, targetCol)
    println(f"   - Nueva $newCol vs $targetCol: $corrNew%+.4f (${corrLabel(corrNew)})")

    println("\n Correlación de la nueva feature con las originales:")
    validOriginalColsAfter.foreach { c => val corr = corrSafe(dfAfter, newCol, c)
      println(f"   - $newCol vs $c: $corr%+.4f (${corrLabel(corr)})")}

    println("\n Distribución de la nueva feature:")
    Utils.showDF(s"Stats de $newCol",Utils.analyzeNumericGlobalStats(dfAfter, newCol, positiveOnly = positiveOnlyForNewCol),n = 1)

    Utils.showDF(s"$newCol vs log($newCol)",Utils.analyzeNumericVsLog(dfAfter, newCol, positiveOnly = positiveOnlyForNewCol),n = 1)

  Utils.showDF(
  s"Percentiles de $newCol",
  Utils.analyzeNumericPercentiles(
    dfAfter,
    newCol,
    positiveOnly = positiveOnlyForNewCol,
    probs = Seq(0.25, 0.50, 0.75, 0.95, 0.99, 0.995, 0.999)
  ),
  n = 7
)
 
    if (validSegmentCols.nonEmpty) {
      println("\n Comportamiento segmentado de la nueva feature:")
      Utils.showDF(s"$newCol por ${validSegmentCols.mkString(" + ")}",Utils.getNumericSegmentStats(dfAfter,numericCol = newCol,
	  segmentCols = validSegmentCols,positiveOnly = positiveOnlyForNewCol,minGroupSize = minGroupSize),n = 15)
    }

    if (analyzeOutliersBySegment && validSegmentCols.nonEmpty) {
      val suspicious = Utils.detectSuspiciousNumericOutliersBySegment(dfAfter,numericCol = newCol,segmentCols = validSegmentCols,
	  positiveOnly = positiveOnlyForNewCol,minGroupSize = minGroupSize,iqrMultiplier = outlierIqrMultiplier,minAbsoluteValue = outlierMinAbsoluteValue)

      Utils.showDF(s"Outliers sospechosos de $newCol por ${validSegmentCols.mkString(" + ")}",suspicious,n = 20)

      Utils.showDF(s"Resumen de outliers sospechosos de $newCol",Utils.summarizeSuspiciousNumericOutliers(suspicious,numericCol = newCol,
	  segmentCols = validSegmentCols),n = 20)
    }

    println("\n Conclusión:")
    if (semanticConclusion.trim.nonEmpty) {
      println(s"   - $semanticConclusion")
    } else {
      println(s"   - Revisar si '$newCol' aporta una señal más interpretable o más útil que las variables originales.")
    }
  }


    // ************************************************************************************
    // ************************************************************************************