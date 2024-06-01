package sda.traitement

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC(): DataFrame = {
      // Remplacer les virgules par des points et convertir en DoubleType
      val dfWithCorrectedFormat = dataFrame
        .withColumn("HTT", regexp_replace(col("HTT"), ",", ".").cast(DoubleType))
        .withColumn("TVA", regexp_replace(col("TVA"), ",", ".").cast(DoubleType))

      // Calculer TTC
      val dfWithTTC = dfWithCorrectedFormat
        .withColumn("TTC", round(col("HTT") + (col("HTT") * col("TVA")), 2))
        .drop("HTT", "TVA")

      // Retourner le DataFrame avec la colonne TTC calculée
      dfWithTTC
    }

    def extractDateEndContratVille(): DataFrame = {
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, nullable = false)
        .add("Date_End_contrat", StringType, nullable = false)

      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), nullable = true)

      dataFrame.withColumn("MetaTransaction", explode(from_json(col("metaData"), schema).getField("MetaTransaction")))
        .select(
          col("MetaTransaction.Ville").as("Ville"),
          regexp_extract(col("MetaTransaction.Date_End_contrat"), """(\d{4}-\d{2}-\d{2})""", 1).as("Date_End_contrat")
        )
        .na.drop()
    }

    def contratStatus(): DataFrame = {
      val currentDate = current_date()
      val dateEndContrat = to_date(col("Date_End_contrat"))
      val statusDataFrame = dataFrame.withColumn("Contrat_Status",
        when(dateEndContrat < currentDate, "Expired")
          .otherwise("Actif")
      )
      statusDataFrame
    }


  }

}