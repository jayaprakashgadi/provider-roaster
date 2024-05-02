package com.availity.spark.provider

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, concat, count, lit, month}
import org.apache.spark.storage.StorageLevel

object ProviderRoster  {

  def main(args: Array[String]): Unit = {

    implicit val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("ProviderRoster")
      .getOrCreate()

    // don't want _committed files
    sparkSession.conf.set("parquet.enable.summary-metadata", "false")
    // don't want _SUCCESS file
    sparkSession.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    sparkSession.sparkContext.setLogLevel("ERROR")

    //do actual processing
    process()
  }

  def readProviderDs(path:String)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession
      .read
      .option("delimiter", "|")
      .option("header", "true")
      .csv(path)
      .withColumn("name", concat(col("first_name"), lit(""), col("middle_name"), lit(""), col("last_name")))
  }

  def readVisitorDs(path:String)(implicit sparkSession: SparkSession):DataFrame={
    //manually specifying the schema
    val visitorSchema = StructType(Array(
      StructField("visitor_id", StringType, true),
      StructField("provider_id", StringType, true),
      StructField("service_date", DateType, true)
    ))
    //read the visitors dataset
    sparkSession
      .read
      .schema(visitorSchema)
      .csv(path)
  }

  def getJoinDf(providerDf:DataFrame, visitorDf:DataFrame):DataFrame={
    providerDf.join(visitorDf, Seq("provider_id") , "inner")
  }


  def getVisitorsBySpecality(joindDf:DataFrame):DataFrame={
    joindDf
      .groupBy("provider_id", "provider_specialty", "name")
      .agg(count("*").alias("number_of_visits"))
  }


  def getVisitorsByDate(visitorDf:DataFrame):DataFrame={
    visitorDf
      .groupBy("provider_id", "service_date")
      .agg(count("*").alias("number_of_visits"))
  }

  def process()(implicit sparkSession: SparkSession):Unit={
    //read the provider dataset
    val providerDf = readProviderDs("data/providers.csv")
      .persist(StorageLevel.MEMORY_AND_DISK)

    //read the visitors dataset
    val visitorDf = readVisitorDs("data/visits.csv")
      .persist(StorageLevel.MEMORY_AND_DISK)

    //join provider and visitors on provider_id
    val providerVisitorJoinDf = getJoinDf(providerDf, visitorDf)

    //group by provider_id, provider_specialty, name
    val providerVisitorDf = getVisitorsBySpecality(providerVisitorJoinDf)

    //write output to json partition by provider_speciality
    providerVisitorDf
      .repartition(1)
      .withColumn("specialty", col("provider_specialty"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("provider_specialty")
      .json("data/provider_speciality")

    //group visitor count by provider by date
    val providerVisitorServiceDf = getVisitorsByDate(visitorDf)
    providerVisitorServiceDf
      .repartition(1)
      .write.mode(SaveMode.Overwrite)
      .json("data/provider_monthly")

    providerDf.unpersist()
    visitorDf.unpersist()
  }

}
