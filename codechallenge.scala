package com.marlabs.codechallenge

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
    
object codechallenge {
  
def parse(record: String):List[(Int, String)]=
  {
    val line = record.split("}]")
    val cast = line(0)
    val crew = line(1)
    val id = line(2).split(",")(1).toInt
    val cast_list = cast.split("'name'")
    val result_cast = for (i <- 1 to cast_list.size-1)yield{
      (id, cast_list(i).split("\\'")(1))
    }
    val crew_list = crew.split("'name'")

    val result_crew = for (i <- 1 to crew_list.size-1)yield{
      (id, crew_list(i).split("\\'")(1))
    }

    val result_cast_temp = result_cast.toList
    val result_crew_temp = result_crew.toList

    result_cast_temp ++ result_crew_temp
  }


  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    
    val credit = sc.textFile("/user/maria_dev/credits.csv")
  val header = credit.first()
  val creditH =  credit.filter(x => x != header)

  

  val parsed_data = creditH.flatMap(x => parse(x)).toDF("id", "name")

  ///////////////
  val revenue = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("/user/maria_dev/movies_metadata.csv")
  val result = revenue.select("id", "revenue")
  val joinDF = parsed_data.join(result,parsed_data.col("id") === result.col("id"))

  val result1 = joinDF.drop("id")
  val finalResult = result1.withColumn("revenueInt", result1.col("revenue").cast("long"))
  val finalResult1 = finalResult.drop("revenue")
  val rQuery = finalResult1.groupBy("name").sum("revenueInt")

  rQuery.orderBy("revenueInt", ascending = False).show(5)

  spark.stop()
  }
}