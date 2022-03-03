package sparkday37

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import sys.process._

object sparkobj {
  
  	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
					val spark =SparkSession.builder().getOrCreate()
					import spark.implicits._

					val df = spark.read.format("csv").option("header","true").load("file:///E:/Project 2021-22/data/txns")
					df.show(5,false)

					val df2 = df.filter(!col("category").isin("Gymnastics","Team Sports"))




	}
}
  
