package com.renault

import org.apache.spark.sql.SparkSession
import org.scalatest._

class PatternMatchExcelTest extends FlatSpec with Matchers {

  // Creating spark session variable
  val spark = SparkSession.builder().master("local").getOrCreate()

  // Dataframe containing Wrong Emails
  val input = this.getClass.getClassLoader.getResource("testInput.xlsx").getFile
  val inputDF = PatternMatchExcel.readFromExcel(input)
  val index = PatternMatchExcel.findTheEmailIndex(inputDF)
  val wrongDF = PatternMatchExcel.filterAllWrongEmails(inputDF, index)

  // Expected Dataframe
  val output = this.getClass.getClassLoader.getResource("output.xlsx").getFile
  val outdf = PatternMatchExcel.readFromExcel(output)

  // Checking Expected output with programmes output
  val check = outdf.columns should contain theSameElementsAs wrongDF.columns


}
