package com.renault

import org.apache.spark.sql.{DataFrame, SparkSession}

object PatternMatchExcel {

  val trueString = "true"
  val falseString = "false"

  // Creating spark session variable
  val spark = SparkSession.builder().master("local").getOrCreate()

  // pattern to find email addresses
  val regxForEmail = "(?i)([a-z0-9]+.[a-z0-9]+@+[a-z]+.[a-z]{2,3}(.[a-z]{2,3})?)"

  def main(args: Array[String]): Unit = {

    // input excel file
    val input = this.getClass.getClassLoader.getResource("testInput.xlsx").getFile

    // path of output file"C:\\Users\\z024376\\Desktop\\hackathon\\output.xlsx"
    val output = this.getClass.getClassLoader.getResource("output.xlsx").getFile

    // Loading excel file into dataframe
    val df: DataFrame = readFromExcel(input)

    // Automatically identifying column which contains email
    val emailIndex = findTheEmailIndex(df)

    // filters all wrong emails and stores into a dataframe
    val newdf = filterAllWrongEmails(df, emailIndex)

    newdf.show(true)
  }

  // function to read from a excel file
  def readFromExcel(input: String): DataFrame = {

    spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", trueString)
      .option("treatEmptyValuesAsNulls", trueString)
      .option("inferSchema", trueString)
      .option("addColorColumns", falseString)
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .load(input)

  }

  // function to write to a excel file
  def writeToExcel(output: String, newdf: DataFrame): Unit = {
    newdf.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", trueString)
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("overwrite") // Optional, default: overwrite.
      .save(output)
  }

  // Main logic
  def filterAllWrongEmails(df: DataFrame, index: Int): DataFrame = df
    .filter(m => !m.getString(index).matches(regxForEmail)).toDF


  // find the rows containing wrong emails
  def findTheEmailIndex(df: DataFrame): Int = {
    var emailIndex: Int = -1 //scalastyle:ignore
    for {y <- 1 to 5; x <- df.columns.indices} {
      if (df.head(y)
        .map(m => m.get(x).toString.matches(regxForEmail))
        .foldLeft(true)(_ && _)) emailIndex = x
    }
    if (emailIndex == -1) {
      println("Invalid Search: The table does not contain Email")
      -1
    }
    else emailIndex
  }

}
