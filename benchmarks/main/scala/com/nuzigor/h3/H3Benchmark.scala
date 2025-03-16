package com.nuzigor.h3

import com.nuzigor.spark.sql.h3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ceil, col, lit, rand}
import org.apache.spark.sql.types.LongType
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

private class H3Benchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def h3BaseCellNumber(blackHole: Blackhole): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val result = spark
      .range(5000000)
      .withColumn("h3", ceil(lit(622485130170302463L) + rand(42) * 10).cast(LongType))
      .withColumn("baseCellNumber", h3.functions.h3_base_cell_number(col("h3")))
      .collect()

    blackHole.consume(result)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def h3BaseCellNumber(blackHole: Blackhole): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val result = spark
      .range(5000000)
      .withColumn("h3", ceil(lit(622485130170302463L) + rand(42) * 10).cast(LongType))
      .withColumn("baseCellNumber", h3.altfunctions.h3_base_cell_number2(col("h3")))
      .collect()

    blackHole.consume(result)
  }
}
