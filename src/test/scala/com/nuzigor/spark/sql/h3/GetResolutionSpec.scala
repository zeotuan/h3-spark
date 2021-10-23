/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column

class GetResolutionSpec extends H3Spec {
  it should "return h3 index resolution" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_get_resolution(${h3}l)")
    val resolution = spatialDf.first().getAs[Int](0)
    assert(resolution === 10)
  }

  it should "return null for null h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_get_resolution(null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_get_resolution(-1)")
    assert(!spatialDf.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val result = df.select(h3_get_resolution(column("h3")).alias("res"))
    val resolution = result.first().getAs[Int](0)
    assert(resolution === 10)
  }

  protected override def functionName: String = "h3_get_resolution"
}
