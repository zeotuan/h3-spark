/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType}

import scala.collection.JavaConverters._

/**
 * Returns h3 indices contained within of the original index and child resolution.
 *
 * @param h3Expr h3 index.
 * @param childResolutionExpr child resolution.
 */
@ExpressionDescription(
  usage = "_FUNC_(h3, resolution) - Returns h3 indices contained within of the original index and child resolution.",
  arguments = """
       Arguments:
         * h3 - parent h3 index
             622485130170302463l
         * resolution - child index resolution
             11
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l, 12);
          [626988729797644287,626988729797648383,626988729797652479,626988729797656575,626988729797660671,626988729797664767,626988729797668863]
     """,
  group = "array_funcs",
  since = "0.1.0")
case class ToChildren(h3Expr: Expression, childResolutionExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = h3Expr
  override def right: Expression = childResolutionExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, IntegerType)
  override def dataType: DataType = ArrayType(LongType)

  override protected def nullSafeEval(h3Any: Any, childResolutionAny: Any): Any = {
    val h3 = h3Any.asInstanceOf[Long]
    val childResolution = childResolutionAny.asInstanceOf[Int]
    new GenericArrayData(H3.getInstance().h3ToChildren(h3, childResolution).asScala)
  }
}
