/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

import java.util
import scala.collection.JavaConverters._

/**
 * Compacts the set of indices as best as possible.
 *
 * @param h3Expr h3 indices.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the compacted `expr` array on indices.",
  arguments = """
       Arguments:
         * expr - array of h3 indices
             array(622485130170302463l, 622485130170957823l)
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(h3_k_ring(622485130170302463l, 3));
          [622485130170761215,622485130170793983,622485130171482111,622485130151526399,...]
         > SELECT _FUNC_(array(0));
          []
     """,
  group = "array_funcs",
  since = "0.1.0")
case class Compact(h3Expr: Expression,
                   failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends UnaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def this(h3Expr: Expression) = this(h3Expr, SQLConf.get.ansiEnabled)

  override def child: Expression = h3Expr
  override def inputTypes: Seq[DataType] = Seq(ArrayType(LongType))
  override def dataType: DataType = ArrayType(LongType, containsNull = false)
  override def nullable: Boolean = if (failOnError) child.nullable || child.dataType.asInstanceOf[ArrayType].containsNull else true

  override protected def nullSafeEval(h3Any: Any): Any = {
    val list = new util.ArrayList[java.lang.Long]()
    var nullFound = false
    h3Any.asInstanceOf[ArrayData].foreach(LongType, (_, v) =>
      if (v == null) {
        nullFound = true
      } else {
        list.add(v.asInstanceOf[Long])
      }
    )

    if (nullFound) {
      null
    } else {
      try {
        new GenericArrayData(H3.getInstance().compact(list).asScala)
      } catch {
        case _: IllegalArgumentException if !failOnError => null
      }
    }
  }
}
