/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}

object altfunctions {
  private def withExpr(expr: Expression): Column = new Column(expr)
  /**
   * Returns the base cell number for h3 index
   *
   * @param h3 h3 index
   * @return base cell number of h3 index
   */
  def h3_base_cell_number2(h3: Column): Column = withExpr {
    GetBaseCellNumber2(h3.expr)
  }
}
