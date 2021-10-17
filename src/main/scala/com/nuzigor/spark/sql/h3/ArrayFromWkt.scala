/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.uber.h3core.H3Core
import com.uber.h3core.exceptions.LineUndefinedException
import com.uber.h3core.util.GeoCoord
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom._
import org.locationtech.jts.io.{ParseException, WKTReader}

import scala.collection.JavaConverters._

/**
 * Return h3 addresses from a WKT geometry
 *
 * @param wktExpr geometry in WKT format, supports POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON.
 * @param resolutionExpr h3 resolution
 */
case class ArrayFromWkt(wktExpr: Expression, resolutionExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = wktExpr
  override def right: Expression = resolutionExpr
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType)
  override def dataType: DataType = ArrayType(LongType)
  override def nullable: Boolean = true

  override protected def nullSafeEval(wktAny: Any, resolutionAny: Any): Any = {
    val wkt = wktAny.asInstanceOf[UTF8String].toString
    val resolution = resolutionAny.asInstanceOf[Int]
    try {
      val reader = new WKTReader()
      val geometry = reader.read(wkt)
      val h3Instance = H3.getInstance()
      val result = getGeometryIndices(h3Instance, geometry, resolution)
      if (result.isEmpty) {
        null
      } else {
        new GenericArrayData(result)
      }
    } catch {
      case _: ParseException => null
      case _: LineUndefinedException => null
    }
  }

  private def getGeometryIndices(h3Instance: H3Core, geometry: Geometry, resolution: Int): Seq[Long] = {
    geometry match {
      case geometry if geometry.isEmpty => Seq.empty
      case polygon: Polygon => getPolygonIndices(h3Instance, polygon, resolution)
      case point: Point => Array(h3Instance.geoToH3(point.getY, point.getX, resolution))
      case multiPoint: MultiPoint => gemMultiPointIndices(h3Instance, multiPoint, resolution)
      case multiPolygon: MultiPolygon => getMultiPolygonIndices(h3Instance, multiPolygon, resolution)
      case lineString: LineString => getLineStringIndices(h3Instance, lineString, resolution)
      case multiLineString: MultiLineString => getMultiLineStringIndices(h3Instance, multiLineString, resolution)
      case _ => Seq.empty
    }
  }

  private def getMultiLineStringIndices(h3Instance: H3Core, multiLineString: MultiLineString, resolution: Int) = {
    (0 until multiLineString.getNumGeometries)
      .map(i => multiLineString.getGeometryN(i).asInstanceOf[LineString])
      .flatMap(l => getLineStringIndices(h3Instance, l, resolution))
      .distinct
  }

  private def getMultiPolygonIndices(h3Instance: H3Core, multiPolygon: MultiPolygon, resolution: Int) = {
    (0 until multiPolygon.getNumGeometries)
      .map(i => multiPolygon.getGeometryN(i).asInstanceOf[Polygon])
      .flatMap(p => getPolygonIndices(h3Instance, p, resolution))
      .distinct
  }

  private def gemMultiPointIndices(h3Instance: H3Core, multiPoint: MultiPoint, resolution: Int): Seq[Long] = {
    (0 until multiPoint.getNumGeometries)
      .map(i => multiPoint.getGeometryN(i).getCoordinate)
      .map(c => h3Instance.geoToH3(c.y, c.x, resolution))
      .distinct
  }

  private def getPolygonIndices(h3Instance: H3Core, polygon: Polygon, resolution: Int): Seq[Long] = {
    val toGeoJavaList = (ring: LinearRing) => ring.getCoordinates.map(c => new GeoCoord(c.y, c.x)).toList.asJava
    val coordinates = toGeoJavaList(polygon.getExteriorRing)
    val holes = (0 until polygon.getNumInteriorRing).map(i => toGeoJavaList(polygon.getInteriorRingN(i))).toList.asJava
    h3Instance
      .polyfill(coordinates, holes, resolution)
      .asScala
      .map(Long2long)
  }

  private def getLineStringIndices(h3Instance: H3Core, lineString: LineString, resolution: Int): Seq[Long] = {
    val indices = lineString.getCoordinates.map(c => h3Instance.geoToH3(c.y, c.x, resolution))
    (0 until indices.length - 1)
      .flatMap(i => {
        val start = indices(i)
        val end = indices(i + 1)
        h3Instance
          .h3Line(start, end)
          .asScala
          .map(Long2long)
      })
      .distinct
  }
}
