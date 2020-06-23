package org.engine

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import org.apache.calcite.sql.SqlBasicCall
import org._
import org.apache.calcite.sql.{SqlCall, SqlInsert, SqlSelect}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import org.service.AerospikeService


class AerospikeEngine(dbUrl: String) extends Engine {

  @transient
  lazy val aerospikeService: AerospikeService = AerospikeService(dbUrl)

  override def executeInsert(parsedQuery: SqlInsert): QueryResult = {

    val parsedTab = parsedQuery.getTargetTable.toString.split("\\.")

    val ns = parsedTab(0).toLowerCase()
    val set = parsedTab(1).toLowerCase()

    val binValues = parsedQuery.getSource
      .asInstanceOf[SqlBasicCall]
      .getOperandList.get(0)
      .asInstanceOf[SqlBasicCall]
      .getOperandList
      .toArray()
      .toList
      .map(_.toString.toLowerCase())

    val binNames = parsedQuery
      .getTargetColumnList
      .toString
      .replace("`","")
      .split(",")
      .toList
      .map(_.toString.toLowerCase())

    val result = aerospikeService.insert(ns, set, binNames, binValues)

    QueryResult(result)
  }

  override def executeSelect(parsedQuery: SqlSelect): QueryResult = {

    val parsedTab = parsedQuery.getFrom.toString.split("\\.")
    val ns = parsedTab(0).toLowerCase()
    val set = parsedTab(1).toLowerCase()
    val columns = parsedQuery
      .getSelectList
      .toArray
      .toList
      .map(_.toString.toLowerCase())

    if(parsedQuery.hasWhere){
      val parsedFilter = parsedQuery
        .getWhere
        .asInstanceOf[SqlBasicCall]

      val filterExp = parsedFilter
        .getOperandList
        .toArray()
        .toList
        .map(_.toString.toLowerCase)

      val filterType = parsedFilter.getKind.toString
      val result = aerospikeService.selectFilter(ns, set, columns, filterType, filterExp)
      QueryResult(result)
    } else {
      val result = aerospikeService.selectAll(ns, set, columns)
      QueryResult(result)
    }
  }



}



object AerospikeEngine {
  def apply(dbUrl: String): Props = Props(classOf[AerospikeEngine], dbUrl)
}
