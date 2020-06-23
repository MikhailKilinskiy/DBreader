package org

import akka.actor.{Actor, Props}
import akka.pattern.pipe
//import com.typesafe.scalalogging.LazyLogging
import org._
import org.apache.calcite.sql.{SqlSelect, SqlInsert, SqlNode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait Engine extends Actor {

  def executeInsert(parsedQuery: SqlInsert): QueryResult
  def executeSelect(parsedQuery: SqlSelect): QueryResult

 def executeQuery(parsedQuery: SqlNode): Try[QueryResult] = {
    Try {
      parsedQuery match {
        case parsedQuery: SqlSelect => executeSelect(parsedQuery.asInstanceOf[SqlSelect])
        case parsedQuery: SqlInsert => executeInsert(parsedQuery.asInstanceOf[SqlInsert])
      }
    }
  }

  override def receive: Receive = {
    case ParsedQuery(null) =>
      QueryResult(null)
    case ParsedQuery(parsedQuery) =>
      Future {
        executeQuery(parsedQuery) match {
          case Success(result) => result
          case Failure(ex) => QueryResult("Empty result")
        }
      }
        .pipeTo(sender)
        .onComplete {
          case Success(res: QueryResult) =>
            sender ! res
          case Failure(ex) =>
            sender ! QueryResult(null)
        }
  }


}
