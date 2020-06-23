package org

import akka.actor.{Actor, Props}
import akka.pattern._
import org.apache.calcite.sql.{SqlSelect, SqlInsert, SqlCall, SqlNode}
import org.apache.calcite.sql.parser.SqlParser

import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent._
import org.engine.AerospikeEngine


class Parser[T <: Database](db: Database) extends Actor with LazyLogging {

  implicit val timeout: Timeout = Timeout(3 seconds)

  def parse(query: String): Option[SqlNode] = {
    val p = SqlParser.create(query).parseQuery()
    try {
      Some(p)
    } catch {
      case e: Exception =>
        logInfo(e.toString)
        None
    }
  }

  val engine = db match {
    case Aerospike(url) =>
      context.actorOf(AerospikeEngine(url))
  }

  def initialized: Receive = {
    case Query(null) =>
      logInfo("Empty query")
    case Query(query) =>
      //logInfo("Process query...")
      val res = parse(query) match {
        case Some(r) => ParsedQuery(r)
        case None => ParsedQuery(null)
      }
      val future = engine ? res
      val result = Await.result(future, timeout.duration)
      sender ! result
  }

  override def receive: Receive = initialized

}

object Parser {
  def apply(db: Database): Props = Props(new Parser(db))
}
