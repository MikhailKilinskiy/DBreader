package org

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import scala.io.StdIn._

import scala.concurrent.duration._
import scala.concurrent._


object Main extends App with LazyLogging {

  def execute(command: String): Unit = {
    implicit val timeout = Timeout(10 seconds)
    val future: Future[Any] = parser ? Query(command)
    val result = Await.result(future, timeout.duration)
    println(s"Query result: $result")
  }

  println("Insert Aerospike URL :> ")
  val aerospikeURL = readLine()

  println("Init Actor System...")
  val system = ActorSystem("test")
  val parser = system.actorOf(Parser(Aerospike(aerospikeURL)))

  println("Insert query :> ")
  var command = ""
  while ({command = readLine(); command != null}) {
    execute(command)
    println("Insert query :> ")
  }


}
