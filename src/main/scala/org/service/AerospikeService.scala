package org.service

import com.aerospike.client.{AerospikeClient, Bin, Key, Host, Record, ScanCallback}
import com.aerospike.client.query._
import com.aerospike.client.policy._
import org.LazyLogging
import collection.JavaConverters._

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class AerospikeService(url: String) extends LazyLogging {

  protected val policy = {
    val policy = new ClientPolicy()
    policy.writePolicyDefault.commitLevel = CommitLevel.COMMIT_MASTER
    policy.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE
    policy
  }

  @transient
  protected lazy val client = {
    val splittedUrl = url.split(":")
    var host: String = ""
    var port: Int = 0

    if (!url.startsWith("http")) {
      host = splittedUrl(0)
      port = splittedUrl(1).toInt
    } else {
      host = s"${splittedUrl(0)}:${splittedUrl(1)}"
      port = splittedUrl(2).toInt
    }

    new AerospikeClient(policy, Host.parseHosts(host, port):_*)
  }

  private def tryCast(x: Any): Either[Int, String] = {
    try {
      Left(x.toString.toInt)
    } catch {
      case ex: Exception => Left(None)
    }
    Right(x.toString.toLowerCase)
  }


  def insert(ns: String,
             set: String,
             binNames: List[String],
             binValues: List[_]): String = {

    val key: Key = new Key(ns, set, binValues.head.toString)
    val bins: ListBuffer[Bin] = new ListBuffer[Bin]()
    val data = binNames.zip(binValues)
    val writePolicy = new WritePolicy()
    writePolicy.commitLevel = CommitLevel.COMMIT_MASTER
    writePolicy.sendKey = true

    data.foreach{row =>
      val col = row._1
      val value = tryCast(row._2) match {
        case Left(x) => x
        case Right(y) => y
        }
      bins += new Bin(col, value)
      }

    Try(client.put(writePolicy, key, bins:_*)) match {
      case Success(_) =>
        "Row inserted"
      case Failure(ex) =>
        logInfo(ex.getLocalizedMessage)
        throw ex
      }
    }

  def deleteAll(ns: String,
             set: String): String = {

    val scanPolicy: ScanPolicy = new ScanPolicy()
    client.scanAll(scanPolicy, ns, set, new ScanCallback {
      override def scanCallback(key: Key, record: Record): Unit = {
        client.delete(new WritePolicy(), key)
      }
    })
    "Rows deleted"
  }


  private def processRecord(key: Key, record: Record): String = {
    val recordKey = key.userKey.toString
    val binNames = record.bins.asScala.keys
    val values = new ListBuffer[String]()

    binNames.foreach {name =>
      val binValue = record.bins.getOrDefault(name,"").toString
      values += s"[name: $name value: $binValue]"
    }
    s"key: $recordKey ${values.mkString(" ")} "
  }


  def selectAll(ns: String,
              set: String,
              columns: List[String]): String = {

    val scanPolicy: ScanPolicy = new ScanPolicy()
    scanPolicy.includeBinData = true
    val rows: ListBuffer[String] = new ListBuffer[String]()

    val scanCallback = new ScanCallback {
      override def scanCallback(key: Key, record: Record): Unit = {
        rows += processRecord(key, record)
      }
    }

    if(columns.size == 1 && columns.head.toString.equals("*")) {
      client.scanAll(scanPolicy, ns, set, scanCallback)
    } else {
      client.scanAll(scanPolicy, ns, set, scanCallback, columns: _*)
    }
    rows.mkString("\n")
  }


  def selectFilter(ns: String,
                   set: String,
                   columns: List[String],
                   filterType: String,
                   filterExp: List[String]): String = {

    val filterBin = filterExp.head

    filterType match {
      case "EQUALS" =>
        val task = client.createIndex(null, ns, set,
          "test_index", filterBin, IndexType.STRING)
        task.waitTillComplete(100)
      case "BETWEEN" =>
        val task = client.createIndex(null, ns, set,
          "test_index", filterBin, IndexType.NUMERIC)
        task.waitTillComplete(100)
      case _ =>
        throw new Exception("Filter expression must be EQUALS or BETWEEN")
    }

    val stmt = new Statement()
    stmt.setNamespace(ns)
    stmt.setSetName(set)
    stmt.setIndexName("test_index")

    if(!(columns.size == 1 && columns.head.toString.equals("*"))) {
      stmt.setBinNames(columns:_*)
    }

    var filter: Filter = null

    filterType match {
      case "EQUALS" =>
        filter = Filter.equal(filterBin, filterExp(1))
      case "BETWEEN" =>
        filter = Filter.range(filterBin, filterExp(1).toLong, filterExp(2).toLong)
      case _ =>
        throw new Exception("Filter expression must be EQUALS or BETWEEN")
    }

    stmt.setFilter(filter)

    val rs = client.query(null, stmt)
    val rows: ListBuffer[String] = new ListBuffer[String]()

    while(rs.iterator.hasNext){
      val record = rs.getRecord
      val key = rs.getKey
      rows += processRecord(key, record)
    }

    val dropTask = client.dropIndex(null, ns, set,"test_index")
    dropTask.waitTillComplete(100)

    rows.mkString("\n")

  }
}




object AerospikeService {
  def apply(url: String): AerospikeService = new AerospikeService(url)
}
