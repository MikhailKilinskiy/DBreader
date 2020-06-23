package org

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime


trait LazyLogging extends Serializable {
  def logInfo(msg: String): Unit = {
    val dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
    val now = LocalDateTime.now().toString

    val className = getClass
    println(s"[INFO] - [$now] - [$className]: $msg")

  }

}
