package object org {
  import org.apache.calcite.sql.SqlNode

  sealed trait Command
  case class Query(query: String) extends Command
  case class ParsedQuery(parsedQuery: SqlNode) extends Command
  case class QueryResult(data: String) extends Command

  sealed trait Database
  case class Aerospike(url: String) extends Database
}
