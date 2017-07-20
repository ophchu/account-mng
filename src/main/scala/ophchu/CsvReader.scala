package ophchu

import scala.util.Try

/**
  * Created by ophchu on 7/20/17.
  */
object CsvReader {

  case class BankCsvRow(date: String, transactionType: String, amountRec: Double, amountPay: Double)

  def readFile(fileName: String) = {
    val bufferedSource = scala.io.Source.fromFile(fileName)
    bufferedSource.getLines.drop(1).map(line => {
      val cols = line.split(",").map(_.trim)
      // do whatever you want with the columns here
      BankCsvRow(
        cols(0),
        cols(2),
        Try(cols(3).toDouble).getOrElse(0.0),
        Try(cols(4).toDouble).getOrElse(0.0))
    }
    )
  }
}
