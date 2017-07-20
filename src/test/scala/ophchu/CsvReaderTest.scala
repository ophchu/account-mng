package ophchu

import java.io.File


/**
  * Created by ophchu on 7/20/17.
  */
class CsvReaderTest extends AccountMngBaseTest {

  test("testReadFile") {
    val filePath = "src/test/resources/in/bank-2017-05.csv"
    val res = CsvReader.readFile(filePath)
    println(res.mkString("\n"))
  }

}
