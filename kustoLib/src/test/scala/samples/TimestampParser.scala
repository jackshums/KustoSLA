package samples

import scala.collection.mutable.Stack
import org.scalatest.Assertions
import org.junit.Test
import com.microsoft.adf.kusto._

class TimestampParser extends Assertions {
  @Test def testParseTimestamp() = {
    val timeStr = "2017-12-08T23:47:02.83198Z"
    TypeLib.toTimestamp(timeStr)
  }
}