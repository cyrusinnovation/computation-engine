package com.cyrusinnovation.computation.persistence.reader

import org.scalatest.{Matchers, FlatSpec}
import java.io.{InputStreamReader, InputStream}

class CsvParserTest extends FlatSpec with Matchers {
  "A CSV node parser" should "generate a map of nodeId to a map of attribute name value pairs from a CSV nodes file" in {
    val inputStream: InputStream = getClass.getResourceAsStream("/sampleNodes.csv")
    val table: Map[Long, Map[String, String]] = CsvNodeFileParser.parse(new InputStreamReader(inputStream), "test", "1.0", CsvReaderConfig(1))

    table(1)("label") should be("library")
    table(1)("name") should be("test")
    table(2)("label") should be("version")
    table(2)("versionNumber") should be("1.0")
    table(2)("state") should be("Editable")
    table(2)("lastEditDate") should be("2014-04-07T09:30:10Z")
    table(4)("label") should be("simpleComputation")
    table(4)("package") should be("test.computations")
    table(4)("name") should be("MaximumTestValueComputation")
    table(4)("changedInVersion") should be("1.0")
    table(4)("description") should be("Take the maximum of the values of the testValues map")
    table(4)("computationExpression") should include("val toTestImports")
    table(7)("label") should be("imports")
    table(7)("text") should be("scala.collection.mutable.{Map => MutableMap}")
    table(32)("label") should be("ref")
    table(38)("testValues: Map[String, Int]") should be("testValues")
    table(50)("sumAccumulator") should be("addend2")
    table(54)("securityConfiguration") should be("testSecurityConfiguration")
  }

  "A CSV edge parser" should "generate a map of origin nodeId to target nodeId from a CSV edges file" in {
    //NOTE: Data file is deliberately sorted to mix up order so as to test that sequence is preserved.
    val inputStream: InputStream = getClass.getResourceAsStream("/sampleEdges.csv")
    val table = CsvEdgeFileParser.parse(new InputStreamReader(inputStream), "test", "1.0", CsvReaderConfig(1))
    table(1)  should be(List(2))
    table(2)  should be(List(3, 4, 16, 28, 42))
    table(4)  should be(List(7, 8, 10))
    table(54) should be(List(58, 61))
  }
}
