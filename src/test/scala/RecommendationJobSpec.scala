import cascading.tuple.Fields
import com.twitter.scalding._
import org.scalatest.{Matchers, FunSuite}

import scala.collection.mutable
import scala.io.Source

class RecommendationJobSpec extends FunSuite with Matchers with FieldConversions {
  type Input = (String, Long, Int)
  val input = Source.fromFile("src/test/resources/input.txt").getLines()
    .map { inputLine =>
      val splitLine = inputLine.split(",")
      (splitLine(0), splitLine(1), splitLine(2))
    }
    .toList

  def validateRecommendations(recs: Map[Long, List[Long]]) = {
    recs.size should be(5)
    recs(123) should be(List(234, 345, 567))
    recs(456) should be(List(567, 123, 234))
    recs(567) should be(List(456, 123, 234))
  }

  test("Dimsum Job should produce correct results") {
    JobTest("RecommendationJob")
      .arg("input", "input")
      .arg("output", "output")
      .source(TypedCsv[Input]("input"), input)
      .sink[(Long, Long, Double)](TypedTsv[(Long, Long, Double)]("output")) { outputBuffer =>
        outputBuffer.foreach(println)
        validateRecommendations(
          outputBuffer.groupBy(_._1).mapValues { value =>
            value.toList.sortBy(_._3)(Ordering[Double].reverse)
              .map(_._2)
          }
        )
      }
      .run
      .finish
  }
}
