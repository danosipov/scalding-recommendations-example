import cascading.tuple.{Fields, TupleEntry}
import com.twitter.scalding._
import com.twitter.scalding.mathematics.GraphOperations._
import com.twitter.scalding.mathematics._

/**
 * Generic job setting up inputs
 */
class RecommendationJob(args: Args) extends Job(args) {
  import TDsl._

  type User = String
  type Item = Long
  type Rating = Int
  type Input = (User, Item, Rating)
  val RecommendationsPerTrack = 3

  val inputSource = args("input")
  val outputSink = args("output")

  val input: TypedPipe[Input] = TypedCsv[Input](inputSource)
  val normalizedMatrix = Matrix2(input, NoClue).rowL2Normalize
  val transposedMatrix = normalizedMatrix.transpose * normalizedMatrix

  val edges = withInNorm(transposedMatrix.toTypedPipe
    .groupBy(_._1)
    .mapValueStream { (value: Iterator[(Item, Item, Double)]) =>
      value.flatMap {
        case (item1, item2, weight) => Some(Edge(item1, item2, Weight(weight)))
      }
    }
    .values)

  val similarity = new DimsumInCosine[Long](
    minCos = 0.0001,
    delta = 0.1,
    boundedProb = 0.001)
  val result: TypedPipe[Edge[Item, Double]] = similarity(edges)

  result
    .map { edge =>
      (edge.from, edge.to, edge.data)
    }
    .groupBy(_._1)
    .sortedReverseTake(RecommendationsPerTrack + 1)(Ordering.by(_._3))
    .values
    .flatten
    .filter(tuple => tuple._1 != tuple._2)
    .write(TypedTsv(outputSink))
}
