import Config._
import scala.io.Source



object Test {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("last_successful_batch.txt")
    for (line <- source.getLines()) {
        println(line)
    }
    source.close()
    }
}
