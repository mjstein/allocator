import org.scalatest.{FlatSpec, GivenWhenThen}

class NodeLabelGeneratorSpec extends FlatSpec with GivenWhenThen {
  "a requested array list" should "return a valid array of nodes" in {
   val nlg =  NodeLabelGenerator.generateNodes(2)
    assert(nlg.length == 2)
    assert(nlg(0) == "kubmin001")
    assert(nlg(1) == "kubmin002")
  }
  "a requested array list" should "return a valid array of stream labels" in {
    val slg =  NodeLabelGenerator.generateStreamLabels(2)
    assert(slg.length == 2)
    assert(slg(0) == "stream001")
    assert(slg(1) == "stream002")
  }
}
