import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.collection.mutable.ArrayBuffer

class AllocationStrategyRedundancy100Spec extends FlatSpec with GivenWhenThen {
  "a 100% allocation strategy" should "a valid distribution" in {
   val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy100(NodeLabelGenerator.generateNodes(4),
     NodeLabelGenerator.generateStreamLabels(2)
   ))

    assert(allocationStrategy == Map(
      "kubmin001" -> ArrayBuffer("stream001","stream002"),
      "kubmin002" -> ArrayBuffer("stream001","stream002"),
      "kubmin003" -> ArrayBuffer("stream001","stream002"),
      "kubmin004" -> ArrayBuffer("stream001","stream002"))
    )

  }
  "a 100% allocation strategy" should "a valid distribution even with too many nodes" in {
    val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy100(NodeLabelGenerator.generateNodes(600),
      NodeLabelGenerator.generateStreamLabels(2)
    ))

    assert(allocationStrategy == Map(
      "kubmin001" -> ArrayBuffer("stream001","stream002"),
      "kubmin002" -> ArrayBuffer("stream001","stream002"),
      "kubmin003" -> ArrayBuffer("stream001","stream002"),
      "kubmin004" -> ArrayBuffer("stream001","stream002"))
    )
  }
  "a 100% allocation strategy" should "a valid distribution even with an odd number of labels" in {
    val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy100(NodeLabelGenerator.generateNodes(6),
      NodeLabelGenerator.generateStreamLabels(3)
    ))

    assert(allocationStrategy == Map(
      "kubmin001" -> ArrayBuffer("stream001","stream002"),
      "kubmin002" -> ArrayBuffer("stream001","stream002"),
      "kubmin003" -> ArrayBuffer("stream001","stream002"),
      "kubmin004" -> ArrayBuffer("stream001","stream002"),
      "kubmin005" -> ArrayBuffer("stream003"),
      "kubmin006" -> ArrayBuffer("stream003"))
    )
  }
  "a 100% allocation strategy" should "a valid distribution even with an odd number of labels and extra nodes" in {
    val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy100(NodeLabelGenerator.generateNodes(8),
      NodeLabelGenerator.generateStreamLabels(3)
    ))

    assert(allocationStrategy == Map(
      "kubmin001" -> ArrayBuffer("stream001","stream002"),
      "kubmin002" -> ArrayBuffer("stream001","stream002"),
      "kubmin003" -> ArrayBuffer("stream001","stream002"),
      "kubmin004" -> ArrayBuffer("stream001","stream002"),
      "kubmin005" -> ArrayBuffer("stream003"),
      "kubmin006" -> ArrayBuffer("stream003"))
    )
  }
  "a 100% allocation strategy" should "throw an exception if we do not provide enough nodes" in {
    assertThrows[StrategyRequiresMoreNodes]{
      AllocationStrategy.returnDistribution(Redundancy100(NodeLabelGenerator.generateNodes(1),
        NodeLabelGenerator.generateStreamLabels(2)
      ))
    }
  }
}
class AllocationStrategyRedundancy50Spec extends FlatSpec with GivenWhenThen {
  "a 50% allocation strategy" should "a valid distribution" in {
    val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy50(NodeLabelGenerator.generateNodes(3),
      NodeLabelGenerator.generateStreamLabels(2)
    ))

    assert(allocationStrategy == Map(
      "kubmin001" -> ArrayBuffer("stream001","stream002"),
      "kubmin002" -> ArrayBuffer("stream001","stream002"),
      "kubmin003" -> ArrayBuffer("stream001","stream002")
    ))

  }
  "a 50% allocation strategy" should "a valid distribution even with too many nodes" in {
    val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy50(NodeLabelGenerator.generateNodes(600),
      NodeLabelGenerator.generateStreamLabels(2)
    ))
    assert(allocationStrategy == Map(
      "kubmin001" -> ArrayBuffer("stream001","stream002"),
      "kubmin002" -> ArrayBuffer("stream001","stream002"),
      "kubmin003" -> ArrayBuffer("stream001","stream002")
    ))

  }
  "a 50% allocation strategy" should "a valid distribution even with an odd number of labels" in {
    val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy50(NodeLabelGenerator.generateNodes(5),
      NodeLabelGenerator.generateStreamLabels(3)
    ))
    assert(allocationStrategy == Map(
      "kubmin001" -> ArrayBuffer("stream001","stream002"),
      "kubmin002" -> ArrayBuffer("stream001","stream002"),
      "kubmin003" -> ArrayBuffer("stream001","stream002"),
      "kubmin004" -> ArrayBuffer("stream003"),
      "kubmin005" -> ArrayBuffer("stream003")
    ))

  }
  "a 50% allocation strategy" should "a valid distribution even with an odd number of labels and extra nodes" in {
    val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy50(NodeLabelGenerator.generateNodes(8),
      NodeLabelGenerator.generateStreamLabels(3)
    ))
    assert(allocationStrategy == Map(
      "kubmin001" -> ArrayBuffer("stream001","stream002"),
      "kubmin002" -> ArrayBuffer("stream001","stream002"),
      "kubmin003" -> ArrayBuffer("stream001","stream002"),
      "kubmin004" -> ArrayBuffer("stream003"),
      "kubmin005" -> ArrayBuffer("stream003")
    ))

  }
  "a 5% allocation strategy" should "throw an exception if we do not provide enough nodes" in {
    assertThrows[StrategyRequiresMoreNodes]{
      AllocationStrategy.returnDistribution(Redundancy50(NodeLabelGenerator.generateNodes(1),
        NodeLabelGenerator.generateStreamLabels(2)
      ))
    }
  }
}
