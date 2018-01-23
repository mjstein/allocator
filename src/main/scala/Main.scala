object Main extends App {

  val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy100(NodeLabelGenerator.generateNodes(8),
    NodeLabelGenerator.generateStreamLabels(3)
  ))
  println(allocationStrategy.mkString("\n"))



}


