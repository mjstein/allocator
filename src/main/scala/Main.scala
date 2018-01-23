import scala.collection.immutable.ListMap
import scala.collection.mutable

object Main extends App {

  val allocationStrategy = AllocationStrategy.returnDistribution(Redundancy50(NodeLabelGenerator.generateNodes(3),
    NodeLabelGenerator.generateStreamLabels(2)
  ))

  println(ListMap(allocationStrategy.toSeq.sortWith(_._1 < _._1):_*).mkString("\n"))
  for ((node,labels) <- allocationStrategy){
    CreateKubernetesLabel.createLabelsForNode(node,labels)
  }
}


