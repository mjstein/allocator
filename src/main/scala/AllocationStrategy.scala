import scala.collection.mutable

trait Redundancy
case class Redundancy100(nodes: Array[String],labels: Array[String]) extends Redundancy
case class Redundancy50(nodes: Array[String],labels: Array[String]) extends Redundancy
case class Redundancy33(nodes: Array[String],labels: Array[String]) extends Redundancy


object AllocationStrategy {
  def returnDistribution(redundancyModel: Redundancy):mutable.HashMap[String,mutable.ArrayBuffer[String]]=(
    redundancyModel match {
      case Redundancy100(nodes,labels) => calculateRedundancyDistribution(nodes,labels,2,4,2) //labels in groups of 2 on 4 nodes each
      case Redundancy50(nodes,labels) => calculateRedundancyDistribution(nodes,labels,2,3,1.5) // labels in groups of 2 on 3 nodes each
      case Redundancy33(nodes,labels) => calculateRedundancyDistribution(nodes,labels,3,4,1.333) // labels in groups of 2 on 3 nodes each
    }
  )
  def calculateRedundancyDistribution(nodes: Array[String],labels: Array[String], labelsInGroup: Int, nodesInGroup: Int, nodeToLabelRatio: Double):mutable.HashMap[String,mutable.ArrayBuffer[String]]={
    var distribution = new mutable.HashMap[String,mutable.ArrayBuffer[String]]
    // for this strategy we group nodes into 4 nodes per 2 labels
    // Each node is able to be allocated to either of the pair

    if ((labels.size * nodeToLabelRatio) > nodes.size)
      throw new StrategyRequiresMoreNodes(s"Need at least ${(labels.size * nodeToLabelRatio) } nodes")

    val nodeGroups = nodes.grouped(nodesInGroup).toArray
    val labelGroups = labels.grouped(labelsInGroup).toArray

    for ((labelGroup,nodeGroup)<-labelGroups zip nodeGroups){

    for (node <- nodeGroup.take((labelGroup.size * nodeToLabelRatio).ceil.toInt)){
      distribution.put(node, mutable.ArrayBuffer())
      for (label <- labelGroup){
        (distribution(node))+=(label)
      }

    }
    }
    return distribution
  }
}

class StrategyRequiresMoreNodes(msg: String) extends Exception(msg)
