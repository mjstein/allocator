import scala.collection.mutable

trait Redundancy
case class Redundancy100(nodes: Array[String],labels: Array[String]) extends Redundancy
case class Redundancy50(nodes: Array[String],labels: Array[String]) extends Redundancy


object AllocationStrategy {
  def returnDistribution(redundancyModel: Redundancy):mutable.HashMap[String,mutable.ArrayBuffer[String]]=(
    redundancyModel match {
      case Redundancy100(nodes,labels) => calculateRedundancyDistribution(nodes,labels,2,4,2)
    }
  )
  def calculate50PercentRedundancyDistribution(nodes: Array[String], labels: Array[String]):mutable.HashMap[String,mutable.ArrayBuffer[String]]={
    new mutable.HashMap[String,mutable.ArrayBuffer[String]]
    // for this strategy we group labels into triplets, each of the triplets gets associated with three nodes

    //split labels and nodes into threes


  }
  def calculateRedundancyDistribution(nodes: Array[String],labels: Array[String], labelsInGroup: Int, nodesInGroup: Int, nodeToLabelRatio: Int):mutable.HashMap[String,mutable.ArrayBuffer[String]]={
    var distribution = new mutable.HashMap[String,mutable.ArrayBuffer[String]]
    // for this strategy we group nodes into 4 nodes per 2 labels
    // Each node is able to be allocated to either of the pair

    if ((labels.size * nodeToLabelRatio) > nodes.size)
      throw new StrategyRequiresMoreNodes(s"Need at least ${(labels.size * nodeToLabelRatio) } nodes")

    val nodeGroups = nodes.grouped(nodesInGroup).toArray
    val labelGroups = labels.grouped(labelsInGroup).toArray

    for ((labelGroup,nodeGroup)<-labelGroups zip nodeGroups){
      println(s"labelGroup ${labelGroup.mkString(" ")} associated with:")
    println(s"nodeGroup ${nodeGroup.mkString(" ")}\n")

    for (node <- nodeGroup.take(labelGroup.size * nodeToLabelRatio)){
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
