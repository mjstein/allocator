object NodeLabelGenerator {
  def generateNodes(numNodes: Int, nodePrefix: String = "kubmin"): Array[String] = {
    generateGeneric(numNodes,nodePrefix)
  }

  def generateStreamLabels(numStreams: Int, streamPrefix: String = "stream"): Array[String] = {
     generateGeneric(numStreams,streamPrefix)
  }

  private def generateGeneric(num: Int, prefix: String): Array[String]={
    (1 to num).map(i => s"${prefix}${"%03d".format(i)}").toArray

  }
}
