import io.fabric8.kubernetes.api.model.{Node, NodeList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object CreateKubernetesLabel {

  def createLabelsForNode(nodeName: String, labels: ArrayBuffer[String])={
     val labelsMap: Map[String,String] = labels.map(item => (item -> "ciscoStream")).toMap
     val nodes: NodeList = Client.returnClient().nodes.list()
    def node: Node = Client.returnClient().nodes.withName(nodeName).edit()
      .editMetadata().addToLabels(labelsMap.asJava).endMetadata().done()
  }

}
