
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import io.fabric8.kubernetes.client.KubernetesClient

object Client {

  val client :KubernetesClient = new DefaultKubernetesClient()

  def returnClient():KubernetesClient ={
    client
  }
}