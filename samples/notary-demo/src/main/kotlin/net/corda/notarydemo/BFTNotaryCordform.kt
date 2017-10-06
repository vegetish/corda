package net.corda.notarydemo

import net.corda.cordform.CordformContext
import net.corda.cordform.CordformDefinition
import net.corda.cordform.CordformNode
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.div
import net.corda.core.internal.stream
import net.corda.core.internal.toTypedArray
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.services.transactions.BFTNonValidatingNotaryService
import net.corda.node.services.transactions.minCorrectReplicas
import net.corda.node.utilities.ServiceIdentityGenerator
import net.corda.nodeapi.internal.ServiceInfo
import net.corda.testing.ALICE
import net.corda.testing.BOB
import net.corda.testing.internal.demorun.advertisedServices
import net.corda.testing.internal.demorun.name
import net.corda.testing.internal.demorun.node
import net.corda.testing.internal.demorun.notaryClusterAddresses
import net.corda.testing.internal.demorun.rpcUsers
import net.corda.testing.internal.demorun.runNodes

fun main(args: Array<String>) = BFTNotaryCordform.runNodes()

private val clusterSize = 4 // Minimum size that tolerates a faulty replica.
private val notaryNames = createNotaryNames(clusterSize)

// This is not the intended final design for how to use CordformDefinition, please treat this as experimental and DO
// NOT use this as a design to copy.
object BFTNotaryCordform : CordformDefinition("build" / "notary-demo-nodes") {
    private val serviceType = BFTNonValidatingNotaryService.type
    private val clusterName = CordaX500Name(serviceType.id, "BFT", "Zurich", "CH")
    private val advertisedService = ServiceInfo(serviceType, clusterName)

    init {
        node {
            name(ALICE.name)
            p2pPort(10002)
            rpcPort(10003)
            rpcUsers(notaryDemoUser)
        }
        node {
            name(BOB.name)
            p2pPort(10005)
            rpcPort(10006)
        }
        val clusterAddresses = (0 until clusterSize).stream().mapToObj { NetworkHostAndPort("localhost", 11000 + it * 10) }.toTypedArray()
        fun notaryNode(replicaId: Int, configure: CordformNode.() -> Unit) = node {
            name(notaryNames[replicaId])
            advertisedServices(advertisedService)
            notaryClusterAddresses(*clusterAddresses)
            bftReplicaId(replicaId)
            configure()
        }
        notaryNode(0) {
            p2pPort(10009)
            rpcPort(10010)
        }
        notaryNode(1) {
            p2pPort(10013)
            rpcPort(10014)
        }
        notaryNode(2) {
            p2pPort(10017)
            rpcPort(10018)
        }
        notaryNode(3) {
            p2pPort(10021)
            rpcPort(10022)
        }
    }

    override fun setup(context: CordformContext) {
        ServiceIdentityGenerator.generateToDisk(notaryNames.map { context.baseDirectory(it.toString()) }, clusterName, minCorrectReplicas(clusterSize))
    }
}
