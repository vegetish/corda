@file:JvmName("CordformUtils")

package net.corda.testing.internal.demorun

import net.corda.cordform.CordformDefinition
import net.corda.cordform.CordformNode
import net.corda.core.identity.CordaX500Name
import net.corda.node.services.config.NotaryConfig
import net.corda.nodeapi.User
import net.corda.nodeapi.config.toConfig
import net.corda.nodeapi.internal.ServiceInfo

fun CordformDefinition.node(configure: CordformNode.() -> Unit) {
    addNode { cordformNode -> cordformNode.configure() }
}

fun CordformNode.name(name: CordaX500Name) = name(name.toString())

fun CordformNode.rpcUsers(vararg users: User) {
    rpcUsers = users.map { it.toMap() }
}

fun CordformNode.notary(notaryConfig: NotaryConfig) {
    notary = notaryConfig.toConfig().root().unwrapped()
}

fun CordformNode.advertisedServices(vararg services: ServiceInfo) {
    advertisedServices = services.map { it.toString() }
}
