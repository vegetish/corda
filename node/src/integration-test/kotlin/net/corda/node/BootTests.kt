package net.corda.node

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.internal.div
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StartableByRPC
import net.corda.core.messaging.startFlow
import net.corda.core.utilities.getOrThrow
import net.corda.testing.ALICE
import net.corda.node.internal.NodeStartup
import net.corda.node.services.FlowPermissions.Companion.startFlowPermission
import net.corda.nodeapi.internal.ServiceInfo
import net.corda.nodeapi.internal.ServiceType
import net.corda.nodeapi.User
import net.corda.testing.driver.ListenProcessDeathException
import net.corda.testing.driver.NetworkMapStartStrategy
import net.corda.testing.ProjectStructure.projectRootDir
import net.corda.testing.driver.driver
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Ignore
import org.junit.Test
import java.io.*
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BootTests {

    @Test
    fun `java deserialization is disabled`() {
        driver {
            val user = User("u", "p", setOf(startFlowPermission<ObjectInputStreamFlow>()))
            val future = startNode(rpcUsers = listOf(user)).getOrThrow().rpcClientToNode().
                    start(user.username, user.password).proxy.startFlow(::ObjectInputStreamFlow).returnValue
            assertThatThrownBy { future.getOrThrow() }.isInstanceOf(InvalidClassException::class.java).hasMessage("filter status: REJECTED")
        }
    }

    @Test
    fun `double node start doesn't write into log file`() {
        val logConfigFile = projectRootDir / "config" / "dev" / "log4j2.xml"
        assertThat(logConfigFile).isRegularFile()
        driver(isDebug = true, systemProperties = mapOf("log4j.configurationFile" to logConfigFile.toString())) {
            val alice = startNode(providedName = ALICE.name).get()
            val logFolder = alice.configuration.baseDirectory / NodeStartup.LOGS_DIRECTORY_NAME
            val logFile = logFolder.toFile().listFiles { _, name -> name.endsWith(".log") }.single()
            // Start second Alice, should fail
            assertThatThrownBy {
                startNode(providedName = ALICE.name).getOrThrow()
            }
            // We count the number of nodes that wrote into the logfile by counting "Logs can be found in"
            val numberOfNodesThatLogged = Files.lines(logFile.toPath()).filter { NodeStartup.LOGS_CAN_BE_FOUND_IN_STRING in it }.count()
            assertEquals(1, numberOfNodesThatLogged)
        }
    }

    @Ignore("Need rewriting to produce too big network map registration (adverticed services trick doesn't work after services removal).")
    @Test
    fun `node quits on failure to register with network map`() {
        val tooManyAdvertisedServices = (1..100).map { ServiceInfo(ServiceType.notary.getSubType("$it")) }.toSet()
        driver(networkMapStartStrategy = NetworkMapStartStrategy.Nominated(ALICE.name)) {
            val future = startNode(providedName = ALICE.name, advertisedServices = tooManyAdvertisedServices)
            assertFailsWith(ListenProcessDeathException::class) { future.getOrThrow() }
        }
    }
}

@StartableByRPC
class ObjectInputStreamFlow : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        val data = ByteArrayOutputStream().apply { ObjectOutputStream(this).use { it.writeObject(object : Serializable {}) } }.toByteArray()
        ObjectInputStream(data.inputStream()).use { it.readObject() }
    }
}
