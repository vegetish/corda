package net.corda.docs

import net.corda.core.internal.concurrent.transpose
import net.corda.core.messaging.startFlow
import net.corda.core.messaging.vaultTrackBy
import net.corda.core.node.services.Vault
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.finance.DOLLARS
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.flows.CashIssueFlow
import net.corda.finance.flows.CashPaymentFlow
import net.corda.node.services.FlowPermissions.Companion.startFlowPermission
import net.corda.nodeapi.User
import net.corda.testing.*
import net.corda.testing.driver.driver
import org.junit.Test
import kotlin.test.assertEquals

class IntegrationTestingTutorial {
    @Test
    fun `alice bob cash exchange example`() {
        // START 1
        driver(startNodesInProcess = true,
                extraCordappPackagesToScan = listOf("net.corda.finance.contracts.asset")) {
            val aliceUser = User("aliceUser", "testPassword1", permissions = setOf(
                    startFlowPermission<CashIssueFlow>(),
                    startFlowPermission<CashPaymentFlow>()
            ))
            val bobUser = User("bobUser", "testPassword2", permissions = setOf(
                    startFlowPermission<CashPaymentFlow>()
            ))
            val (alice, bob) = listOf(
                    startNode(providedName = ALICE.name, rpcUsers = listOf(aliceUser)),
                    startNode(providedName = BOB.name, rpcUsers = listOf(bobUser)),
                    startNotaryNode(DUMMY_NOTARY.name)
            ).transpose().getOrThrow()
            // END 1

            // START 2
            val aliceClient = alice.rpcClientToNode()
            val aliceProxy = aliceClient.start("aliceUser", "testPassword1").proxy

            val bobClient = bob.rpcClientToNode()
            val bobProxy = bobClient.start("bobUser", "testPassword2").proxy

            aliceProxy.waitUntilNetworkReady().getOrThrow()
            bobProxy.waitUntilNetworkReady().getOrThrow()
            // END 2

            // START 3
            val bobVaultUpdates = bobProxy.vaultTrackBy<Cash.State>().updates
            val aliceVaultUpdates = aliceProxy.vaultTrackBy<Cash.State>().updates
            // END 3

            // START 4
            val issueRef = OpaqueBytes.of(0)
            val notaryParty = aliceProxy.notaryIdentities().first()
            (1..10).map { i ->
                aliceProxy.startFlow(::CashIssueFlow,
                        i.DOLLARS,
                        issueRef,
                        notaryParty
                ).returnValue
            }.transpose().getOrThrow()
            // We wait for all of the issuances to run before we start making payments
            (1..10).map { i ->
                aliceProxy.startFlow(::CashPaymentFlow,
                        i.DOLLARS,
                        bob.nodeInfo.chooseIdentity(),
                        true
                ).returnValue
            }.transpose().getOrThrow()

            bobVaultUpdates.expectEvents {
                parallel(
                        (1..10).map { i ->
                            expect(
                                    match = { update: Vault.Update<Cash.State> ->
                                        update.produced.first().state.data.amount.quantity == i * 100L
                                    }
                            ) { update ->
                                println("Bob vault update of $update")
                            }
                        }
                )
            }
            // END 4

            // START 5
            for (i in 1..10) {
                bobProxy.startFlow(::CashPaymentFlow, i.DOLLARS, alice.nodeInfo.chooseIdentity()).returnValue.getOrThrow()
            }

            aliceVaultUpdates.expectEvents {
                sequence(
                        (1..10).map { i ->
                            expect { update: Vault.Update<Cash.State> ->
                                println("Alice got vault update of $update")
                                assertEquals(update.produced.first().state.data.amount.quantity, i * 100L)
                            }
                        }
                )
            }
            // END 5
        }
    }
}