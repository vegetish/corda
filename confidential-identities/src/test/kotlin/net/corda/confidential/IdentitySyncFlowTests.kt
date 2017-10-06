package net.corda.confidential

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.identity.Party
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.unwrap
import net.corda.finance.DOLLARS
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.flows.CashIssueAndPaymentFlow
import net.corda.finance.flows.CashPaymentFlow
import net.corda.testing.*
import net.corda.testing.node.MockNetwork
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class IdentitySyncFlowTests {
    lateinit var mockNet: MockNetwork

    @Before
    fun before() {
        setCordappPackages("net.corda.finance.contracts.asset")
        // We run this in parallel threads to help catch any race conditions that may exist.
        mockNet = MockNetwork(networkSendManuallyPumped = false, threadPerNode = true)
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()
        unsetCordappPackages()
    }

    @Test
    fun `sync confidential identities`() {
        // Set up values we'll need
        val notaryNode = mockNet.createNotaryNode()
        val aliceNode = mockNet.createPartyNode(ALICE_NAME)
        val bobNode = mockNet.createPartyNode(BOB_NAME)
        val alice: Party = notaryNode.services.networkMapCache.getPeerByLegalName(ALICE_NAME)!!
        val bob: Party = notaryNode.services.networkMapCache.getPeerByLegalName(BOB_NAME)!!
        val notary = notaryNode.services.getDefaultNotary()
        bobNode.internals.registerInitiatedFlow(Receive::class.java)

        // Alice issues then pays some cash to a new confidential identity that Bob doesn't know about
        val anonymous = true
        val ref = OpaqueBytes.of(0x01)
        val issueFlow = aliceNode.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, alice, anonymous, notary))
        val issueTx = issueFlow.resultFuture.getOrThrow().stx
        val confidentialIdentity = issueTx.tx.outputs.map { it.data }.filterIsInstance<Cash.State>().single().owner
        assertNull(bobNode.database.transaction { bobNode.services.identityService.wellKnownPartyFromAnonymous(confidentialIdentity) })

        // Run the flow to sync up the identities
        aliceNode.services.startFlow(Initiator(bob, issueTx.tx)).resultFuture.getOrThrow()
        val expected = aliceNode.database.transaction {
            aliceNode.services.identityService.wellKnownPartyFromAnonymous(confidentialIdentity)
        }
        val actual = bobNode.database.transaction {
            bobNode.services.identityService.wellKnownPartyFromAnonymous(confidentialIdentity)
        }
        assertEquals(expected, actual)
    }

    @Test
    fun `don't offer other's identities confidential identities`() {
        // Set up values we'll need
        val notaryNode = mockNet.createNotaryNode()
        val aliceNode = mockNet.createPartyNode(ALICE_NAME)
        val bobNode = mockNet.createPartyNode(BOB_NAME)
        val charlieNode = mockNet.createPartyNode(CHARLIE_NAME)
        val alice: Party = notaryNode.services.networkMapCache.getPeerByLegalName(ALICE_NAME)!!
        val bob: Party = notaryNode.services.networkMapCache.getPeerByLegalName(BOB_NAME)!!
        val charlie: Party = notaryNode.services.networkMapCache.getPeerByLegalName(CHARLIE_NAME)!!
        val notary = notaryNode.services.getDefaultNotary()
        bobNode.internals.registerInitiatedFlow(Receive::class.java)

        // Charlie issues then pays some cash to a new confidential identity
        val anonymous = true
        val ref = OpaqueBytes.of(0x01)
        val issueFlow = charlieNode.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, charlie, anonymous, notary))
        val issueTx = issueFlow.resultFuture.getOrThrow().stx
        val confidentialIdentity = issueTx.tx.outputs.map { it.data }.filterIsInstance<Cash.State>().single().owner
        val confidentialIdentCert = charlieNode.services.identityService.certificateFromKey(confidentialIdentity.owningKey)!!

        // Manually inject this identity into Alice's database so the node could leak it, but we prove won't
        aliceNode.database.transaction { aliceNode.services.identityService.verifyAndRegisterIdentity(confidentialIdentCert) }
        assertNotNull(aliceNode.database.transaction { aliceNode.services.identityService.wellKnownPartyFromAnonymous(confidentialIdentity) })

        // Generate a payment from Charlie to Alice, including the confidential state
        val payTx = charlieNode.services.startFlow(CashPaymentFlow(1000.DOLLARS, alice, anonymous)).resultFuture.getOrThrow().stx

        // Run the flow to sync up the identities, and confirm Charlie's confidential identity doesn't leak
        assertNull(bobNode.database.transaction { bobNode.services.identityService.wellKnownPartyFromAnonymous(confidentialIdentity) })
        aliceNode.services.startFlow(Initiator(bob, payTx.tx)).resultFuture.getOrThrow()
        assertNull(bobNode.database.transaction { bobNode.services.identityService.wellKnownPartyFromAnonymous(confidentialIdentity) })
    }

    /**
     * Very lightweight wrapping flow to trigger the counterparty flow that receives the identities.
     */
    @InitiatingFlow
    class Initiator(val otherSide: Party, val tx: WireTransaction): FlowLogic<Boolean>() {
        @Suspendable
        override fun call(): Boolean {
            val session = initiateFlow(otherSide)
            subFlow(IdentitySyncFlow.Send(session, tx))
            // Wait for the counterparty to indicate they're done
            return session.receive<Boolean>().unwrap { it }
        }
    }

    @InitiatedBy(IdentitySyncFlowTests.Initiator::class)
    class Receive(val otherSideSession: FlowSession): FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            subFlow(IdentitySyncFlow.Receive(otherSideSession))
            // Notify the initiator that we've finished syncing
            otherSideSession.send(true)
        }
    }
}