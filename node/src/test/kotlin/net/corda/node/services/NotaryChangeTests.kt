package net.corda.node.services

import net.corda.core.contracts.*
import net.corda.core.crypto.generateKeyPair
import net.corda.core.flows.NotaryChangeFlow
import net.corda.core.flows.NotaryFlow
import net.corda.core.flows.StateReplacementException
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.node.internal.StartedNode
import net.corda.testing.*
import net.corda.testing.contracts.DummyContract
import net.corda.testing.node.MockNetwork
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.time.Instant
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NotaryChangeTests {
    lateinit var mockNet: MockNetwork
    lateinit var oldNotaryNode: StartedNode<MockNetwork.MockNode>
    lateinit var newNotaryNode: StartedNode<MockNetwork.MockNode>
    lateinit var clientNodeA: StartedNode<MockNetwork.MockNode>
    lateinit var clientNodeB: StartedNode<MockNetwork.MockNode>
    lateinit var newNotaryParty: Party
    lateinit var oldNotaryParty: Party

    @Before
    fun setUp() {
        setCordappPackages("net.corda.testing.contracts")
        mockNet = MockNetwork()
        oldNotaryNode = mockNet.createNotaryNode(legalName = DUMMY_NOTARY.name)
        clientNodeA = mockNet.createNode()
        clientNodeB = mockNet.createNode()
        newNotaryNode = mockNet.createNotaryNode()
        mockNet.runNetwork() // Clear network map registration messages
        oldNotaryNode.internals.ensureRegistered()
        newNotaryParty = newNotaryNode.info.legalIdentities[1]
        oldNotaryParty = oldNotaryNode.info.legalIdentities[1]
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()
        unsetCordappPackages()
    }

    @Test
    fun `should change notary for a state with single participant`() {
        val state = issueState(clientNodeA, oldNotaryParty)
        assertEquals(state.state.notary, oldNotaryParty)
        val newState = changeNotary(state, clientNodeA, newNotaryParty)
        assertEquals(newState.state.notary, newNotaryParty)
    }

    @Test
    fun `should change notary for a state with multiple participants`() {
        val state = issueMultiPartyState(clientNodeA, clientNodeB, oldNotaryNode, oldNotaryParty)
        val newNotary = newNotaryParty
        val flow = NotaryChangeFlow(state, newNotary)
        val future = clientNodeA.services.startFlow(flow)

        mockNet.runNetwork()

        val newState = future.resultFuture.getOrThrow()
        assertEquals(newState.state.notary, newNotary)
        val loadedStateA = clientNodeA.services.loadState(newState.ref)
        val loadedStateB = clientNodeB.services.loadState(newState.ref)
        assertEquals(loadedStateA, loadedStateB)
    }

    @Test
    fun `should throw when a participant refuses to change Notary`() {
        val state = issueMultiPartyState(clientNodeA, clientNodeB, oldNotaryNode, oldNotaryParty)
        val newEvilNotary = getTestPartyAndCertificate(CordaX500Name(organisation = "Evil R3", locality = "London", country = "GB"), generateKeyPair().public)
        val flow = NotaryChangeFlow(state, newEvilNotary.party)
        val future = clientNodeA.services.startFlow(flow)

        mockNet.runNetwork()

        assertThatExceptionOfType(StateReplacementException::class.java).isThrownBy {
            future.resultFuture.getOrThrow()
        }
    }

    @Test
    fun `should not break encumbrance links`() {
        val issueTx = issueEncumberedState(clientNodeA, oldNotaryParty)

        val state = StateAndRef(issueTx.outputs.first(), StateRef(issueTx.id, 0))
        val newNotary = newNotaryParty
        val flow = NotaryChangeFlow(state, newNotary)
        val future = clientNodeA.services.startFlow(flow)
        mockNet.runNetwork()
        val newState = future.resultFuture.getOrThrow()
        assertEquals(newState.state.notary, newNotary)

        val recordedTx = clientNodeA.services.validatedTransactions.getTransaction(newState.ref.txhash)!!
        val notaryChangeTx = recordedTx.resolveNotaryChangeTransaction(clientNodeA.services)

        // Check that all encumbrances have been propagated to the outputs
        val originalOutputs = issueTx.outputStates
        val newOutputs = notaryChangeTx.outputStates
        assertTrue(originalOutputs.minus(newOutputs).isEmpty())

        // Check that encumbrance links aren't broken after notary change
        val encumbranceLink = HashMap<ContractState, ContractState?>()
        issueTx.outputs.forEach {
            val currentState = it.data
            val encumbranceState = it.encumbrance?.let { issueTx.outputs[it].data }
            encumbranceLink[currentState] = encumbranceState
        }
        notaryChangeTx.outputs.forEach {
            val currentState = it.data
            val encumbranceState = it.encumbrance?.let { notaryChangeTx.outputs[it].data }
            assertEquals(encumbranceLink[currentState], encumbranceState)
        }
    }

    @Test
    fun `notary change and regular transactions are properly handled during resolution in longer chains`() {
        val issued = issueState(clientNodeA, oldNotaryParty)
        val moved = moveState(issued, clientNodeA, clientNodeB)

        // We don't to tx resolution when moving state to another node, so need to add the issue transaction manually
        // to node B. The resolution process is tested later during notarisation.
        clientNodeB.services.recordTransactions(clientNodeA.services.validatedTransactions.getTransaction(issued.ref.txhash)!!)

        val changedNotary = changeNotary(moved, clientNodeB, newNotaryParty)
        val movedBack = moveState(changedNotary, clientNodeB, clientNodeA)
        val changedNotaryBack = changeNotary(movedBack, clientNodeA, oldNotaryParty)

        assertEquals(issued.state, changedNotaryBack.state)
    }

    private fun changeNotary(movedState: StateAndRef<DummyContract.SingleOwnerState>, node: StartedNode<*>, newNotary: Party): StateAndRef<DummyContract.SingleOwnerState> {
        val flow = NotaryChangeFlow(movedState, newNotary)
        val future = node.services.startFlow(flow)
        mockNet.runNetwork()

        return future.resultFuture.getOrThrow()
    }

    private fun moveState(state: StateAndRef<DummyContract.SingleOwnerState>, fromNode: StartedNode<*>, toNode: StartedNode<*>): StateAndRef<DummyContract.SingleOwnerState> {
        val tx = DummyContract.move(state, toNode.info.chooseIdentity())
        val stx = fromNode.services.signInitialTransaction(tx)

        val notaryFlow = NotaryFlow.Client(stx)
        val future = fromNode.services.startFlow(notaryFlow)
        mockNet.runNetwork()

        val notarySignature = future.resultFuture.getOrThrow()
        val finalTransaction = stx + notarySignature

        fromNode.services.recordTransactions(finalTransaction)
        toNode.services.recordTransactions(finalTransaction)

        return finalTransaction.tx.outRef(0)
    }

    private fun issueEncumberedState(node: StartedNode<*>, notaryIdentity: Party): WireTransaction {
        val owner = node.info.chooseIdentity().ref(0)
        val stateA = DummyContract.SingleOwnerState(Random().nextInt(), owner.party)
        val stateB = DummyContract.SingleOwnerState(Random().nextInt(), owner.party)
        val stateC = DummyContract.SingleOwnerState(Random().nextInt(), owner.party)

        val tx = TransactionBuilder(null).apply {
            addCommand(Command(DummyContract.Commands.Create(), owner.party.owningKey))
            addOutputState(stateA, DummyContract.PROGRAM_ID, notaryIdentity, encumbrance = 2) // Encumbered by stateB
            addOutputState(stateC, DummyContract.PROGRAM_ID, notaryIdentity)
            addOutputState(stateB, DummyContract.PROGRAM_ID, notaryIdentity, encumbrance = 1) // Encumbered by stateC
        }
        val stx = node.services.signInitialTransaction(tx)
        node.services.recordTransactions(stx)
        return tx.toWireTransaction(node.services)
    }

    // TODO: Add more test cases once we have a general flow/service exception handling mechanism:
    //       - A participant is offline/can't be found on the network
    //       - The requesting party is not a participant
    //       - The requesting party wants to change additional state fields
    //       - Multiple states in a single "notary change" transaction
    //       - Transaction contains additional states and commands with business logic
    //       - The transaction type is not a notary change transaction at all.
}

fun issueState(node: StartedNode<*>, notaryIdentity: Party): StateAndRef<DummyContract.SingleOwnerState> {
    val tx = DummyContract.generateInitial(Random().nextInt(), notaryIdentity, node.info.chooseIdentity().ref(0))
    val stx = node.services.signInitialTransaction(tx)
    node.services.recordTransactions(stx)
    return stx.tx.outRef(0)
}

fun issueMultiPartyState(nodeA: StartedNode<*>, nodeB: StartedNode<*>, notaryNode: StartedNode<*>, notaryIdentity: Party): StateAndRef<DummyContract.MultiOwnerState> {
    val participants = listOf(nodeA.info.chooseIdentity(), nodeB.info.chooseIdentity())
    val state = TransactionState(
            DummyContract.MultiOwnerState(0, participants),
            DummyContract.PROGRAM_ID, notaryIdentity)
    val tx = TransactionBuilder(notary = notaryIdentity).withItems(state, dummyCommand(participants.first().owningKey))
    val signedByA = nodeA.services.signInitialTransaction(tx)
    val signedByAB = nodeB.services.addSignature(signedByA)
    val stx = notaryNode.services.addSignature(signedByAB, notaryIdentity.owningKey)
    nodeA.services.recordTransactions(stx)
    nodeB.services.recordTransactions(stx)
    return stx.tx.outRef(0)
}

fun issueInvalidState(node: StartedNode<*>, notary: Party): StateAndRef<DummyContract.SingleOwnerState> {
    val tx = DummyContract.generateInitial(Random().nextInt(), notary, node.info.chooseIdentity().ref(0))
    tx.setTimeWindow(Instant.now(), 30.seconds)
    val stx = node.services.signInitialTransaction(tx)
    node.services.recordTransactions(stx)
    return stx.tx.outRef(0)
}