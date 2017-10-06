package net.corda.nodeapi.internal

import net.corda.core.contracts.*
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.testing.*
import net.corda.testing.node.MockServices
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class AttachmentsClassLoaderStaticContractTests : TestDependencyInjectionBase() {

    class AttachmentDummyContract : Contract {
        companion object {
            private val ATTACHMENT_PROGRAM_ID = "net.corda.nodeapi.internal.AttachmentsClassLoaderStaticContractTests\$AttachmentDummyContract"
        }

        data class State(val magicNumber: Int = 0) : ContractState {
            override val participants: List<AbstractParty>
                get() = listOf()
        }

        interface Commands : CommandData {
            class Create : TypeOnlyCommandData(), Commands
        }

        override fun verify(tx: LedgerTransaction) {
            // Always accepts.
        }

        fun generateInitial(owner: PartyAndReference, magicNumber: Int, notary: Party): TransactionBuilder {
            val state = State(magicNumber)
            return TransactionBuilder(notary)
                    .withItems(StateAndContract(state, ATTACHMENT_PROGRAM_ID), Command(Commands.Create(), owner.party.owningKey))
        }
    }

    private lateinit var serviceHub: MockServices

    @Before
    fun `create service hub`() {
        serviceHub = MockServices(cordappPackages = listOf("net.corda.nodeapi.internal"))
    }

    @After
    fun `clear packages`() {
        unsetCordappPackages()
    }

    @Test
    fun `test serialization of WireTransaction with statically loaded contract`() {
        val tx = AttachmentDummyContract().generateInitial(MEGA_CORP.ref(0), 42, DUMMY_NOTARY)
        val wireTransaction = tx.toWireTransaction(serviceHub)
        val bytes = wireTransaction.serialize()
        val copiedWireTransaction = bytes.deserialize()

        assertEquals(1, copiedWireTransaction.outputs.size)
        assertEquals(42, (copiedWireTransaction.outputs[0].data as AttachmentDummyContract.State).magicNumber)
    }

    @Test
    fun `verify that contract DummyContract is in classPath`() {
        val contractClass = Class.forName("net.corda.nodeapi.internal.AttachmentsClassLoaderStaticContractTests\$AttachmentDummyContract")
        val contract = contractClass.newInstance() as Contract

        assertNotNull(contract)
    }
}