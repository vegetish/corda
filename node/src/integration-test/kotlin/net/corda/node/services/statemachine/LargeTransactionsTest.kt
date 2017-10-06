package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.*
import net.corda.core.internal.InputStreamAndHash
import net.corda.core.messaging.startFlow
import net.corda.core.transactions.TransactionBuilder
import net.corda.testing.BOB
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.aliceBobAndNotary
import net.corda.testing.contracts.DummyContract
import net.corda.testing.contracts.DummyState
import net.corda.testing.driver.driver
import net.corda.testing.dummyCommand
import org.junit.Test
import kotlin.test.assertEquals

/**
 * Check that we can add lots of large attachments to a transaction and that it works OK, e.g. does not hit the
 * transaction size limit (which should only consider the hashes).
 */
class LargeTransactionsTest {
    @StartableByRPC
    @InitiatingFlow
    class SendLargeTransactionFlow(private val hash1: SecureHash,
                                   private val hash2: SecureHash,
                                   private val hash3: SecureHash,
                                   private val hash4: SecureHash) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val tx = TransactionBuilder(notary = DUMMY_NOTARY)
                    .addOutputState(DummyState(), DummyContract.PROGRAM_ID)
                    .addCommand(dummyCommand(ourIdentity.owningKey))
                    .addAttachment(hash1)
                    .addAttachment(hash2)
                    .addAttachment(hash3)
                    .addAttachment(hash4)
            val stx = serviceHub.signInitialTransaction(tx, ourIdentity.owningKey)
            // Send to the other side and wait for it to trigger resolution from us.
            val bob = serviceHub.identityService.wellKnownPartyFromX500Name(BOB.name)!!
            val bobSession = initiateFlow(bob)
            subFlow(SendTransactionFlow(bobSession, stx))
            bobSession.receive<Unit>()
        }
    }

    @InitiatedBy(SendLargeTransactionFlow::class)
    @Suppress("UNUSED")
    class ReceiveLargeTransactionFlow(private val otherSide: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            subFlow(ReceiveTransactionFlow(otherSide))
            // Unblock the other side by sending some dummy object (Unit is fine here as it's a singleton).
            otherSide.send(Unit)
        }
    }

    @Test
    fun checkCanSendLargeTransactions() {
        // These 4 attachments yield a transaction that's got >10mb attached, so it'd push us over the Artemis
        // max message size.
        val bigFile1 = InputStreamAndHash.createInMemoryTestZip(1024 * 1024 * 3, 0)
        val bigFile2 = InputStreamAndHash.createInMemoryTestZip(1024 * 1024 * 3, 1)
        val bigFile3 = InputStreamAndHash.createInMemoryTestZip(1024 * 1024 * 3, 2)
        val bigFile4 = InputStreamAndHash.createInMemoryTestZip(1024 * 1024 * 3, 3)
        driver(startNodesInProcess = true, extraCordappPackagesToScan = listOf("net.corda.testing.contracts")) {
            val (alice, _, _) = aliceBobAndNotary()
            alice.useRPC {
                val hash1 = it.uploadAttachment(bigFile1.inputStream)
                val hash2 = it.uploadAttachment(bigFile2.inputStream)
                val hash3 = it.uploadAttachment(bigFile3.inputStream)
                val hash4 = it.uploadAttachment(bigFile4.inputStream)
                assertEquals(hash1, bigFile1.sha256)
                // Should not throw any exceptions.
                it.startFlow(::SendLargeTransactionFlow, hash1, hash2, hash3, hash4).returnValue.get()
            }
        }
    }
}