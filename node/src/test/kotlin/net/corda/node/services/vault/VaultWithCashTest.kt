package net.corda.node.services.vaultService

import net.corda.core.contracts.ContractState
import net.corda.core.contracts.LinearState
import net.corda.core.contracts.UniqueIdentifier
import net.corda.core.identity.AnonymousParty
import net.corda.core.node.services.Vault
import net.corda.core.node.services.VaultService
import net.corda.core.node.services.queryBy
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.node.services.vault.QueryCriteria.VaultQueryCriteria
import net.corda.core.transactions.TransactionBuilder
import net.corda.finance.*
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.contracts.asset.Cash.Companion.generateSpend
import net.corda.finance.contracts.asset.DUMMY_CASH_ISSUER
import net.corda.finance.contracts.asset.DUMMY_CASH_ISSUER_KEY
import net.corda.finance.contracts.getCashBalance
import net.corda.finance.schemas.CashSchemaV1
import net.corda.node.utilities.CordaPersistence
import net.corda.testing.*
import net.corda.testing.contracts.*
import net.corda.testing.node.MockServices
import net.corda.testing.node.MockServices.Companion.makeTestDatabaseAndMockServices
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.assertEquals

// TODO: Move this to the cash contract tests once mock services are further split up.

class VaultWithCashTest : TestDependencyInjectionBase() {
    lateinit var services: MockServices
    lateinit var issuerServices: MockServices
    val vaultService: VaultService get() = services.vaultService
    lateinit var database: CordaPersistence
    lateinit var notaryServices: MockServices

    @Before
    fun setUp() {
        setCordappPackages("net.corda.testing.contracts", "net.corda.finance.contracts.asset")

        LogHelper.setLevel(VaultWithCashTest::class)
        val databaseAndServices = makeTestDatabaseAndMockServices(keys = listOf(DUMMY_CASH_ISSUER_KEY, DUMMY_NOTARY_KEY),
                customSchemas = setOf(CashSchemaV1))
        database = databaseAndServices.first
        services = databaseAndServices.second
        issuerServices = MockServices(DUMMY_CASH_ISSUER_KEY, MEGA_CORP_KEY)
        notaryServices = MockServices(DUMMY_NOTARY_KEY)
    }

    @After
    fun tearDown() {
        LogHelper.reset(VaultWithCashTest::class)
        database.close()
        unsetCordappPackages()
    }

    @Test
    fun splits() {
        database.transaction {
            // Fix the PRNG so that we get the same splits every time.
            services.fillWithSomeTestCash(100.DOLLARS, issuerServices, DUMMY_NOTARY, 3, 3, Random(0L), issuedBy = DUMMY_CASH_ISSUER)
        }
        database.transaction {
            val w = vaultService.queryBy<Cash.State>().states
            assertEquals(3, w.size)

            val state = w[0].state.data
            assertEquals(30.45.DOLLARS `issued by` DUMMY_CASH_ISSUER, state.amount)
            assertEquals(services.key.public, state.owner.owningKey)

            assertEquals(34.70.DOLLARS `issued by` DUMMY_CASH_ISSUER, (w[2].state.data).amount)
            assertEquals(34.85.DOLLARS `issued by` DUMMY_CASH_ISSUER, (w[1].state.data).amount)
        }
    }

    @Test
    fun `issue and spend total correctly and irrelevant ignored`() {
        val megaCorpServices = MockServices(MEGA_CORP_KEY)
        val freshKey = services.keyManagementService.freshKey()

        val usefulTX =
                database.transaction {
                    // A tx that sends us money.
                    val usefulBuilder = TransactionBuilder(null)
                    Cash().generateIssue(usefulBuilder, 100.DOLLARS `issued by` MEGA_CORP.ref(1), AnonymousParty(freshKey), DUMMY_NOTARY)
                    megaCorpServices.signInitialTransaction(usefulBuilder)
                }
        database.transaction {
            assertEquals(0.DOLLARS, services.getCashBalance(USD))
            services.recordTransactions(usefulTX)
        }
        val spendTX =
                database.transaction {
                    // A tx that spends our money.
                    val spendTXBuilder = TransactionBuilder(DUMMY_NOTARY)
                    generateSpend(services, spendTXBuilder, 80.DOLLARS, BOB, ourIdentity, emptySet())
                    val spendPTX = services.signInitialTransaction(spendTXBuilder, freshKey)
                    notaryServices.addSignature(spendPTX)
                }
        database.transaction {
            assertEquals(100.DOLLARS, services.getCashBalance(USD))
        }
        database.transaction {
            // A tx that doesn't send us anything.
            val irrelevantBuilder = TransactionBuilder(DUMMY_NOTARY)
            Cash().generateIssue(irrelevantBuilder, 100.DOLLARS `issued by` MEGA_CORP.ref(1), BOB, DUMMY_NOTARY)

            val irrelevantPTX = megaCorpServices.signInitialTransaction(irrelevantBuilder)
            val irrelevantTX = notaryServices.addSignature(irrelevantPTX)

            services.recordTransactions(irrelevantTX)
        }
        database.transaction {
            assertEquals(100.DOLLARS, services.getCashBalance(USD))
        }
        database.transaction {
            services.recordTransactions(spendTX)
        }
        database.transaction {
            assertEquals(20.DOLLARS, services.getCashBalance(USD))
        }
    }

    @Test
    fun `issue and attempt double spend`() {
        val freshKey = services.keyManagementService.freshKey()
        val criteriaLocked = VaultQueryCriteria(softLockingCondition = QueryCriteria.SoftLockingCondition(QueryCriteria.SoftLockingType.LOCKED_ONLY))

        database.transaction {
            // A tx that sends us money.
            services.fillWithSomeTestCash(100.DOLLARS, issuerServices, DUMMY_NOTARY, 10, 10, Random(0L), ownedBy = AnonymousParty(freshKey),
                    issuedBy = MEGA_CORP.ref(1))
            println("Cash balance: ${services.getCashBalance(USD)}")
        }
        database.transaction {
            assertThat(vaultService.queryBy<Cash.State>().states).hasSize(10)
            assertThat(vaultService.queryBy<Cash.State>(criteriaLocked).states).hasSize(0)
        }

        val backgroundExecutor = Executors.newFixedThreadPool(2)
        val countDown = CountDownLatch(2)

        // 1st tx that spends our money.
        backgroundExecutor.submit {
            database.transaction {
                try {
                    val txn1Builder = TransactionBuilder(DUMMY_NOTARY)
                    generateSpend(services, txn1Builder, 60.DOLLARS, BOB, ourIdentity, emptySet())
                    val ptxn1 = notaryServices.signInitialTransaction(txn1Builder)
                    val txn1 = services.addSignature(ptxn1, freshKey)
                    println("txn1: ${txn1.id} spent ${((txn1.tx.outputs[0].data) as Cash.State).amount}")
                    val unconsumedStates1 = vaultService.queryBy<Cash.State>()
                    val consumedStates1 = vaultService.queryBy<Cash.State>(VaultQueryCriteria(status = Vault.StateStatus.CONSUMED))
                    val lockedStates1 = vaultService.queryBy<Cash.State>(criteriaLocked).states
                    println("""txn1 states:
                                UNCONSUMED: ${unconsumedStates1.totalStatesAvailable} : $unconsumedStates1,
                                CONSUMED: ${consumedStates1.totalStatesAvailable} : $consumedStates1,
                                LOCKED: ${lockedStates1.count()} : $lockedStates1
                    """)
                    services.recordTransactions(txn1)
                    println("txn1: Cash balance: ${services.getCashBalance(USD)}")
                    val unconsumedStates2 = vaultService.queryBy<Cash.State>()
                    val consumedStates2 = vaultService.queryBy<Cash.State>(VaultQueryCriteria(status = Vault.StateStatus.CONSUMED))
                    val lockedStates2 = vaultService.queryBy<Cash.State>(criteriaLocked).states
                    println("""txn1 states:
                                UNCONSUMED: ${unconsumedStates2.totalStatesAvailable} : $unconsumedStates2,
                                CONSUMED: ${consumedStates2.totalStatesAvailable} : $consumedStates2,
                                LOCKED: ${lockedStates2.count()} : $lockedStates2
                    """)
                    txn1
                } catch (e: Exception) {
                    println(e)
                }
            }
            println("txn1 COMMITTED!")
            countDown.countDown()
        }

        // 2nd tx that attempts to spend same money
        backgroundExecutor.submit {
            database.transaction {
                try {
                    val txn2Builder = TransactionBuilder(DUMMY_NOTARY)
                    generateSpend(services, txn2Builder, 80.DOLLARS, BOB, ourIdentity, emptySet())
                    val ptxn2 = notaryServices.signInitialTransaction(txn2Builder)
                    val txn2 = services.addSignature(ptxn2, freshKey)
                    println("txn2: ${txn2.id} spent ${((txn2.tx.outputs[0].data) as Cash.State).amount}")
                    val unconsumedStates1 = vaultService.queryBy<Cash.State>()
                    val consumedStates1 = vaultService.queryBy<Cash.State>(VaultQueryCriteria(status = Vault.StateStatus.CONSUMED))
                    val lockedStates1 = vaultService.queryBy<Cash.State>(criteriaLocked).states
                    println("""txn2 states:
                                UNCONSUMED: ${unconsumedStates1.totalStatesAvailable} : $unconsumedStates1,
                                CONSUMED: ${consumedStates1.totalStatesAvailable} : $consumedStates1,
                                LOCKED: ${lockedStates1.count()} : $lockedStates1
                    """)
                    services.recordTransactions(txn2)
                    println("txn2: Cash balance: ${services.getCashBalance(USD)}")
                    val unconsumedStates2 = vaultService.queryBy<Cash.State>()
                    val consumedStates2 = vaultService.queryBy<Cash.State>()
                    val lockedStates2 = vaultService.queryBy<Cash.State>(criteriaLocked).states
                    println("""txn2 states:
                                UNCONSUMED: ${unconsumedStates2.totalStatesAvailable} : $unconsumedStates2,
                                CONSUMED: ${consumedStates2.totalStatesAvailable} : $consumedStates2,
                                LOCKED: ${lockedStates2.count()} : $lockedStates2
                    """)
                    txn2
                } catch (e: Exception) {
                    println(e)
                }
            }
            println("txn2 COMMITTED!")

            countDown.countDown()
        }

        countDown.await()
        database.transaction {
            println("Cash balance: ${services.getCashBalance(USD)}")
            assertThat(services.getCashBalance(USD)).isIn(DOLLARS(20), DOLLARS(40))
        }
    }

    @Test
    fun `branching LinearStates fails to verify`() {
        database.transaction {
            val freshKey = services.keyManagementService.freshKey()
            val freshIdentity = AnonymousParty(freshKey)
            val linearId = UniqueIdentifier()

            // Issue a linear state
            val dummyIssueBuilder = TransactionBuilder(notary = DUMMY_NOTARY).apply {
                addOutputState(DummyLinearContract.State(linearId = linearId, participants = listOf(freshIdentity)), DUMMY_LINEAR_CONTRACT_PROGRAM_ID)
                addOutputState(DummyLinearContract.State(linearId = linearId, participants = listOf(freshIdentity)), DUMMY_LINEAR_CONTRACT_PROGRAM_ID)
                addCommand(dummyCommand(notaryServices.myInfo.chooseIdentity().owningKey))
            }
            val dummyIssue = notaryServices.signInitialTransaction(dummyIssueBuilder)

            assertThatThrownBy {
                dummyIssue.toLedgerTransaction(services).verify()
            }
        }
    }

    @Test
    fun `sequencing LinearStates works`() {
        val freshKey = services.keyManagementService.freshKey()
        val freshIdentity = AnonymousParty(freshKey)
        val linearId = UniqueIdentifier()

        val dummyIssue =
                database.transaction {
                    // Issue a linear state
                    val dummyIssueBuilder = TransactionBuilder(notary = DUMMY_NOTARY)
                            .addOutputState(DummyLinearContract.State(linearId = linearId, participants = listOf(freshIdentity)), DUMMY_LINEAR_CONTRACT_PROGRAM_ID)
                            .addCommand(dummyCommand(notaryServices.myInfo.chooseIdentity().owningKey))
                    val dummyIssuePtx = notaryServices.signInitialTransaction(dummyIssueBuilder)
                    val dummyIssue = services.addSignature(dummyIssuePtx)

                    dummyIssue.toLedgerTransaction(services).verify()

                    services.recordTransactions(dummyIssue)
                    dummyIssue
                }
        database.transaction {
            assertThat(vaultService.queryBy<DummyLinearContract.State>().states).hasSize(1)

            // Move the same state
            val dummyMoveBuilder = TransactionBuilder(notary = DUMMY_NOTARY)
                    .addOutputState(DummyLinearContract.State(linearId = linearId, participants = listOf(freshIdentity)), DUMMY_LINEAR_CONTRACT_PROGRAM_ID)
                    .addInputState(dummyIssue.tx.outRef<LinearState>(0))
                    .addCommand(dummyCommand(notaryServices.myInfo.chooseIdentity().owningKey))

            val dummyMove = notaryServices.signInitialTransaction(dummyMoveBuilder)

            dummyIssue.toLedgerTransaction(services).verify()

            services.recordTransactions(dummyMove)
        }
        database.transaction {
            assertThat(vaultService.queryBy<DummyLinearContract.State>().states).hasSize(1)
        }
    }

    @Test
    fun `spending cash in vault of mixed state types works`() {

        val freshKey = services.keyManagementService.freshKey()
        database.transaction {
            services.fillWithSomeTestCash(100.DOLLARS, issuerServices, DUMMY_NOTARY, 3, 3, Random(0L), ownedBy = AnonymousParty(freshKey))
            services.fillWithSomeTestCash(100.SWISS_FRANCS, issuerServices, DUMMY_NOTARY, 2, 2, Random(0L))
            services.fillWithSomeTestCash(100.POUNDS, issuerServices, DUMMY_NOTARY, 1, 1, Random(0L))
        }
        database.transaction {
            val cash = vaultService.queryBy<Cash.State>().states
            cash.forEach { println(it.state.data.amount) }
        }
        database.transaction {
            services.fillWithSomeTestDeals(listOf("123", "456", "789"))
        }
        database.transaction {
            val deals = vaultService.queryBy<DummyDealContract.State>().states
            deals.forEach { println(it.state.data.linearId.externalId!!) }
        }

        database.transaction {
            // A tx that spends our money.
            val spendTXBuilder = TransactionBuilder(DUMMY_NOTARY)
            generateSpend(services, spendTXBuilder, 80.DOLLARS, BOB, ourIdentity, emptySet())
            val spendPTX = notaryServices.signInitialTransaction(spendTXBuilder)
            val spendTX = services.addSignature(spendPTX, freshKey)
            services.recordTransactions(spendTX)
        }
        database.transaction {
            val consumedStates = vaultService.queryBy<ContractState>(VaultQueryCriteria(status = Vault.StateStatus.CONSUMED)).states
            assertEquals(3, consumedStates.count())

            val unconsumedStates = vaultService.queryBy<ContractState>().states
            assertEquals(7, unconsumedStates.count())
        }
    }

    @Test
    fun `consuming multiple contract state types`() {

        val freshKey = services.keyManagementService.freshKey()
        val freshIdentity = AnonymousParty(freshKey)
        database.transaction {
            services.fillWithSomeTestDeals(listOf("123", "456", "789"))
        }
        val deals =
                database.transaction {
                    vaultService.queryBy<DummyDealContract.State>().states
                }
        database.transaction {
            services.fillWithSomeTestLinearStates(3)
        }
        database.transaction {
            val linearStates = vaultService.queryBy<DummyLinearContract.State>().states
            linearStates.forEach { println(it.state.data.linearId) }

            // Create a txn consuming different contract types
            val dummyMoveBuilder = TransactionBuilder(notary = DUMMY_NOTARY).apply {
                addOutputState(DummyLinearContract.State(participants = listOf(freshIdentity)), DUMMY_LINEAR_CONTRACT_PROGRAM_ID)
                addOutputState(DummyDealContract.State(ref = "999", participants = listOf(freshIdentity)), DUMMY_DEAL_PROGRAM_ID)
                addInputState(linearStates.first())
                addInputState(deals.first())
                addCommand(dummyCommand(notaryServices.myInfo.chooseIdentity().owningKey))
            }

            val dummyMove = notaryServices.signInitialTransaction(dummyMoveBuilder)
            dummyMove.toLedgerTransaction(services).verify()
            services.recordTransactions(dummyMove)
        }
        database.transaction {
            val consumedStates = vaultService.queryBy<ContractState>(VaultQueryCriteria(status = Vault.StateStatus.CONSUMED)).states
            assertEquals(2, consumedStates.count())

            val unconsumedStates = vaultService.queryBy<ContractState>().states
            assertEquals(6, unconsumedStates.count())
        }
    }
}
