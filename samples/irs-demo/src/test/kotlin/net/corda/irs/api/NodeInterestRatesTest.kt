package net.corda.irs.api

import net.corda.core.contracts.Command
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.TransactionState
import net.corda.core.crypto.generateKeyPair
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.getOrThrow
import net.corda.finance.DOLLARS
import net.corda.finance.contracts.Fix
import net.corda.finance.contracts.FixOf
import net.corda.finance.contracts.asset.CASH
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.contracts.asset.`issued by`
import net.corda.finance.contracts.asset.`owned by`
import net.corda.irs.flows.RatesFixFlow
import net.corda.node.utilities.CordaPersistence
import net.corda.node.utilities.configureDatabase
import net.corda.testing.*
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockServices
import net.corda.testing.node.MockServices.Companion.makeTestDataSourceProperties
import net.corda.testing.node.MockServices.Companion.makeTestDatabaseProperties
import net.corda.testing.node.MockServices.Companion.makeTestIdentityService
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.math.BigDecimal
import java.util.function.Predicate
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class NodeInterestRatesTest : TestDependencyInjectionBase() {
    private val TEST_DATA = NodeInterestRates.parseFile("""
        LIBOR 2016-03-16 1M = 0.678
        LIBOR 2016-03-16 2M = 0.685
        LIBOR 2016-03-16 1Y = 0.890
        LIBOR 2016-03-16 2Y = 0.962
        EURIBOR 2016-03-15 1M = 0.123
        EURIBOR 2016-03-15 2M = 0.111
        """.trimIndent())

    private val DUMMY_CASH_ISSUER_KEY = generateKeyPair()
    private val DUMMY_CASH_ISSUER = Party(CordaX500Name(organisation = "Cash issuer", locality = "London", country = "GB"), DUMMY_CASH_ISSUER_KEY.public)
    private val services = MockServices(listOf("net.corda.finance.contracts.asset"), DUMMY_CASH_ISSUER_KEY, MEGA_CORP_KEY)

    private lateinit var oracle: NodeInterestRates.Oracle
    private lateinit var database: CordaPersistence

    private fun fixCmdFilter(elem: Any): Boolean {
        return when (elem) {
            is Command<*> -> services.myInfo.chooseIdentity().owningKey in elem.signers && elem.value is Fix
            else -> false
        }
    }

    private fun filterCmds(elem: Any): Boolean = elem is Command<*>

    @Before
    fun setUp() {
        setCordappPackages("net.corda.finance.contracts")
        database = configureDatabase(makeTestDataSourceProperties(), makeTestDatabaseProperties(), createIdentityService = ::makeTestIdentityService)
        database.transaction {
            oracle = NodeInterestRates.Oracle(services).apply { knownFixes = TEST_DATA }
        }
    }

    @After
    fun tearDown() {
        database.close()
        unsetCordappPackages()
    }

    @Test
    fun `query successfully`() {
        database.transaction {
            val q = NodeInterestRates.parseFixOf("LIBOR 2016-03-16 1M")
            val res = oracle.query(listOf(q))
            assertEquals(1, res.size)
            assertEquals(BigDecimal("0.678"), res[0].value)
            assertEquals(q, res[0].of)
        }
    }

    @Test
    fun `query with one success and one missing`() {
        database.transaction {
            val q1 = NodeInterestRates.parseFixOf("LIBOR 2016-03-16 1M")
            val q2 = NodeInterestRates.parseFixOf("LIBOR 2016-03-15 1M")
            val e = assertFailsWith<NodeInterestRates.UnknownFix> { oracle.query(listOf(q1, q2)) }
            assertEquals(e.fix, q2)
        }
    }

    @Test
    fun `query successfully with interpolated rate`() {
        database.transaction {
            val q = NodeInterestRates.parseFixOf("LIBOR 2016-03-16 5M")
            val res = oracle.query(listOf(q))
            assertEquals(1, res.size)
            assertEquals(0.7316228, res[0].value.toDouble(), 0.0000001)
            assertEquals(q, res[0].of)
        }
    }

    @Test
    fun `rate missing and unable to interpolate`() {
        database.transaction {
            val q = NodeInterestRates.parseFixOf("EURIBOR 2016-03-15 3M")
            assertFailsWith<NodeInterestRates.UnknownFix> { oracle.query(listOf(q)) }
        }
    }

    @Test
    fun `empty query`() {
        database.transaction {
            assertFailsWith<IllegalArgumentException> { oracle.query(emptyList()) }
        }
    }

    @Test
    fun `refuse to sign with no relevant commands`() {
        database.transaction {
            val tx = makeFullTx()
            val wtx1 = tx.toWireTransaction(services)
            fun filterAllOutputs(elem: Any): Boolean {
                return when (elem) {
                    is TransactionState<ContractState> -> true
                    else -> false
                }
            }

            val ftx1 = wtx1.buildFilteredTransaction(Predicate(::filterAllOutputs))
            assertFailsWith<IllegalArgumentException> { oracle.sign(ftx1) }
            tx.addCommand(Cash.Commands.Move(), ALICE_PUBKEY)
            val wtx2 = tx.toWireTransaction(services)
            val ftx2 = wtx2.buildFilteredTransaction(Predicate { x -> filterCmds(x) })
            assertFalse(wtx1.id == wtx2.id)
            assertFailsWith<IllegalArgumentException> { oracle.sign(ftx2) }
        }
    }

    @Test
    fun `sign successfully`() {
        database.transaction {
            val tx = makePartialTX()
            val fix = oracle.query(listOf(NodeInterestRates.parseFixOf("LIBOR 2016-03-16 1M"))).first()
            tx.addCommand(fix, services.myInfo.chooseIdentity().owningKey)
            // Sign successfully.
            val wtx = tx.toWireTransaction(services)
            val ftx = wtx.buildFilteredTransaction(Predicate { fixCmdFilter(it) })
            val signature = oracle.sign(ftx)
            wtx.checkSignature(signature)
        }
    }

    @Test
    fun `do not sign with unknown fix`() {
        database.transaction {
            val tx = makePartialTX()
            val fixOf = NodeInterestRates.parseFixOf("LIBOR 2016-03-16 1M")
            val badFix = Fix(fixOf, BigDecimal("0.6789"))
            tx.addCommand(badFix, services.myInfo.chooseIdentity().owningKey)
            val wtx = tx.toWireTransaction(services)
            val ftx = wtx.buildFilteredTransaction(Predicate { fixCmdFilter(it) })
            val e1 = assertFailsWith<NodeInterestRates.UnknownFix> { oracle.sign(ftx) }
            assertEquals(fixOf, e1.fix)
        }
    }

    @Test
    fun `do not sign too many leaves`() {
        database.transaction {
            val tx = makePartialTX()
            val fix = oracle.query(listOf(NodeInterestRates.parseFixOf("LIBOR 2016-03-16 1M"))).first()
            fun filtering(elem: Any): Boolean {
                return when (elem) {
                    is Command<*> -> services.myInfo.chooseIdentity().owningKey in elem.signers && elem.value is Fix
                    is TransactionState<ContractState> -> true
                    else -> false
                }
            }
            tx.addCommand(fix, services.myInfo.chooseIdentity().owningKey)
            val wtx = tx.toWireTransaction(services)
            val ftx = wtx.buildFilteredTransaction(Predicate(::filtering))
            assertFailsWith<IllegalArgumentException> { oracle.sign(ftx) }
        }
    }

    @Test
    fun `empty partial transaction to sign`() {
        val tx = makeFullTx()
        val wtx = tx.toWireTransaction(services)
        val ftx = wtx.buildFilteredTransaction(Predicate { false })
        assertFailsWith<IllegalArgumentException> { oracle.sign(ftx) } // It throws failed requirement (as it is empty there is no command to check and sign).
    }

    @Test
    fun `network tearoff`() {
        val mockNet = MockNetwork(initialiseSerialization = false)
        val n1 = mockNet.createNotaryNode()
        val oracleNode = mockNet.createNode().apply {
            internals.registerInitiatedFlow(NodeInterestRates.FixQueryHandler::class.java)
            internals.registerInitiatedFlow(NodeInterestRates.FixSignHandler::class.java)
            database.transaction {
                internals.installCordaService(NodeInterestRates.Oracle::class.java).knownFixes = TEST_DATA
            }
        }
        val tx = makePartialTX()
        val fixOf = NodeInterestRates.parseFixOf("LIBOR 2016-03-16 1M")
        val flow = FilteredRatesFlow(tx, oracleNode.info.chooseIdentity(), fixOf, BigDecimal("0.675"), BigDecimal("0.1"))
        LogHelper.setLevel("rates")
        mockNet.runNetwork()
        val future = n1.services.startFlow(flow).resultFuture
        mockNet.runNetwork()
        future.getOrThrow()
        // We should now have a valid fix of our tx from the oracle.
        val fix = tx.toWireTransaction(services).commands.map { it.value as Fix }.first()
        assertEquals(fixOf, fix.of)
        assertEquals(BigDecimal("0.678"), fix.value)
        mockNet.stopNodes()
    }

    class FilteredRatesFlow(tx: TransactionBuilder,
                            oracle: Party,
                            fixOf: FixOf,
                            expectedRate: BigDecimal,
                            rateTolerance: BigDecimal,
                            progressTracker: ProgressTracker = RatesFixFlow.tracker(fixOf.name))
        : RatesFixFlow(tx, oracle, fixOf, expectedRate, rateTolerance, progressTracker) {
        override fun filtering(elem: Any): Boolean {
            return when (elem) {
                is Command<*> -> oracle.owningKey in elem.signers && elem.value is Fix
                else -> false
            }
        }
    }

    private fun makePartialTX() = TransactionBuilder(DUMMY_NOTARY).withItems(
            TransactionState(1000.DOLLARS.CASH `issued by` DUMMY_CASH_ISSUER `owned by` ALICE, Cash.PROGRAM_ID, DUMMY_NOTARY))

    private fun makeFullTx() = makePartialTX().withItems(dummyCommand())
}
