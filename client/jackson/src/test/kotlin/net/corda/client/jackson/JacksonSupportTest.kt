package net.corda.client.jackson

import com.fasterxml.jackson.databind.SerializationFeature
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.contracts.Amount
import net.corda.core.cordapp.CordappProvider
import net.corda.core.crypto.*
import net.corda.core.node.ServiceHub
import net.corda.core.transactions.SignedTransaction
import net.corda.finance.USD
import net.corda.testing.ALICE_PUBKEY
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.MINI_CORP
import net.corda.testing.TestDependencyInjectionBase
import net.corda.testing.contracts.DummyContract
import org.junit.Before
import org.junit.Test
import java.math.BigInteger
import java.security.PublicKey
import java.util.*
import kotlin.test.assertEquals

class JacksonSupportTest : TestDependencyInjectionBase() {
    companion object {
        private val SEED = BigInteger.valueOf(20170922L)
        val mapper = JacksonSupport.createNonRpcMapper()
    }

    private lateinit var services: ServiceHub
    private lateinit var cordappProvider: CordappProvider

    @Before
    fun setup() {
        services = mock()
        cordappProvider = mock()
        whenever(services.cordappProvider).thenReturn(cordappProvider)
    }

    @Test
    fun `should serialize Composite keys`() {
        val expected = "\"MIHAMBUGE2mtoq+J1bjir/ONk6yd5pab0FoDgaYAMIGiAgECMIGcMDIDLQAwKjAFBgMrZXADIQAgIX1QlJRgaLlD0ttLlJF5kNqT/7P7QwCvrWc9+/248gIBATAyAy0AMCowBQYDK2VwAyEAqS0JPGlzdviBZjB9FaNY+w6cVs3/CQ2A5EimE9Lyng4CAQEwMgMtADAqMAUGAytlcAMhALq4GG0gBQZIlaKE6ucooZsuoKUbH4MtGSmA6cwj136+AgEB\""
        val innerKeys = (1..3).map { i ->
            Crypto.deriveKeyPairFromEntropy(Crypto.EDDSA_ED25519_SHA512, SEED.plus(BigInteger.valueOf(i.toLong()))).public
        }
        // Build a 2 of 3 composite key
        val publicKey = CompositeKey.Builder().let {
            innerKeys.forEach { key -> it.addKey(key, 1) }
            it.build(2)
        }
        val serialized = mapper.writeValueAsString(publicKey)
        assertEquals(expected, serialized)
        val parsedKey = mapper.readValue(serialized, PublicKey::class.java)
        assertEquals(publicKey, parsedKey)
    }

    private class Dummy(val notional: Amount<Currency>)

    @Test
    fun `should serialize EdDSA keys`() {
        val expected = "\"MCowBQYDK2VwAyEACFTgLk1NOqYXAfxLoR7ctSbZcl9KMXu58Mq31Kv1Dwk=\""
        val publicKey = Crypto.deriveKeyPairFromEntropy(Crypto.EDDSA_ED25519_SHA512, SEED).public
        val serialized = mapper.writeValueAsString(publicKey)
        assertEquals(expected, serialized)
        val parsedKey = mapper.readValue(serialized, PublicKey::class.java)
        assertEquals(publicKey, parsedKey)
    }

    @Test
    fun readAmount() {
        val oldJson = """
            {
              "notional": {
                  "quantity": 2500000000,
                  "token": "USD"
              }
            }
        """
        val newJson = """ { "notional" : "$25000000" } """

        assertEquals(Amount(2500000000L, USD), mapper.readValue(newJson, Dummy::class.java).notional)
        assertEquals(Amount(2500000000L, USD), mapper.readValue(oldJson, Dummy::class.java).notional)
    }

    @Test
    fun writeAmount() {
        val writer = mapper.writer().without(SerializationFeature.INDENT_OUTPUT)
        assertEquals("""{"notional":"25000000.00 USD"}""", writer.writeValueAsString(Dummy(Amount.parseCurrency("$25000000"))))
    }

    @Test
    fun writeTransaction() {
        val attachmentRef = SecureHash.randomSHA256()
        whenever(cordappProvider.getContractAttachmentID(DummyContract.PROGRAM_ID))
                .thenReturn(attachmentRef)
        fun makeDummyTx(): SignedTransaction {
            val wtx = DummyContract.generateInitial(1, DUMMY_NOTARY, MINI_CORP.ref(1))
                    .toWireTransaction(services)
            val signatures = TransactionSignature(
                    ByteArray(1),
                    ALICE_PUBKEY,
                    SignatureMetadata(
                            1,
                            Crypto.findSignatureScheme(ALICE_PUBKEY).schemeNumberID
                    )
            )
            return SignedTransaction(wtx, listOf(signatures))
        }

        val writer = mapper.writer()
        // We don't particularly care about the serialized format, just need to make sure it completes successfully.
        writer.writeValueAsString(makeDummyTx())
    }
}
