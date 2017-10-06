@file:JvmName("TestConstants")

package net.corda.testing

import net.corda.core.contracts.Command
import net.corda.core.contracts.TypeOnlyCommandData
import net.corda.core.crypto.entropyToKeyPair
import net.corda.core.crypto.generateKeyPair
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.toX509CertHolder
import net.corda.node.utilities.CertificateAndKeyPair
import net.corda.node.utilities.X509Utilities
import net.corda.node.utilities.getCertificateAndKeyPair
import net.corda.node.utilities.loadKeyStore
import org.bouncycastle.cert.X509CertificateHolder
import java.math.BigInteger
import java.security.KeyPair
import java.security.PublicKey
import java.security.cert.Certificate
import java.time.Instant

// A dummy time at which we will be pretending test transactions are created.
val TEST_TX_TIME: Instant get() = Instant.parse("2015-04-17T12:00:00.00Z")

val DUMMY_KEY_1: KeyPair by lazy { generateKeyPair() }
val DUMMY_KEY_2: KeyPair by lazy { generateKeyPair() }

val DUMMY_NOTARY_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(20)) }
/** Dummy notary identity for tests and simulations */
val DUMMY_NOTARY_IDENTITY: PartyAndCertificate get() = getTestPartyAndCertificate(DUMMY_NOTARY)
val DUMMY_NOTARY: Party get() = Party(CordaX500Name(organisation = "Notary Service", locality = "Zurich", country = "CH"), DUMMY_NOTARY_KEY.public)

val DUMMY_MAP_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(30)) }
/** Dummy network map service identity for tests and simulations */
val DUMMY_MAP: Party get() = Party(CordaX500Name(organisation = "Network Map Service", locality = "Amsterdam", country = "NL"), DUMMY_MAP_KEY.public)

val DUMMY_BANK_A_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(40)) }
/** Dummy bank identity for tests and simulations */
val DUMMY_BANK_A: Party get() = Party(CordaX500Name(organisation = "Bank A", locality = "London", country = "GB"), DUMMY_BANK_A_KEY.public)

val DUMMY_BANK_B_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(50)) }
/** Dummy bank identity for tests and simulations */
val DUMMY_BANK_B: Party get() = Party(CordaX500Name(organisation = "Bank B", locality = "New York", country = "US"), DUMMY_BANK_B_KEY.public)

val DUMMY_BANK_C_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(60)) }
/** Dummy bank identity for tests and simulations */
val DUMMY_BANK_C: Party get() = Party(CordaX500Name(organisation = "Bank C", locality = "Tokyo", country = "JP"), DUMMY_BANK_C_KEY.public)

val ALICE_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(70)) }
/** Dummy individual identity for tests and simulations */
val ALICE_IDENTITY: PartyAndCertificate get() = getTestPartyAndCertificate(ALICE)
val ALICE_NAME = CordaX500Name(organisation = "Alice Corp", locality = "Madrid", country = "ES")
val ALICE: Party get() = Party(ALICE_NAME, ALICE_KEY.public)

val BOB_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(80)) }
/** Dummy individual identity for tests and simulations */
val BOB_IDENTITY: PartyAndCertificate get() = getTestPartyAndCertificate(BOB)
val BOB_NAME = CordaX500Name(organisation = "Bob Plc", locality = "Rome", country = "IT")
val BOB: Party get() = Party(BOB_NAME, BOB_KEY.public)

val CHARLIE_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(90)) }
/** Dummy individual identity for tests and simulations */
val CHARLIE_IDENTITY: PartyAndCertificate get() = getTestPartyAndCertificate(CHARLIE)
val CHARLIE_NAME = CordaX500Name(organisation = "Charlie Ltd", locality = "Athens", country = "GR")
val CHARLIE: Party get() = Party(CHARLIE_NAME, CHARLIE_KEY.public)

val DUMMY_REGULATOR_KEY: KeyPair by lazy { entropyToKeyPair(BigInteger.valueOf(100)) }
/** Dummy regulator for tests and simulations */
val DUMMY_REGULATOR: Party get() = Party(CordaX500Name(organisation = "Regulator A", locality = "Paris", country = "FR"), DUMMY_REGULATOR_KEY.public)

val DEV_CA: CertificateAndKeyPair by lazy {
    // TODO: Should be identity scheme
    val caKeyStore = loadKeyStore(ClassLoader.getSystemResourceAsStream("net/corda/node/internal/certificates/cordadevcakeys.jks"), "cordacadevpass")
    caKeyStore.getCertificateAndKeyPair(X509Utilities.CORDA_INTERMEDIATE_CA, "cordacadevkeypass")
}
val DEV_TRUST_ROOT: X509CertificateHolder by lazy {
    // TODO: Should be identity scheme
    val caKeyStore = loadKeyStore(ClassLoader.getSystemResourceAsStream("net/corda/node/internal/certificates/cordadevcakeys.jks"), "cordacadevpass")
    caKeyStore.getCertificateChain(X509Utilities.CORDA_INTERMEDIATE_CA).last().toX509CertHolder()
}

fun dummyCommand(vararg signers: PublicKey = arrayOf(generateKeyPair().public)) = Command<TypeOnlyCommandData>(DummyCommandData, signers.toList())

object DummyCommandData : TypeOnlyCommandData()

val DUMMY_IDENTITY_1: PartyAndCertificate get() = getTestPartyAndCertificate(DUMMY_PARTY)
val DUMMY_PARTY: Party get() = Party(CordaX500Name(organisation = "Dummy", locality = "Madrid", country = "ES"), DUMMY_KEY_1.public)
