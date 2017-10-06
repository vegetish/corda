package net.corda.node.services.transactions

import net.corda.core.contracts.StateRef
import net.corda.core.crypto.SecureHash
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.ThreadBox
import net.corda.core.node.services.UniquenessException
import net.corda.core.node.services.UniquenessProvider
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.parsePublicKeyBase58
import net.corda.core.utilities.toBase58String
import net.corda.node.utilities.AppendOnlyPersistentMap
import net.corda.node.utilities.NODE_DATABASE_PREFIX
import java.io.Serializable
import java.util.*
import javax.annotation.concurrent.ThreadSafe
import javax.persistence.*

/** A RDBMS backed Uniqueness provider */
@ThreadSafe
class PersistentUniquenessProvider : UniquenessProvider, SingletonSerializeAsToken() {

    @MappedSuperclass
    open class PersistentUniqueness(
            @EmbeddedId
            var id: PersistentStateRef = PersistentStateRef(),

            @Column(name = "consuming_transaction_id")
            var consumingTxHash: String = "",

            @Column(name = "consuming_input_index", length = 36)
            var consumingIndex: Int = 0,

            @Embedded
            var party: PersistentParty = PersistentParty()
    )

    @Embeddable
    data class PersistentParty(
            @Column(name = "requesting_party_name")
            var name: String = "",

            @Column(name = "requesting_party_key", length = 255)
            var owningKey: String = ""
    ) : Serializable

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}notary_commit_log")
    class PersistentNotaryCommit(id: PersistentStateRef, consumingTxHash: String, consumingIndex: Int, party: PersistentParty) :
            PersistentUniqueness(id, consumingTxHash, consumingIndex, party)


    private class InnerState {
        val committedStates = createMap()
    }

    private val mutex = ThreadBox(InnerState())

    companion object {
        private val log = loggerFor<PersistentUniquenessProvider>()

        fun createMap(): AppendOnlyPersistentMap<StateRef, UniquenessProvider.ConsumingTx, PersistentNotaryCommit, PersistentStateRef> =
                AppendOnlyPersistentMap(
                        toPersistentEntityKey = { PersistentStateRef(it.txhash.toString(), it.index) },
                        fromPersistentEntity = {
                            //TODO null check will become obsolete after making DB/JPA columns not nullable
                            var txId = it.id.txId ?: throw IllegalStateException("DB returned null SecureHash transactionId")
                            var index = it.id.index ?: throw IllegalStateException("DB returned null SecureHash index")
                            Pair(StateRef(txhash = SecureHash.parse(txId), index = index),
                                    UniquenessProvider.ConsumingTx(
                                            id = SecureHash.parse(it.consumingTxHash),
                                            inputIndex = it.consumingIndex,
                                            requestingParty = Party(
                                                    name = CordaX500Name.parse(it.party.name),
                                                    owningKey = parsePublicKeyBase58(it.party.owningKey))))
                        },
                        toPersistentEntity = { (txHash, index): StateRef, (id, inputIndex, requestingParty): UniquenessProvider.ConsumingTx ->
                            PersistentNotaryCommit(
                                    id = PersistentStateRef(txHash.toString(), index),
                                    consumingTxHash = id.toString(),
                                    consumingIndex = inputIndex,
                                    party = PersistentParty(requestingParty.name.toString(), requestingParty.owningKey.toBase58String())
                            )
                        },
                        persistentEntityClass = PersistentNotaryCommit::class.java
                )
    }

    override fun commit(states: List<StateRef>, txId: SecureHash, callerIdentity: Party) {

        val conflict = mutex.locked {
            val conflictingStates = LinkedHashMap<StateRef, UniquenessProvider.ConsumingTx>()
            for (inputState in states) {
                val consumingTx = committedStates.get(inputState)
                if (consumingTx != null) conflictingStates[inputState] = consumingTx
            }
            if (conflictingStates.isNotEmpty()) {
                log.debug("Failure, input states already committed: ${conflictingStates.keys}")
                UniquenessProvider.Conflict(conflictingStates)
            } else {
                states.forEachIndexed { i, stateRef ->
                    committedStates[stateRef] = UniquenessProvider.ConsumingTx(txId, i, callerIdentity)
                }
                log.debug("Successfully committed all input states: $states")
                null
            }
        }

        if (conflict != null) throw UniquenessException(conflict)
    }
}
