package net.corda.core.transactions

import net.corda.core.CordaException
import net.corda.core.contracts.*
import net.corda.core.crypto.*
import net.corda.core.identity.Party
import net.corda.core.serialization.*
import net.corda.core.utilities.OpaqueBytes
import java.security.PublicKey
import java.util.function.Predicate

/**
 * Implemented by [WireTransaction] and [FilteredTransaction]. A TraversableTransaction allows you to iterate
 * over the flattened components of the underlying transaction structure, taking into account that some
 * may be missing in the case of this representing a "torn" transaction. Please see the user guide section
 * "Transaction tear-offs" to learn more about this feature.
 */
abstract class TraversableTransaction(open val componentGroups: List<ComponentGroup>) : CoreTransaction() {
    /** Hashes of the ZIP/JAR files that are needed to interpret the contents of this wire transaction. */
    val attachments: List<SecureHash> = deserialiseComponentGroup(ComponentGroupEnum.ATTACHMENTS_GROUP, { SerializedBytes<SecureHash>(it).deserialize() })

    /** Pointers to the input states on the ledger, identified by (tx identity hash, output index). */
    override val inputs: List<StateRef> = deserialiseComponentGroup(ComponentGroupEnum.INPUTS_GROUP, { SerializedBytes<StateRef>(it).deserialize() })

    override val outputs: List<TransactionState<ContractState>> = deserialiseComponentGroup(ComponentGroupEnum.OUTPUTS_GROUP, { SerializedBytes<TransactionState<ContractState>>(it).deserialize(context = SerializationFactory.defaultFactory.defaultContext.withAttachmentsClassLoader(attachments)) })

    /** Ordered list of ([CommandData], [PublicKey]) pairs that instruct the contracts what to do. */
    val commands: List<Command<*>> = deserialiseComponentGroup(ComponentGroupEnum.COMMANDS_GROUP, { SerializedBytes<Command<*>>(it).deserialize(context = SerializationFactory.defaultFactory.defaultContext.withAttachmentsClassLoader(attachments)) })

    override val notary: Party? = let {
        val notaries: List<Party> = deserialiseComponentGroup(ComponentGroupEnum.NOTARY_GROUP, { SerializedBytes<Party>(it).deserialize() })
        check(notaries.size <= 1) { "Invalid Transaction. More than 1 notary party detected." }
        if (notaries.isNotEmpty()) notaries[0] else null
    }

    val timeWindow: TimeWindow? = let {
        val timeWindows: List<TimeWindow> = deserialiseComponentGroup(ComponentGroupEnum.TIMEWINDOW_GROUP, { SerializedBytes<TimeWindow>(it).deserialize() })
        check(timeWindows.size <= 1) { "Invalid Transaction. More than 1 time-window detected." }
        if (timeWindows.isNotEmpty()) timeWindows[0] else null
    }

    /**
     * Returns a list of all the component groups that are present in the transaction, excluding the privacySalt,
     * in the following order (which is the same with the order in [ComponentGroupEnum]:
     * - list of each input that is present
     * - list of each output that is present
     * - list of each command that is present
     * - list of each attachment that is present
     * - The notary [Party], if present (list with one element)
     * - The time-window of the transaction, if present (list with one element)
    */
    val availableComponentGroups: List<List<Any>>
        get() {
            val result = mutableListOf(inputs, outputs, commands, attachments)
            notary?.let { result += listOf(it) }
            timeWindow?.let { result += listOf(it) }
            return result
        }

    // Helper function to return a meaningful exception if deserialisation of a component fails.
    private fun <T> deserialiseComponentGroup(groupEnum: ComponentGroupEnum, deserialiseBody: (ByteArray) -> T): List<T> {
        val group = componentGroups.firstOrNull { it.groupIndex == groupEnum.ordinal }
        return if (group != null && group.components.isNotEmpty()) {
            group.components.mapIndexed { internalIndex, component ->
                try {
                    deserialiseBody(component.bytes)
                } catch (e: MissingAttachmentsException) {
                    throw e
                } catch (e: Exception) {
                    throw Exception("Malformed transaction, $groupEnum at index $internalIndex cannot be deserialised", e)
                }
            }
        } else {
            emptyList()
        }
    }
}

/**
 * Class representing merkleized filtered transaction.
 * @param id Merkle tree root hash.
 * @param filteredComponentGroups list of transaction components groups remained after filters are applied to [WireTransaction].
 * @param groupHashes the roots of the transaction component groups.
 */
@CordaSerializable
class FilteredTransaction private constructor(
        override val id: SecureHash,
        val filteredComponentGroups: List<FilteredComponentGroup>,
        val groupHashes: List<SecureHash>
) : TraversableTransaction(filteredComponentGroups) {

    companion object {
        /**
         * Construction of filtered transaction with partial Merkle tree.
         * @param wtx WireTransaction to be filtered.
         * @param filtering filtering over the whole WireTransaction.
         */
        @JvmStatic
        fun buildFilteredTransaction(wtx: WireTransaction, filtering: Predicate<Any>): FilteredTransaction {
            val filteredComponentGroups = filterWithFun(wtx, filtering)
            return FilteredTransaction(wtx.id, filteredComponentGroups, wtx.groupHashes)
        }

        /**
         * Construction of partial transaction from [WireTransaction] based on filtering.
         * Note that list of nonces to be sent is updated on the fly, based on the index of the filtered tx component.
         * @param filtering filtering over the whole WireTransaction.
         * @return a list of [FilteredComponentGroup] used in PartialMerkleTree calculation and verification.
         */
        private fun filterWithFun(wtx: WireTransaction, filtering: Predicate<Any>): List<FilteredComponentGroup> {
            val filteredSerialisedComponents: MutableMap<Int, MutableList<OpaqueBytes>> = hashMapOf()
            val filteredComponentNonces: MutableMap<Int, MutableList<SecureHash>> = hashMapOf()
            val filteredComponentHashes: MutableMap<Int, MutableList<SecureHash>> = hashMapOf() // Required for partial Merkle tree generation.
            var signersIncluded = false

            fun <T : Any> filter(t: T, componentGroupIndex: Int, internalIndex: Int) {
                if (filtering.test(t)) {
                    val group = filteredSerialisedComponents[componentGroupIndex]
                    // Because the filter passed, we know there is a match. We also use first vs single as the init function
                    // of WireTransaction ensures there are no duplicated groups.
                    val serialisedComponent = wtx.componentGroups.first { it.groupIndex == componentGroupIndex }.components[internalIndex]
                    if (group == null) {
                        // As all of the helper Map structures, like availableComponentNonces, availableComponentHashes
                        // and groupsMerkleRoots, are computed lazily via componentGroups.forEach, there should always be
                        // a match on Map.get ensuring it will never return null.
                        filteredSerialisedComponents.put(componentGroupIndex, mutableListOf(serialisedComponent))
                        filteredComponentNonces.put(componentGroupIndex, mutableListOf(wtx.availableComponentNonces[componentGroupIndex]!![internalIndex]))
                        filteredComponentHashes.put(componentGroupIndex, mutableListOf(wtx.availableComponentHashes[componentGroupIndex]!![internalIndex]))
                    } else {
                        group.add(serialisedComponent)
                        // If the group[componentGroupIndex] existed, then we guarantee that
                        // filteredComponentNonces[componentGroupIndex] and filteredComponentHashes[componentGroupIndex] are not null.
                        filteredComponentNonces[componentGroupIndex]!!.add(wtx.availableComponentNonces[componentGroupIndex]!![internalIndex])
                        filteredComponentHashes[componentGroupIndex]!!.add(wtx.availableComponentHashes[componentGroupIndex]!![internalIndex])
                    }
                    // If at least one command is visible, then all command-signers should be visible as well.
                    // This is required for visibility purposes, see FilteredTransaction.checkAllCommandsVisible() for more details.
                    if (componentGroupIndex == ComponentGroupEnum.COMMANDS_GROUP.ordinal && !signersIncluded) {
                        signersIncluded = true
                        val signersGroupComponents = wtx.componentGroups.firstOrNull() { it.groupIndex == ComponentGroupEnum.SIGNERS_GROUP.ordinal }
                        // Check if there is indeed a SIGNERS_GROUP. It is possible that this transaction was sent from an old client,
                        // in which case there is no signers group to send.
                        if (signersGroupComponents != null) {
                            val groupIndex = signersGroupComponents.groupIndex
                            filteredSerialisedComponents.put(groupIndex, signersGroupComponents.components.toMutableList())
                            filteredComponentNonces.put(groupIndex, wtx.availableComponentNonces[groupIndex]!!.toMutableList())
                            filteredComponentHashes.put(groupIndex, wtx.availableComponentHashes[groupIndex]!!.toMutableList())
                        }
                    }
                }
            }

            fun updateFilteredComponents() {
                wtx.inputs.forEachIndexed { internalIndex, it -> filter(it, ComponentGroupEnum.INPUTS_GROUP.ordinal, internalIndex) }
                wtx.outputs.forEachIndexed { internalIndex, it -> filter(it, ComponentGroupEnum.OUTPUTS_GROUP.ordinal, internalIndex) }
                wtx.commands.forEachIndexed { internalIndex, it -> filter(it, ComponentGroupEnum.COMMANDS_GROUP.ordinal, internalIndex)  }
                wtx.attachments.forEachIndexed { internalIndex, it -> filter(it, ComponentGroupEnum.ATTACHMENTS_GROUP.ordinal, internalIndex) }
                if (wtx.notary != null) filter(wtx.notary, ComponentGroupEnum.NOTARY_GROUP.ordinal, 0)
                if (wtx.timeWindow != null) filter(wtx.timeWindow, ComponentGroupEnum.TIMEWINDOW_GROUP.ordinal, 0)

                // It's sometimes possible that when we receive a WireTransaction for which there is a new or more unknown component groups,
                // we decide to filter and attach this field to a FilteredTransaction.
                // An example would be to redact certain contract state types, but otherwise leave a transaction alone,
                // including the unknown new components.
                wtx.componentGroups.filter { it.groupIndex >= ComponentGroupEnum.values().size }.forEach { componentGroup -> componentGroup.components.forEachIndexed { internalIndex, component-> filter(component, componentGroup.groupIndex, internalIndex) }}
            }

            fun createPartialMerkleTree(componentGroupIndex: Int) = PartialMerkleTree.build(MerkleTree.getMerkleTree(wtx.availableComponentHashes[componentGroupIndex]!!), filteredComponentHashes[componentGroupIndex]!!)

            fun createFilteredComponentGroups(): List<FilteredComponentGroup> {
                updateFilteredComponents()
                val filteredComponentGroups: MutableList<FilteredComponentGroup> = mutableListOf()
                filteredSerialisedComponents.forEach { (groupIndex, value) ->
                    filteredComponentGroups.add(FilteredComponentGroup(groupIndex, value, filteredComponentNonces[groupIndex]!!, createPartialMerkleTree(groupIndex) ))
                }
                return filteredComponentGroups
            }

            return createFilteredComponentGroups()
        }
    }

    /**
     * Runs verification of partial Merkle branch against [id].
     * Note that empty filtered transactions (with no component groups) are accepted as well,
     * e.g. for Timestamp Authorities to blindly sign or any other similar case in the future
     * that requires a blind signature over a transaction's [id].
     * @throws FilteredTransactionVerificationException if verification fails.
     */
    @Throws(FilteredTransactionVerificationException::class)
    fun verify() {
        verificationCheck(groupHashes.isNotEmpty()) { "At least one component group hash is required" }
        // Verify the top level Merkle tree (group hashes are its leaves, including allOnesHash for empty list or null components in WireTransaction).
        verificationCheck(MerkleTree.getMerkleTree(groupHashes).hash == id) { "Top level Merkle tree cannot be verified against transaction's id" }

        // For completely blind verification (no components are included).
        if (filteredComponentGroups.isEmpty()) return

        // Compute partial Merkle roots for each filtered component and verify each of the partial Merkle trees.
        filteredComponentGroups.forEach { (groupIndex, components, nonces, groupPartialTree) ->
            verificationCheck(groupIndex < groupHashes.size ) { "There is no matching component group hash for group $groupIndex" }
            val groupMerkleRoot = groupHashes[groupIndex]
            verificationCheck(groupMerkleRoot == PartialMerkleTree.rootAndUsedHashes(groupPartialTree.root, mutableListOf())) { "Partial Merkle tree root and advertised full Merkle tree root for component group $groupIndex do not match" }
            verificationCheck(groupPartialTree.verify(groupMerkleRoot, components.mapIndexed { index, component -> componentHash(nonces[index], component) })) { "Visible components in group $groupIndex cannot be verified against their partial Merkle tree" }
        }
    }

    /**
     * Function that checks the whole filtered structure.
     * Force type checking on a structure that we obtained, so we don't sign more than expected.
     * Example: Oracle is implemented to check only for commands, if it gets an attachment and doesn't expect it - it can sign
     * over a transaction with the attachment that wasn't verified. Of course it depends on how you implement it, but else -> false
     * should solve a problem with possible later extensions to WireTransaction.
     * @param checkingFun function that performs type checking on the structure fields and provides verification logic accordingly.
     * @return false if no elements were matched on a structure or checkingFun returned false.
     */
    fun checkWithFun(checkingFun: (Any) -> Boolean): Boolean {
        val checkList = availableComponentGroups.flatten().map { checkingFun(it) }
        return (!checkList.isEmpty()) && checkList.all { it }
    }

    /**
     * Function that checks if all of the components in a particular group are visible.
     * This functionality is required on non-Validating Notaries to check that all inputs are visible.
     * It might also be applied in Oracles, where an Oracle should know it can see all commands.
     * The logic behind this algorithm is that we check that the root of the provided group partialMerkleTree matches with the
     * root of a fullMerkleTree if computed using all visible components.
     * Note that this method is usually called after or before [verify], to also ensure that the provided partial Merkle
     * tree corresponds to the correct leaf in the top Merkle tree.
     * @param componentGroupEnum the [ComponentGroupEnum] that corresponds to the componentGroup for which we require full component visibility.
     * @throws ComponentVisibilityException if not all of the components are visible or if the component group is not present in the [FilteredTransaction].
     */
    @Throws(ComponentVisibilityException::class)
    fun checkAllComponentsVisible(componentGroupEnum: ComponentGroupEnum) {
        val group = filteredComponentGroups.firstOrNull { it.groupIndex == componentGroupEnum.ordinal }
        if (group == null) {
            // If we don't receive elements of a particular component, check if its ordinal is bigger that the
            // groupHashes.size or if the group hash is allOnesHash,
            // to ensure there were indeed no elements in the original wire transaction.
            visibilityCheck(componentGroupEnum.ordinal >= groupHashes.size || groupHashes[componentGroupEnum.ordinal] == SecureHash.allOnesHash) {
                "Did not receive components for group ${componentGroupEnum.ordinal} and cannot verify they didn't exist in the original wire transaction"
            }
        } else {
            visibilityCheck(group.groupIndex < groupHashes.size ) { "There is no matching component group hash for group ${group.groupIndex}" }
            val groupPartialRoot = groupHashes[group.groupIndex]
            val groupFullRoot = MerkleTree.getMerkleTree(group.components.mapIndexed { index, component -> componentHash(group.nonces[index], component) }).hash
            visibilityCheck(groupPartialRoot == groupFullRoot) { "Some components for group ${group.groupIndex} are not visible" }
        }
    }

    @Throws(ComponentVisibilityException::class)
    fun checkCommandVisibility(publicKey: PublicKey) {
        val commandSigners = componentGroups.firstOrNull { it.groupIndex == ComponentGroupEnum.SIGNERS_GROUP.ordinal }
        if(commandSigners == null) {
            // Because no list of signing keys is provided,  we should ensure all commands are visible.
            // TODO: Consider removing this requirement, as old clients might not follow this rule and they won't send all commands.
            checkAllComponentsVisible(ComponentGroupEnum.COMMANDS_GROUP)
        } else {
            checkAllComponentsVisible(ComponentGroupEnum.SIGNERS_GROUP)
            val expectedNumOfCommands = expectedNumOfCommands(publicKey, commandSigners)
            val receivedForThisKeyNumOfCommands = commands.filter { it.signers.contains(publicKey) }.size
            visibilityCheck(expectedNumOfCommands == receivedForThisKeyNumOfCommands) { "Expected $expectedNumOfCommands commands, but only $receivedForThisKeyNumOfCommands related commands are visible" }
        }
    }

    private fun expectedNumOfCommands(publicKey: PublicKey, commandSigners: ComponentGroup): Int {
        fun signersKeys (internalIndex: Int, opaqueBytes: OpaqueBytes): List<PublicKey> {
            try {
                return SerializedBytes<List<PublicKey>>(opaqueBytes.bytes).deserialize()
            } catch (e: Exception) {
                throw Exception("Malformed transaction, signers at index $internalIndex cannot be deserialised", e)
            }
        }

        return commandSigners.components
                .mapIndexed { internalIndex, opaqueBytes ->  signersKeys(internalIndex, opaqueBytes) }
                .filter { it.contains(publicKey) }.size
    }

    inline private fun verificationCheck(value: Boolean, lazyMessage: () -> Any) {
        if (!value) {
            val message = lazyMessage()
            throw FilteredTransactionVerificationException(id, message.toString())
        }
    }

    inline private fun visibilityCheck(value: Boolean, lazyMessage: () -> Any) {
        if (!value) {
            val message = lazyMessage()
            throw ComponentVisibilityException(id, message.toString())
        }
    }
}

/**
 * A FilteredComponentGroup is used to store the filtered list of transaction components of the same type in serialised form.
 * This is similar to [ComponentGroup], but it also includes the corresponding nonce per component.
 */
@CordaSerializable
data class FilteredComponentGroup(override val groupIndex: Int, override val components: List<OpaqueBytes>, val nonces: List<SecureHash>, val partialMerkleTree: PartialMerkleTree): ComponentGroup(groupIndex, components) {
    init {
        check(components.size == nonces.size) { "Size of transaction components and nonces do not match" }
    }
}

/** Thrown when checking for visibility of all-components in a group in [FilteredTransaction.checkAllComponentsVisible].
 * @param id transaction's id.
 * @param reason information about the exception.
 */
@CordaSerializable
class ComponentVisibilityException(val id: SecureHash, val reason: String) : CordaException("Component visibility error for transaction with id:$id. Reason: $reason")

/** Thrown when [FilteredTransaction.verify] fails.
 * @param id transaction's id.
 * @param reason information about the exception.
 */
@CordaSerializable
class FilteredTransactionVerificationException(val id: SecureHash, val reason: String) : CordaException("Transaction with id:$id cannot be verified. Reason: $reason")
