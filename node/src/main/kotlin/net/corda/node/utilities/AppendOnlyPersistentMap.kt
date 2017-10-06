package net.corda.node.utilities

import net.corda.core.utilities.loggerFor
import java.util.*


/**
 * Implements a caching layer on top of an *append-only* table accessed via Hibernate mapping. Note that if the same key is [set] twice the
 * behaviour is unpredictable! There is a best-effort check for double inserts, but this should *not* be relied on, so
 * ONLY USE THIS IF YOUR TABLE IS APPEND-ONLY
 */
class AppendOnlyPersistentMap<K, V, E, out EK>(
        val toPersistentEntityKey: (K) -> EK,
        val fromPersistentEntity: (E) -> Pair<K, V>,
        val toPersistentEntity: (key: K, value: V) -> E,
        val persistentEntityClass: Class<E>,
        cacheBound: Long = 1024
) { //TODO determine cacheBound based on entity class later or with node config allowing tuning, or using some heuristic based on heap size

    private companion object {
        val log = loggerFor<AppendOnlyPersistentMap<*, *, *, *>>()
    }

    private val cache = NonInvalidatingCache<K, Optional<V>>(
            bound = cacheBound,
            concurrencyLevel = 8,
            loadFunction = { key -> Optional.ofNullable(loadValue(key)) }
    )

    /**
     * Returns the value associated with the key, first loading that value from the storage if necessary.
     */
    operator fun get(key: K): V? {
        return cache.get(key).orElse(null)
    }

    val size get() = allPersisted().toList().size

    /**
     * Returns all key/value pairs from the underlying storage.
     */
    fun allPersisted(): Sequence<Pair<K, V>> {
        val criteriaQuery = DatabaseTransactionManager.current().session.criteriaBuilder.createQuery(persistentEntityClass)
        val root = criteriaQuery.from(persistentEntityClass)
        criteriaQuery.select(root)
        val query = DatabaseTransactionManager.current().session.createQuery(criteriaQuery)
        val result = query.resultList
        return result.map { x -> fromPersistentEntity(x) }.asSequence()
    }

    private tailrec fun set(key: K, value: V, logWarning: Boolean, store: (K, V) -> V?): Boolean {
        var insertionAttempt = false
        var isUnique = true
        val existingInCache = cache.get(key) {
            // Thread safe, if multiple threads may wait until the first one has loaded.
            insertionAttempt = true
            // Key wasn't in the cache and might be in the underlying storage.
            // Depending on 'store' method, this may insert without checking key duplication or it may avoid inserting a duplicated key.
            val existingInDb = store(key, value)
            if (existingInDb != null) { // Always reuse an existing value from the storage of a duplicated key.
                Optional.of(existingInDb)
            } else {
                Optional.of(value)
            }
        }
        if (!insertionAttempt) {
            if (existingInCache.isPresent) {
                // Key already exists in cache, do nothing.
                isUnique = false
            } else {
                // This happens when the key was queried before with no value associated. We invalidate the cached null
                // value and recursively call set again. This is to avoid race conditions where another thread queries after
                // the invalidate but before the set.
                cache.invalidate(key)
                return set(key, value, logWarning, store)
            }
        }
        if (logWarning && !isUnique) {
            log.warn("Double insert in ${this.javaClass.name} for entity class $persistentEntityClass key $key, not inserting the second time")
        }
        return isUnique
    }

    /**
     * Associates the specified value with the specified key in this map and persists it.
     * If the map previously contained a mapping for the key, the behaviour is unpredictable and may throw an error from the underlying storage.
     */
    operator fun set(key: K, value: V) =
            set(key, value, logWarning = false) { k, v ->
                DatabaseTransactionManager.current().session.save(toPersistentEntity(k, v))
                null
            }

    /**
     * Associates the specified value with the specified key in this map and persists it.
     * If the map previously contained a mapping for the key, the old value is not replaced.
     * @return true if added key was unique, otherwise false
     */
    fun addWithDuplicatesAllowed(key: K, value: V, logWarning: Boolean = true): Boolean =
            set(key, value, logWarning) { k, v ->
                val existingEntry = DatabaseTransactionManager.current().session.find(persistentEntityClass, toPersistentEntityKey(k))
                if (existingEntry == null) {
                    DatabaseTransactionManager.current().session.save(toPersistentEntity(k, v))
                    null
                } else {
                    fromPersistentEntity(existingEntry).second
                }
            }

    fun putAll(entries: Map<K, V>) {
        entries.forEach {
            set(it.key, it.value)
        }
    }

    private fun loadValue(key: K): V? {
        val result = DatabaseTransactionManager.current().session.find(persistentEntityClass, toPersistentEntityKey(key))
        return result?.let(fromPersistentEntity)?.second
    }

    operator fun contains(key: K) = get(key) != null

    /**
     * Removes all of the mappings from this map and underlying storage. The map will be empty after this call returns.
     * WARNING!! The method is not thread safe.
     */
    fun clear() {
        val session = DatabaseTransactionManager.current().session
        val deleteQuery = session.criteriaBuilder.createCriteriaDelete(persistentEntityClass)
        deleteQuery.from(persistentEntityClass)
        session.createQuery(deleteQuery).executeUpdate()
        cache.invalidateAll()
    }
}
