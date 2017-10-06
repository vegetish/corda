@file:JvmName("ConfigUtilities")
package net.corda.nodeapi.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigUtil
import com.typesafe.config.ConfigValueFactory
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.noneOrSingle
import net.corda.core.internal.uncheckedCast
import net.corda.core.utilities.NetworkHostAndPort
import org.slf4j.LoggerFactory
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType
import java.net.Proxy
import java.net.URL
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.Temporal
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure

@Target(AnnotationTarget.PROPERTY)
annotation class OldConfig(val value: String)

// TODO Move other config parsing to use parseAs and remove this
operator fun <T : Any> Config.getValue(receiver: Any, metadata: KProperty<*>): T {
    return getValueInternal(metadata.name, metadata.returnType)
}

fun <T : Any> Config.parseAs(clazz: KClass<T>): T {
    require(clazz.isData) { "Only Kotlin data classes can be parsed" }
    val constructor = clazz.primaryConstructor!!
    val args = constructor.parameters
            .filterNot { it.isOptional && !hasPath(it.name!!) }
            .associateBy({ it }) { param ->
                // Get the matching property for this parameter
                val property = clazz.memberProperties.first { it.name == param.name }
                val path = defaultToOldPath(property)
                getValueInternal<Any>(path, param.type)
            }
    return constructor.callBy(args)
}

inline fun <reified T : Any> Config.parseAs(): T = parseAs(T::class)

fun Config.toProperties(): Properties {
    return entrySet().associateByTo(
            Properties(),
            { ConfigUtil.splitPath(it.key).joinToString(".") },
            { it.value.unwrapped().toString() })
}

private fun <T : Any> Config.getValueInternal(path: String, type: KType): T {
    return uncheckedCast(if (type.arguments.isEmpty()) getSingleValue(path, type) else getCollectionValue(path, type))
}

private fun Config.getSingleValue(path: String, type: KType): Any? {
    if (type.isMarkedNullable && !hasPath(path)) return null
    val typeClass = type.jvmErasure
    return when (typeClass) {
        String::class -> getString(path)
        Int::class -> getInt(path)
        Long::class -> getLong(path)
        Double::class -> getDouble(path)
        Boolean::class -> getBoolean(path)
        LocalDate::class -> LocalDate.parse(getString(path))
        Instant::class -> Instant.parse(getString(path))
        NetworkHostAndPort::class -> NetworkHostAndPort.parse(getString(path))
        Path::class -> Paths.get(getString(path))
        URL::class -> URL(getString(path))
        CordaX500Name::class -> CordaX500Name.parse(getString(path))
        Properties::class -> getConfig(path).toProperties()
        else -> if (typeClass.java.isEnum) {
            parseEnum(typeClass.java, getString(path))
        } else {
            getConfig(path).parseAs(typeClass)
        }
    }
}

private fun Config.getCollectionValue(path: String, type: KType): Collection<Any> {
    val typeClass = type.jvmErasure
    require(typeClass == List::class || typeClass == Set::class) { "$typeClass is not supported" }
    val elementClass = type.arguments[0].type?.jvmErasure ?: throw IllegalArgumentException("Cannot work with star projection: $type")
    if (!hasPath(path)) {
        return if (typeClass == List::class) emptyList() else emptySet()
    }
    val values: List<Any> = when (elementClass) {
        String::class -> getStringList(path)
        Int::class -> getIntList(path)
        Long::class -> getLongList(path)
        Double::class -> getDoubleList(path)
        Boolean::class -> getBooleanList(path)
        LocalDate::class -> getStringList(path).map(LocalDate::parse)
        Instant::class -> getStringList(path).map(Instant::parse)
        NetworkHostAndPort::class -> getStringList(path).map(NetworkHostAndPort.Companion::parse)
        Path::class -> getStringList(path).map { Paths.get(it) }
        URL::class -> getStringList(path).map(::URL)
        CordaX500Name::class -> getStringList(path).map(CordaX500Name.Companion::parse)
        Properties::class -> getConfigList(path).map(Config::toProperties)
        else -> if (elementClass.java.isEnum) {
            getStringList(path).map { parseEnum(elementClass.java, it) }
        } else {
            getConfigList(path).map { it.parseAs(elementClass) }
        }
    }
    return if (typeClass == Set::class) values.toSet() else values
}

private fun Config.defaultToOldPath(property: KProperty<*>): String {
    if (!hasPath(property.name)) {
        val oldConfig = property.annotations.filterIsInstance<OldConfig>().noneOrSingle()
        if (oldConfig != null && hasPath(oldConfig.value)) {
            logger.warn("Config key ${oldConfig.value} has been deprecated and will be removed in a future release. " +
                    "Use ${property.name} instead")
            return oldConfig.value
        }
    }
    return property.name
}

private fun parseEnum(enumType: Class<*>, name: String): Enum<*> = enumBridge<Proxy.Type>(uncheckedCast(enumType), name) // Any enum will do

private fun <T : Enum<T>> enumBridge(clazz: Class<T>, name: String): T = java.lang.Enum.valueOf(clazz, name)

/**
 * Convert the receiver object into a [Config]. This does the inverse action of [parseAs].
 */
fun Any.toConfig(): Config = ConfigValueFactory.fromMap(toConfigMap()).toConfig()

@Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")
// Reflect over the fields of the receiver and generate a value Map that can use to create Config object.
private fun Any.toConfigMap(): Map<String, Any> {
    val values = HashMap<String, Any>()
    for (field in javaClass.declaredFields) {
        if (field.isSynthetic) continue
        field.isAccessible = true
        val value = field.get(this) ?: continue
        val configValue = if (value is String || value is Boolean || value is Number) {
            // These types are supported by Config as use as is
            value
        } else if (value is Temporal || value is NetworkHostAndPort || value is CordaX500Name || value is Path || value is URL) {
            // These types make sense to be represented as Strings and the exact inverse parsing function for use in parseAs
            value.toString()
        } else if (value is Enum<*>) {
            // Expicitly use the Enum's name in case the toString is overridden, which would make parsing problematic.
            value.name
        } else if (value is Properties) {
            // For Properties we treat keys with . as nested configs
            ConfigFactory.parseMap(uncheckedCast(value)).root()
        } else if (value is Iterable<*>) {
            value.toConfigIterable(field)
        } else {
            // Else this is a custom object recursed over
            value.toConfigMap()
        }
        values[field.name] = configValue
    }
    return values
}

// For Iterables figure out the type parameter and apply the same logic as above on the individual elements.
private fun Iterable<*>.toConfigIterable(field: Field): Iterable<Any?> {
    val elementType = (field.genericType as ParameterizedType).actualTypeArguments[0] as Class<*>
    return when (elementType) {
        // For the types already supported by Config we can use the Iterable as is
        String::class.java -> this
        Integer::class.java -> this
        java.lang.Long::class.java -> this
        java.lang.Double::class.java -> this
        java.lang.Boolean::class.java -> this
        LocalDate::class.java -> map(Any?::toString)
        Instant::class.java -> map(Any?::toString)
        NetworkHostAndPort::class.java -> map(Any?::toString)
        Path::class.java -> map(Any?::toString)
        URL::class.java -> map(Any?::toString)
        CordaX500Name::class.java -> map(Any?::toString)
        Properties::class.java -> map { ConfigFactory.parseMap(uncheckedCast(it)).root() }
        else -> if (elementType.isEnum) {
            map { (it as Enum<*>).name }
        } else {
            map { it?.toConfigMap() }
        }
    }
}

private val logger = LoggerFactory.getLogger("net.corda.nodeapi.config")
