package net.corda.nodeapi.internal.serialization.amqp

import org.apache.qpid.proton.amqp.Symbol
import org.apache.qpid.proton.codec.Data
import java.io.NotSerializableException
import java.lang.reflect.Type

/**
 * Serialization / deserialization of arrays.
 */
open class ArraySerializer(override val type: Type, factory: SerializerFactory) : AMQPSerializer<Any> {
    companion object {
        fun make(type: Type, factory: SerializerFactory) = when (type) {
            Array<Char>::class.java -> CharArraySerializer(factory)
            else -> ArraySerializer(type, factory)
        }
    }

    // because this might be an array of array of primitives (to any recursive depth) and
    // because we care that the lowest type is unboxed we can't rely on the inbuilt type
    // id to generate it properly (it will always return [[[Ljava.lang.type -> type[][][]
    // for example).
    //
    // We *need* to retain knowledge for AMQP deserialisation weather that lowest primitive
    // was boxed or unboxed so just infer it recursively
    private fun calcTypeName(type: Type): String =
            if (type.componentType().isArray()) {
                val typeName = calcTypeName(type.componentType()); "$typeName[]"
            } else {
                val arrayType = if (type.asClass()!!.componentType.isPrimitive) "[p]" else "[]"
                "${type.componentType().typeName}$arrayType"
            }

    override val typeDescriptor by lazy { Symbol.valueOf("$DESCRIPTOR_DOMAIN:${fingerprintForType(type, factory)}") }
    internal val elementType: Type by lazy { type.componentType() }
    internal open val typeName by lazy { calcTypeName(type) }

    internal val typeNotation: TypeNotation by lazy {
        RestrictedType(typeName, null, emptyList(), "list", Descriptor(typeDescriptor), emptyList())
    }

    override fun writeClassInfo(output: SerializationOutput) {
        if (output.writeTypeNotations(typeNotation)) {
            output.requireSerializer(elementType)
        }
    }

    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        // Write described
        data.withDescribed(typeNotation.descriptor) {
            withList {
                for (entry in obj as Array<*>) {
                    output.writeObjectOrNull(entry, this, elementType)
                }
            }
        }
    }

    override fun readObject(obj: Any, schema: Schema, input: DeserializationInput): Any {
        if (obj is List<*>) {
            return obj.map { input.readObjectOrNull(it, schema, elementType) }.toArrayOfType(elementType)
        } else throw NotSerializableException("Expected a List but found $obj")
    }

    open fun <T> List<T>.toArrayOfType(type: Type): Any {
        val elementType = type.asClass() ?: throw NotSerializableException("Unexpected array element type $type")
        val list = this
        return java.lang.reflect.Array.newInstance(elementType, this.size).apply {
            (0..lastIndex).forEach { java.lang.reflect.Array.set(this, it, list[it]) }
        }
    }
}

// Boxed Character arrays required a specialisation to handle the type conversion properly when populating
// the array since Kotlin won't allow an implicit cast from Int (as they're stored as 16bit ints) to Char
class CharArraySerializer(factory: SerializerFactory) : ArraySerializer(Array<Char>::class.java, factory) {
    override fun <T> List<T>.toArrayOfType(type: Type): Any {
        val elementType = type.asClass() ?: throw NotSerializableException("Unexpected array element type $type")
        val list = this
        return java.lang.reflect.Array.newInstance(elementType, this.size).apply {
            (0..lastIndex).forEach { java.lang.reflect.Array.set(this, it, (list[it] as Int).toChar()) }
        }
    }
}

// Specialisation of [ArraySerializer] that handles arrays of unboxed java primitive types
abstract class PrimArraySerializer(type: Type, factory: SerializerFactory) : ArraySerializer(type, factory) {
    companion object {
        // We don't need to handle the unboxed byte type as that is coercible to a byte array, but
        // the other 7 primitive types we do
        val primTypes: Map<Type, (SerializerFactory) -> PrimArraySerializer> = mapOf(
                IntArray::class.java to { f -> PrimIntArraySerializer(f) },
                CharArray::class.java to { f -> PrimCharArraySerializer(f) },
                BooleanArray::class.java to { f -> PrimBooleanArraySerializer(f) },
                FloatArray::class.java to { f -> PrimFloatArraySerializer(f) },
                ShortArray::class.java to { f -> PrimShortArraySerializer(f) },
                DoubleArray::class.java to { f -> PrimDoubleArraySerializer(f) },
                LongArray::class.java to { f -> PrimLongArraySerializer(f) }
                // ByteArray::class.java <-> NOT NEEDED HERE (see comment above)
        )

        fun make(type: Type, factory: SerializerFactory) = primTypes[type]!!(factory)
    }

    fun localWriteObject(data: Data, func: () -> Unit) {
        data.withDescribed(typeNotation.descriptor) { withList { func() } }
    }
}

class PrimIntArraySerializer(factory: SerializerFactory) :
        PrimArraySerializer(IntArray::class.java, factory) {
    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        localWriteObject(data) { (obj as IntArray).forEach { output.writeObjectOrNull(it, data, elementType) } }
    }
}

class PrimCharArraySerializer(factory: SerializerFactory) :
        PrimArraySerializer(CharArray::class.java, factory) {
    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        localWriteObject(data) { (obj as CharArray).forEach { output.writeObjectOrNull(it, data, elementType) } }
    }

    override fun <T> List<T>.toArrayOfType(type: Type): Any {
        val elementType = type.asClass() ?: throw NotSerializableException("Unexpected array element type $type")
        val list = this
        return java.lang.reflect.Array.newInstance(elementType, this.size).apply {
            val array = this
            (0..lastIndex).forEach { java.lang.reflect.Array.set(array, it, (list[it] as Int).toChar()) }
        }
    }
}

class PrimBooleanArraySerializer(factory: SerializerFactory) :
        PrimArraySerializer(BooleanArray::class.java, factory) {
    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        localWriteObject(data) { (obj as BooleanArray).forEach { output.writeObjectOrNull(it, data, elementType) } }
    }
}

class PrimDoubleArraySerializer(factory: SerializerFactory) :
        PrimArraySerializer(DoubleArray::class.java, factory) {
    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        localWriteObject(data) { (obj as DoubleArray).forEach { output.writeObjectOrNull(it, data, elementType) } }
    }
}

class PrimFloatArraySerializer(factory: SerializerFactory) :
        PrimArraySerializer(FloatArray::class.java, factory) {
    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        localWriteObject(data) { (obj as FloatArray).forEach { output.writeObjectOrNull(it, data, elementType) } }
    }
}

class PrimShortArraySerializer(factory: SerializerFactory) :
        PrimArraySerializer(ShortArray::class.java, factory) {
    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        localWriteObject(data) { (obj as ShortArray).forEach { output.writeObjectOrNull(it, data, elementType) } }
    }
}

class PrimLongArraySerializer(factory: SerializerFactory) :
        PrimArraySerializer(LongArray::class.java, factory) {
    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        localWriteObject(data) { (obj as LongArray).forEach { output.writeObjectOrNull(it, data, elementType) } }
    }
}
