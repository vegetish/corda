package net.corda.nodeapi.internal.serialization.amqp

import net.corda.core.CordaRuntimeException
import net.corda.core.contracts.Contract
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TransactionState
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowException
import net.corda.core.identity.AbstractParty
import net.corda.core.internal.toX509CertHolder
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.SerializationFactory
import net.corda.core.transactions.LedgerTransaction
import net.corda.client.rpc.RPCException
import net.corda.nodeapi.internal.serialization.AbstractAMQPSerializationScheme
import net.corda.nodeapi.internal.serialization.AllWhitelist
import net.corda.nodeapi.internal.serialization.EmptyWhitelist
import net.corda.nodeapi.internal.serialization.amqp.SerializerFactory.Companion.isPrimitive
import net.corda.testing.BOB_IDENTITY
import net.corda.testing.MEGA_CORP
import net.corda.testing.MEGA_CORP_PUBKEY
import net.corda.testing.withTestSerialization
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.qpid.proton.amqp.*
import org.apache.qpid.proton.codec.DecoderImpl
import org.apache.qpid.proton.codec.EncoderImpl
import org.junit.Assert.assertNotSame
import org.junit.Assert.assertSame
import org.junit.Ignore
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.NotSerializableException
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.*
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SerializationOutputTests {
    data class Foo(val bar: String, val pub: Int)

    data class testFloat(val f: Float)

    data class testDouble(val d: Double)

    data class testShort(val s: Short)

    data class testBoolean(val b: Boolean)

    interface FooInterface {
        val pub: Int
    }

    data class FooImplements(val bar: String, override val pub: Int) : FooInterface

    data class FooImplementsAndList(val bar: String, override val pub: Int, val names: List<String>) : FooInterface

    data class WrapHashMap(val map: Map<String, String>)

    data class WrapFooListArray(val listArray: Array<List<Foo>>) {
        override fun equals(other: Any?): Boolean {
            return other is WrapFooListArray && Objects.deepEquals(listArray, other.listArray)
        }

        override fun hashCode(): Int {
            return 1 // This isn't used, but without overriding we get a warning.
        }
    }

    data class Woo(val fred: Int) {
        @Suppress("unused")
        val bob = "Bob"
    }

    data class Woo2(val fred: Int, val bob: String = "Bob") {
        @ConstructorForDeserialization constructor(fred: Int) : this(fred, "Ginger")
    }

    @CordaSerializable
    data class AnnotatedWoo(val fred: Int) {
        @Suppress("unused")
        val bob = "Bob"
    }

    class FooList : ArrayList<Foo>()

    @Suppress("AddVarianceModifier")
    data class GenericFoo<T>(val bar: String, val pub: T)

    data class ContainsGenericFoo(val contain: GenericFoo<String>)

    data class NestedGenericFoo<T>(val contain: GenericFoo<T>)

    data class ContainsNestedGenericFoo(val contain: NestedGenericFoo<String>)

    data class TreeMapWrapper(val tree: TreeMap<Int, Foo>)

    data class NavigableMapWrapper(val tree: NavigableMap<Int, Foo>)

    data class SortedSetWrapper(val set: SortedSet<Int>)

    open class InheritedGeneric<out X>(val foo: X)

    data class ExtendsGeneric(val bar: Int, val pub: String) : InheritedGeneric<String>(pub)

    interface GenericInterface<out X> {
        val pub: X
    }

    data class ImplementsGenericString(val bar: Int, override val pub: String) : GenericInterface<String>

    data class ImplementsGenericX<out Y>(val bar: Int, override val pub: Y) : GenericInterface<Y>

    abstract class AbstractGenericX<out Z> : GenericInterface<Z>

    data class InheritGenericX<out A>(val duke: Double, override val pub: A) : AbstractGenericX<A>()

    data class CapturesGenericX(val foo: GenericInterface<String>)

    object KotlinObject

    class Mismatch(fred: Int) {
        private val ginger: Int = fred

        override fun equals(other: Any?): Boolean = (other as? Mismatch)?.ginger == ginger
        override fun hashCode(): Int = ginger
    }

    class MismatchType(fred: Long) {
        private val ginger: Int = fred.toInt()

        override fun equals(other: Any?): Boolean = (other as? MismatchType)?.ginger == ginger
        override fun hashCode(): Int = ginger
    }

    @CordaSerializable
    interface AnnotatedInterface

    data class InheritAnnotation(val foo: String) : AnnotatedInterface

    data class PolymorphicProperty(val foo: FooInterface?)

    private inline fun <reified T : Any> serdes(obj: T,
                                                factory: SerializerFactory = SerializerFactory(
                                                        AllWhitelist, ClassLoader.getSystemClassLoader()),
                                                freshDeserializationFactory: SerializerFactory = SerializerFactory(
                                                        AllWhitelist, ClassLoader.getSystemClassLoader()),
                                                expectedEqual: Boolean = true,
                                                expectDeserializedEqual: Boolean = true): T {
        val ser = SerializationOutput(factory)
        val bytes = ser.serialize(obj)

        val decoder = DecoderImpl().apply {
            this.register(Envelope.DESCRIPTOR, Envelope.Companion)
            this.register(Schema.DESCRIPTOR, Schema.Companion)
            this.register(Descriptor.DESCRIPTOR, Descriptor.Companion)
            this.register(Field.DESCRIPTOR, Field.Companion)
            this.register(CompositeType.DESCRIPTOR, CompositeType.Companion)
            this.register(Choice.DESCRIPTOR, Choice.Companion)
            this.register(RestrictedType.DESCRIPTOR, RestrictedType.Companion)
            this.register(ReferencedObject.DESCRIPTOR, ReferencedObject.Companion)
        }
        EncoderImpl(decoder)
        decoder.setByteBuffer(ByteBuffer.wrap(bytes.bytes, 8, bytes.size - 8))
        // Check that a vanilla AMQP decoder can deserialize without schema.
        val result = decoder.readObject() as Envelope
        assertNotNull(result)

        val des = DeserializationInput(freshDeserializationFactory)
        val desObj = des.deserialize(bytes)
        assertTrue(Objects.deepEquals(obj, desObj) == expectedEqual)

        // Now repeat with a re-used factory
        val ser2 = SerializationOutput(factory)
        val des2 = DeserializationInput(factory)
        val desObj2 = des2.deserialize(ser2.serialize(obj))
        assertTrue(Objects.deepEquals(obj, desObj2) == expectedEqual)
        assertTrue(Objects.deepEquals(desObj, desObj2) == expectDeserializedEqual)

        // TODO: add some schema assertions to check correctly formed.
        return desObj
    }

    @Test
    fun isPrimitive() {
        assertTrue(isPrimitive(Character::class.java))
        assertTrue(isPrimitive(Boolean::class.java))
        assertTrue(isPrimitive(Byte::class.java))
        assertTrue(isPrimitive(UnsignedByte::class.java))
        assertTrue(isPrimitive(Short::class.java))
        assertTrue(isPrimitive(UnsignedShort::class.java))
        assertTrue(isPrimitive(Int::class.java))
        assertTrue(isPrimitive(UnsignedInteger::class.java))
        assertTrue(isPrimitive(Long::class.java))
        assertTrue(isPrimitive(UnsignedLong::class.java))
        assertTrue(isPrimitive(Float::class.java))
        assertTrue(isPrimitive(Double::class.java))
        assertTrue(isPrimitive(Decimal32::class.java))
        assertTrue(isPrimitive(Decimal64::class.java))
        assertTrue(isPrimitive(Decimal128::class.java))
        assertTrue(isPrimitive(Char::class.java))
        assertTrue(isPrimitive(Date::class.java))
        assertTrue(isPrimitive(UUID::class.java))
        assertTrue(isPrimitive(ByteArray::class.java))
        assertTrue(isPrimitive(String::class.java))
        assertTrue(isPrimitive(Symbol::class.java))
    }

    @Test
    fun `test foo`() {
        val obj = Foo("Hello World!", 123)
        serdes(obj)
    }

    @Test
    fun `test float`() {
        val obj = testFloat(10.0F)
        serdes(obj)
    }

    @Test
    fun `test double`() {
        val obj = testDouble(10.0)
        serdes(obj)
    }

    @Test
    fun `test short`() {
        val obj = testShort(1)
        serdes(obj)
    }

    @Test
    fun `test bool`() {
        val obj = testBoolean(true)
        serdes(obj)
    }

    @Test
    fun `test foo implements`() {
        val obj = FooImplements("Hello World!", 123)
        serdes(obj)
    }

    @Test
    fun `test foo implements and list`() {
        val obj = FooImplementsAndList("Hello World!", 123, listOf("Fred", "Ginger"))
        serdes(obj)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `test dislike of HashMap`() {
        val obj = WrapHashMap(HashMap())
        serdes(obj)
    }

    @Test
    fun `test string array`() {
        val obj = arrayOf("Fred", "Ginger")
        serdes(obj)
    }

    @Test
    fun `test foo array`() {
        val obj = arrayOf(Foo("Fred", 1), Foo("Ginger", 2))
        serdes(obj)
    }

    @Test
    fun `test top level list array`() {
        val obj = arrayOf(listOf("Fred", "Ginger"), listOf("Rogers", "Hammerstein"))
        serdes(obj)
    }

    @Test
    fun `test foo list array`() {
        val obj = WrapFooListArray(arrayOf(listOf(Foo("Fred", 1), Foo("Ginger", 2)), listOf(Foo("Rogers", 3), Foo("Hammerstein", 4))))
        serdes(obj)
    }

    @Test
    fun `test not all properties in constructor`() {
        val obj = Woo(2)
        serdes(obj)
    }

    @Test
    fun `test annotated constructor`() {
        val obj = Woo2(3)
        serdes(obj)
    }

    @Test(expected = NotSerializableException::class)
    fun `test whitelist`() {
        val obj = Woo2(4)
        serdes(obj, SerializerFactory(EmptyWhitelist, ClassLoader.getSystemClassLoader()))
    }

    @Test
    fun `test annotation whitelisting`() {
        val obj = AnnotatedWoo(5)
        serdes(obj, SerializerFactory(EmptyWhitelist, ClassLoader.getSystemClassLoader()))
    }

    @Test(expected = NotSerializableException::class)
    fun `test generic list subclass is not supported`() {
        val obj = FooList()
        serdes(obj)
    }

    @Test
    fun `test generic foo`() {
        val obj = GenericFoo("Fred", "Ginger")
        serdes(obj)
    }

    @Test
    fun `test generic foo as property`() {
        val obj = ContainsGenericFoo(GenericFoo("Fred", "Ginger"))
        serdes(obj)
    }

    @Test
    fun `test nested generic foo as property`() {
        val obj = ContainsNestedGenericFoo(NestedGenericFoo(GenericFoo("Fred", "Ginger")))
        serdes(obj)
    }

    // TODO: Generic interfaces / superclasses

    @Test
    fun `test extends generic`() {
        val obj = ExtendsGeneric(1, "Ginger")
        serdes(obj)
    }

    @Test
    fun `test implements generic`() {
        val obj = ImplementsGenericString(1, "Ginger")
        serdes(obj)
    }

    @Test
    fun `test implements generic captured`() {
        val obj = CapturesGenericX(ImplementsGenericX(1, "Ginger"))
        serdes(obj)
    }


    @Test
    fun `test inherits generic captured`() {
        val obj = CapturesGenericX(InheritGenericX(1.0, "Ginger"))
        serdes(obj)
    }

    @Test
    fun `test TreeMap`() {
        val obj = TreeMap<Int, Foo>()
        obj[456] = Foo("Fred", 123)
        serdes(obj)
    }

    @Test
    fun `test TreeMap property`() {
        val obj = TreeMapWrapper(TreeMap())
        obj.tree[456] = Foo("Fred", 123)
        serdes(obj)
    }

    @Test
    fun `test NavigableMap property`() {
        val obj = NavigableMapWrapper(TreeMap<Int, Foo>())
        obj.tree[456] = Foo("Fred", 123)
        serdes(obj)
    }

    @Test
    fun `test SortedSet property`() {
        val obj = SortedSetWrapper(TreeSet<Int>())
        obj.set += 456
        serdes(obj)
    }

    @Test(expected = NotSerializableException::class)
    fun `test mismatched property and constructor naming`() {
        val obj = Mismatch(456)
        serdes(obj)
    }

    @Test(expected = NotSerializableException::class)
    fun `test mismatched property and constructor type`() {
        val obj = MismatchType(456)
        serdes(obj)
    }

    @Test
    fun `test custom serializers on public key`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.PublicKeySerializer)
        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.PublicKeySerializer)
        val obj = MEGA_CORP_PUBKEY
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test annotation is inherited`() {
        val obj = InheritAnnotation("blah")
        serdes(obj, SerializerFactory(EmptyWhitelist, ClassLoader.getSystemClassLoader()))
    }

    @Test
    fun `test throwables serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory2))

        val t = IllegalAccessException("message").fillInStackTrace()

        val desThrowable = serdesThrowableWithInternalInfo(t, factory, factory2, false)
        assertSerializedThrowableEquivalent(t, desThrowable)
    }

    private fun serdesThrowableWithInternalInfo(t: Throwable, factory: SerializerFactory, factory2: SerializerFactory, expectedEqual: Boolean = true): Throwable = withTestSerialization {
        val newContext = SerializationFactory.defaultFactory.defaultContext.withProperty(CommonPropertyNames.IncludeInternalInfo, true)

        val deserializedObj = SerializationFactory.defaultFactory.asCurrent { withCurrentContext(newContext) { serdes(t, factory, factory2, expectedEqual) } }
        return deserializedObj
    }

    @Test
    fun `test complex throwables serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory2))

        try {
            try {
                throw IOException("Layer 1")
            } catch (t: Throwable) {
                throw IllegalStateException("Layer 2", t)
            }
        } catch (t: Throwable) {
            val desThrowable = serdesThrowableWithInternalInfo(t, factory, factory2, false)
            assertSerializedThrowableEquivalent(t, desThrowable)
        }
    }

    private fun assertSerializedThrowableEquivalent(t: Throwable, desThrowable: Throwable) {
        assertTrue(desThrowable is CordaRuntimeException) // Since we don't handle the other case(s) yet
        if (desThrowable is CordaRuntimeException) {
            assertEquals("${t.javaClass.name}: ${t.message}", desThrowable.message)
            assertTrue(Objects.deepEquals(t.stackTrace, desThrowable.stackTrace))
            assertEquals(t.suppressed.size, desThrowable.suppressed.size)
            t.suppressed.zip(desThrowable.suppressed).forEach { (before, after) -> assertSerializedThrowableEquivalent(before, after) }
        }
    }

    @Test
    fun `test suppressed throwables serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory2))

        try {
            try {
                throw IOException("Layer 1")
            } catch (t: Throwable) {
                val e = IllegalStateException("Layer 2")
                e.addSuppressed(t)
                throw e
            }
        } catch (t: Throwable) {
            val desThrowable = serdesThrowableWithInternalInfo(t, factory, factory2, false)
            assertSerializedThrowableEquivalent(t, desThrowable)
        }
    }

    @Test
    fun `test flow corda exception subclasses serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory2))

        val obj = FlowException("message").fillInStackTrace()
        serdesThrowableWithInternalInfo(obj, factory, factory2)
    }

    @Test
    fun `test RPC corda exception subclasses serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.ThrowableSerializer(factory2))

        val obj = RPCException("message").fillInStackTrace()
        serdesThrowableWithInternalInfo(obj, factory, factory2)
    }

    @Test
    fun `test polymorphic property`() {
        val obj = PolymorphicProperty(FooImplements("Ginger", 12))
        serdes(obj)
    }

    @Test
    fun `test null polymorphic property`() {
        val obj = PolymorphicProperty(null)
        serdes(obj)
    }

    @Test
    fun `test kotlin object`() {
        serdes(KotlinObject)
    }

    object FooContract : Contract {
        override fun verify(tx: LedgerTransaction) {

        }
    }

    val FOO_PROGRAM_ID = "net.corda.nodeapi.internal.serialization.amqp.SerializationOutputTests.FooContract"

    class FooState : ContractState {
        override val participants: List<AbstractParty> = emptyList()
    }

    @Test
    fun `test transaction state`() {
        val state = TransactionState(FooState(), FOO_PROGRAM_ID, MEGA_CORP)

        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        AbstractAMQPSerializationScheme.registerCustomSerializers(factory)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        AbstractAMQPSerializationScheme.registerCustomSerializers(factory2)

        val desState = serdes(state, factory, factory2, expectedEqual = false, expectDeserializedEqual = false)
        assertTrue((desState as TransactionState<*>).data is FooState)
        assertTrue(desState.notary == state.notary)
        assertTrue(desState.encumbrance == state.encumbrance)
    }

    @Test
    fun `test currencies serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.CurrencySerializer)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.CurrencySerializer)

        val obj = Currency.getInstance("USD")
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test big decimals serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.BigDecimalSerializer)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.BigDecimalSerializer)

        val obj = BigDecimal("100000000000000000000000000000.00")
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test instants serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.InstantSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.InstantSerializer(factory2))

        val obj = Instant.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test durations serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.DurationSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.DurationSerializer(factory2))

        val obj = Duration.of(1000000L, ChronoUnit.MILLIS)
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test local date serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.LocalDateSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.LocalDateSerializer(factory2))

        val obj = LocalDate.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test local time serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.LocalTimeSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.LocalTimeSerializer(factory2))

        val obj = LocalTime.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test local date time serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.LocalDateTimeSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.LocalDateTimeSerializer(factory2))

        val obj = LocalDateTime.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test zoned date time serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.ZonedDateTimeSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.ZonedDateTimeSerializer(factory2))

        val obj = ZonedDateTime.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test offset time serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.OffsetTimeSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.OffsetTimeSerializer(factory2))

        val obj = OffsetTime.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test offset date time serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.OffsetDateTimeSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.OffsetDateTimeSerializer(factory2))

        val obj = OffsetDateTime.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test year serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.YearSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.YearSerializer(factory2))

        val obj = Year.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test year month serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.YearMonthSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.YearMonthSerializer(factory2))

        val obj = YearMonth.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test month day serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.MonthDaySerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.MonthDaySerializer(factory2))

        val obj = MonthDay.now()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test period serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.PeriodSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.PeriodSerializer(factory2))

        val obj = Period.of(99, 98, 97)
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test month serialize`() {
        val obj = Month.APRIL
        serdes(obj)
    }

    @Test
    fun `test day of week serialize`() {
        val obj = DayOfWeek.FRIDAY
        serdes(obj)
    }

    @Test
    fun `test certificate holder serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.X509CertificateHolderSerializer)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.X509CertificateHolderSerializer)

        val obj = BOB_IDENTITY.certificate.toX509CertHolder()
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test party and certificate serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.PartyAndCertificateSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.PartyAndCertificateSerializer(factory2))

        val obj = BOB_IDENTITY
        serdes(obj, factory, factory2)
    }

    class OtherGeneric<T : Any>

    open class GenericSuperclass<T : Any>(val param: OtherGeneric<T>)

    class GenericSubclass(param: OtherGeneric<String>) : GenericSuperclass<String>(param) {
        override fun equals(other: Any?): Boolean = other is GenericSubclass // This is a bit lame but we just want to check it doesn't throw exceptions
    }

    @Test
    fun `test generic in constructor serialize`() {
        val obj = GenericSubclass(OtherGeneric<String>())
        serdes(obj)
    }

    @Test
    fun `test StateRef serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())

        val obj = StateRef(SecureHash.randomSHA256(), 0)
        serdes(obj, factory, factory2)
    }

    interface Container

    data class SimpleContainer(val one: String, val another: String) : Container

    data class ParentContainer(val left: SimpleContainer, val right: Container)

    @Test
    fun `test object referenced multiple times`() {
        val simple = SimpleContainer("Fred", "Ginger")
        val parentContainer = ParentContainer(simple, simple)
        assertSame(parentContainer.left, parentContainer.right)

        val parentCopy = serdes(parentContainer)
        assertSame(parentCopy.left, parentCopy.right)
    }

    data class TestNode(val content: String, val children: MutableCollection<TestNode> = ArrayList())

    @Test
    @Ignore("Ignored due to cyclic graphs not currently supported by AMQP serialization")
    fun `test serialization of cyclic graph`() {
        val nodeA = TestNode("A")
        val nodeB = TestNode("B", ArrayList(Arrays.asList(nodeA)))
        nodeA.children.add(nodeB)

        // Also blows with StackOverflow error
        assertTrue(nodeB.hashCode() > 0)

        val bCopy = serdes(nodeB)
        assertEquals("A", bCopy.children.single().content)
    }

    data class Bob(val byteArrays: List<ByteArray>)

    @Test
    fun `test list of byte arrays`() {
        val a = ByteArray(1)
        val b = ByteArray(2)
        val obj = Bob(listOf(a, b, a))

        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        val obj2 = serdes(obj, factory, factory2, false, false)

        assertNotSame(obj2.byteArrays[0], obj2.byteArrays[2])
    }

    data class Vic(val a: List<String>, val b: List<String>)

    @Test
    fun `test generics ignored from graph logic`() {
        val a = listOf("a", "b")
        val obj = Vic(a, a)

        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        val objCopy = serdes(obj, factory, factory2)
        assertSame(objCopy.a, objCopy.b)
    }

    data class Spike private constructor(val a: String) {
        constructor() : this("a")
    }

    @Test
    fun `test private constructor`() {
        val obj = Spike()

        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        serdes(obj, factory, factory2)
    }

    data class BigDecimals(val a: BigDecimal, val b: BigDecimal)

    @Test
    fun `test toString custom serializer`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.BigDecimalSerializer)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.BigDecimalSerializer)

        val obj = BigDecimals(BigDecimal.TEN, BigDecimal.TEN)
        val objCopy = serdes(obj, factory, factory2)
        assertEquals(objCopy.a, objCopy.b)
    }

    data class ByteArrays(val a: ByteArray, val b: ByteArray)

    @Test
    fun `test byte arrays not reference counted`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.BigDecimalSerializer)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.BigDecimalSerializer)

        val bytes = ByteArray(1)
        val obj = ByteArrays(bytes, bytes)
        val objCopy = serdes(obj, factory, factory2, false, false)
        assertNotSame(objCopy.a, objCopy.b)
    }

    @Test
    fun `test StringBuffer serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.StringBufferSerializer)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.StringBufferSerializer)

        val obj = StringBuffer("Bob")
        val obj2 = serdes(obj, factory, factory2, false, false)
        assertEquals(obj.toString(), obj2.toString())
    }

    @Test
    fun `test SimpleString serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.SimpleStringSerializer)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.SimpleStringSerializer)

        val obj = SimpleString("Bob")
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test kotlin Unit serialize`() {
        val obj = Unit
        serdes(obj)
    }

    @Test
    fun `test kotlin Pair serialize`() {
        val obj = Pair("a", 3)
        serdes(obj)
    }

    @Test
    fun `test InputStream serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.InputStreamSerializer)

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.InputStreamSerializer)
        val bytes = ByteArray(10) { it.toByte() }
        val obj = ByteArrayInputStream(bytes)
        val obj2 = serdes(obj, factory, factory2, expectedEqual = false, expectDeserializedEqual = false)
        val obj3 = ByteArrayInputStream(bytes)  // Can't use original since the stream pointer has moved.
        assertEquals(obj3.available(), obj2.available())
        assertEquals(obj3.read(), obj2.read())
    }

    @Test
    fun `test EnumSet serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.EnumSetSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.EnumSetSerializer(factory2))

        val obj = EnumSet.of(Month.APRIL, Month.AUGUST)
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test BitSet serialize`() {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory.register(net.corda.nodeapi.internal.serialization.amqp.custom.BitSetSerializer(factory))

        val factory2 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        factory2.register(net.corda.nodeapi.internal.serialization.amqp.custom.BitSetSerializer(factory2))

        val obj = BitSet.valueOf(kotlin.ByteArray(16) { it.toByte() }).get(0, 123)
        serdes(obj, factory, factory2)
    }

    @Test
    fun `test EnumMap serialize`() {
        val obj = EnumMap<Month, Int>(Month::class.java)
        obj[Month.APRIL] = Month.APRIL.value
        obj[Month.AUGUST] = Month.AUGUST.value
        serdes(obj)
    }
}