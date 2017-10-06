package net.corda.nodeapi.internal.serialization.amqp

import net.corda.core.serialization.ClassWhitelist
import net.corda.core.serialization.SerializedBytes
import net.corda.nodeapi.internal.serialization.AllWhitelist
import net.corda.nodeapi.internal.serialization.carpenter.ClassCarpenter
import org.assertj.core.api.Assertions
import org.junit.Test
import java.io.File
import java.io.NotSerializableException
import java.lang.reflect.Type
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertEquals

class InStatic : Exception("Help!, help!, I'm being repressed")

class C {
    companion object {
        init {
            throw InStatic()
        }
    }
}

// To re-setup the resource file for the tests
//   * deserializeTest
//   * deserializeTest2
// comment out the companion object from here,  comment out the test code and uncomment
// the generation code, then re-run the test and copy the file shown in the output print
// to the resource directory
class C2(var b: Int) {
    /*
    companion object {
        init {
            throw InStatic()
        }
    }
    */
}

class StaticInitialisationOfSerializedObjectTest {
    @Test(expected = java.lang.ExceptionInInitializerError::class)
    fun itBlowsUp() {
        C()
    }

    @Test
    fun KotlinObjectWithCompanionObject() {
        data class D(val c: C)

        val sf = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())

        val typeMap = sf::class.java.getDeclaredField("serializersByType")
        typeMap.isAccessible = true

        @Suppress("UNCHECKED_CAST")
        val serialisersByType = typeMap.get(sf) as ConcurrentHashMap<Type, AMQPSerializer<Any>>

        // pre building a serializer, we shouldn't have anything registered
        assertEquals(0, serialisersByType.size)

        // build a serializer for type D without an instance of it to serialise, since
        // we can't actually construct one
        sf.get(null, D::class.java)

        // post creation of the serializer we should have one element in the map, this
        // proves we didn't statically construct an instance of C when building the serializer
        assertEquals(1, serialisersByType.size)
    }


    @Test
    fun deserializeTest() {
        data class D(val c: C2)

        val path = EvolvabilityTests::class.java.getResource("StaticInitialisationOfSerializedObjectTest.deserializeTest")
        val f = File(path.toURI())

        // Original version of the class for the serialised version of this class
        //
        //val sf1 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        //val sc = SerializationOutput(sf1).serialize(D(C2(20)))
        //f.writeBytes(sc.bytes)
        //println (path)

        class WL : ClassWhitelist {
            override fun hasListed(type: Class<*>) =
                    type.name == "net.corda.nodeapi.internal.serialization.amqp" +
                            ".StaticInitialisationOfSerializedObjectTest\$deserializeTest\$D"
        }

        val sf2 = SerializerFactory(WL(), ClassLoader.getSystemClassLoader())
        val bytes = f.readBytes()

        Assertions.assertThatThrownBy {
            DeserializationInput(sf2).deserialize(SerializedBytes<D>(bytes))
        }.isInstanceOf(NotSerializableException::class.java)
    }

    // Version of a serializer factory that will allow the class carpenter living on the
    // factory to have a different whitelist applied to it than the factory
    class TestSerializerFactory(wl1: ClassWhitelist, wl2: ClassWhitelist) :
            SerializerFactory(wl1, ClassLoader.getSystemClassLoader()) {
        override val classCarpenter = ClassCarpenter(ClassLoader.getSystemClassLoader(), wl2)
    }

    // This time have the serilization factory and the carpenter use different whitelists
    @Test
    fun deserializeTest2() {
        data class D(val c: C2)

        val path = EvolvabilityTests::class.java.getResource("StaticInitialisationOfSerializedObjectTest.deserializeTest2")
        val f = File(path.toURI())

        // Original version of the class for the serialised version of this class
        //
        //val sf1 = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        //val sc = SerializationOutput(sf1).serialize(D(C2(20)))
        //f.writeBytes(sc.bytes)
        //println (path)

        // whitelist to be used by the serialisation factory
        class WL1 : ClassWhitelist {
            override fun hasListed(type: Class<*>) =
                    type.name == "net.corda.nodeapi.internal.serialization.amqp" +
                            ".StaticInitialisationOfSerializedObjectTest\$deserializeTest\$D"
        }

        // whitelist to be used by the carpenter
        class WL2 : ClassWhitelist {
            override fun hasListed(type: Class<*>) = true
        }

        val sf2 = TestSerializerFactory(WL1(), WL2())
        val bytes = f.readBytes()

        // Deserializing should throw because C is not on the whitelist NOT because
        // we ever went anywhere near statically constructing it prior to not actually
        // creating an instance of it
        Assertions.assertThatThrownBy {
            DeserializationInput(sf2).deserialize(SerializedBytes<D>(bytes))
        }.isInstanceOf(NotSerializableException::class.java)
    }
}