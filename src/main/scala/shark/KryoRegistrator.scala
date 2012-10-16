package shark

import java.util.Arrays
import java.nio.ByteBuffer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import de.javakaffee.kryoserializers.ArraysAsListSerializer
import shark.execution.MapJoinOperator
import org.apache.hadoop.hive.ql.exec.persistence.{MapJoinSingleKey, MapJoinObjectKey, MapJoinDoubleKeys, MapJoinObjectValue}


class KryoRegistrator extends spark.KryoRegistrator {
  def registerClasses(kryo: Kryo) {

    kryo.register(classOf[execution.ReduceKey])

    // Java Arrays.asList returns an internal class that cannot be serialized
    // by default Kryo. This provides a workaround.
    kryo.register(Arrays.asList().getClass, new ArraysAsListSerializer)

    kryo.register(classOf[MapJoinSingleKey], new KryoJavaSerializer)
    kryo.register(classOf[MapJoinObjectKey], new KryoJavaSerializer)
    kryo.register(classOf[MapJoinDoubleKeys], new KryoJavaSerializer)
    kryo.register(classOf[MapJoinObjectValue], new KryoJavaSerializer)
  }
}
