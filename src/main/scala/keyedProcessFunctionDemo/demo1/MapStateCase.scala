package keyedProcessFunctionDemo.demo1

import java.util.Properties
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * 计算每个广告每小时的点击用户数，广告点击日志包含：广告位ID、用户设备ID、点击时间。
  *
  * 数据
  * 411,1,1578269460000    08:11:00
  * 411,2,1578272400000    09:00:00
  * 411,3,1578272460000    09:01:00
  * 411,2,1578278940000    10.49
  * 412,3,1578282540000    11.49
  * 412,4,1578282900000    11.55
  * 411,6,1578282920000    11.55:20
  * 416,5,1578283080000    11.58
  */
object MapStateCase {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test-group")

    val source: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("test",new SimpleStringSchema(), properties))

    val data = source
                  .map(x=>{
                    val s = x.split(",")
                    AdData(s(0).toInt,s(1),s(2).toLong)
                  })
                  // 允许延迟1分钟
                  .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdData](Time.minutes(1)) {
                    override def extractTimestamp(element: AdData): Long = element.time
                  })
                  .keyBy(x=>{
                    // 以该时间窗口的结束时间
                    // 以一小时为一批次，以结束时间为准
                    val endTime = TimeWindow.getWindowStartWithOffset(x.time, 0, Time.hours(1).toMilliseconds) + Time.hours(1).toMilliseconds
                    AdKey(x.id,endTime)
                  })
                  .process(new DistinctProcessFunction())

    data.print()

    env.execute()

  }
}


case class AdData(id:Int,devId:String,time:Long)

case class AdKey(id:Int,time:Long)

case class AdCount(date: Long, id:Int, count:Long)