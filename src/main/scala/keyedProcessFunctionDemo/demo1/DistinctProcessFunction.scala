package keyedProcessFunctionDemo.demo1

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector


class DistinctProcessFunction extends KeyedProcessFunction[AdKey, AdData, AdCount] {
  var devIdState: MapState[String, Int] = _
  var devIdStateDesc: MapStateDescriptor[String, Int] = _

  var countState: ValueState[Long] = _
  var countStateDesc: ValueStateDescriptor[Long] = _

  override def open(parameters: Configuration): Unit = {

    devIdStateDesc = new MapStateDescriptor[String, Int]("devIdState", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[Int]))
    devIdState = getRuntimeContext.getMapState(devIdStateDesc)

    countStateDesc = new ValueStateDescriptor[Long]("countState", TypeInformation.of(classOf[Long]))
    countState = getRuntimeContext.getState(countStateDesc)
  }

  /**
   *  每来一条数据都会调用
    * 参数1：每个Element元素对象
    * 参数2：为KeyedProcessFunction中定义的上下文对象
    * 参数3：收集器
    */
  override def processElement(value: AdData, ctx: KeyedProcessFunction[AdKey, AdData, AdCount]#Context, out: Collector[AdCount]): Unit = {

    // 迟到的数据丢弃 (数据所在窗口的结束时间+1 < 当前的watermark)
    val currW = ctx.timerService().currentWatermark()
    if(ctx.getCurrentKey.time + 1 <= currW) {
      println("late data:" + value)
      return
    }

    val devId = value.devId
    devIdState.get(devId) match {
      case 1 => {
        //表示已经存在
      }
      case _ => {
        //表示不存在
        devIdState.put(devId, 1)
        val c = countState.value()
        countState.update(c + 1)

        // 根据需求,看是需要实时覆盖的话，collect写到这里
        //out.collect(new AdCount(ctx.getCurrentKey.time, ctx.getCurrentKey.id, countState.value()))

        //还需要注册一个定时器
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey.time + 1)
      }
    }

  }

  /**
    * 每个key都会调用
    * 当下一条数据的时间 减去 延迟时间Watermark > 定时器注册的窗口结束时间，该方法就会触发
    * 参数1：触发定时器的时间
    * 参数2：定时器的上下文
    * 参数3：收集器，一般收集定时器触发后的数据
    */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[AdKey, AdData, AdCount]#OnTimerContext, out: Collector[AdCount]): Unit = {

    println(timestamp + " exec clean~~~")
    devIdState.clear()
    countState.clear()

    // 根据需求,一小时输出一次，collect写到这里
    out.collect(new AdCount(ctx.getCurrentKey.time, ctx.getCurrentKey.id, countState.value()))

  }
}