package keyedProcessFunctionDemo.demo2

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * 使用处理时间注册
 */
class CountWithTimeoutProcessingTimeFunction extends KeyedProcessFunction[Long, view, pushdata] {

  var lasttimeState: ValueState[Long] = _
  var lasttimeStateDesc: ValueStateDescriptor[Long] = _

  val interval:Long  =  2 * 60 * 1000

  override def open(parameters: Configuration): Unit = {

       super.open(parameters)
       // 记录上一条记录的时间
       lasttimeStateDesc = new ValueStateDescriptor[Long]("lasttimeState", TypeInformation.of(classOf[Long]))
       lasttimeState = getRuntimeContext.getState(lasttimeStateDesc)

  }

  /**
    * 参数1：每个Element元素对象
    * 参数2：为KeyedProcessFunction中定义的上下文对象
    * 参数3：收集器
    */
  override def processElement(value: view, ctx: KeyedProcessFunction[Long, view, pushdata]#Context, out: Collector[pushdata]): Unit = {

     // ctx.timestamp() 当前数据的时间
     // println(ctx.timestamp())
     // println(value)

     if("shop_page".equals(value.page)){

         val state: Long = lasttimeState.value()
         if (state != null){
             lasttimeState.clear()
         }
     }else{

         lasttimeState.update(value.view_time)
         ctx.timerService().registerProcessingTimeTimer(value.view_time + interval)
     }

  }

  /**
    * 每个key都会调用
    * 如果有watermark, 当下一条数据的时间 减去 延迟时间Watermark > 定时器注册的窗口结束时间，该方法就会触发
    * 参数1：触发定时器的时间
    * 参数2：定时器的上下文
    * 参数3：收集器，一般收集定时器触发后的数据
    */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, view, pushdata]#OnTimerContext, out: Collector[pushdata]): Unit = {

    if(timestamp == lasttimeState.value() + interval){

        // 清除状态
        lasttimeState.clear()

        // 返回符合的数据
        out.collect(new pushdata(ctx.getCurrentKey, timestamp))
    }


  }
}