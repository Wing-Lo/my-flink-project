package scala.myflink

import java.sql.Timestamp
import java.util.Comparator

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object HotItems{

  val INPUT_FILE_NAME = "UserBehavior.csv"
  val dir = HotItems.getClass.getClassLoader.getResource("")
  val fullPath = dir+"/"+INPUT_FILE_NAME;

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputData = env.readTextFile(fullPath)
    inputData.map { (str: String) =>
      val splited = str.split(",")
      (splited(0).toLong, splited(1).toLong, splited(2).toInt, splited(3), splited(4).toLong)
    }

      .filter(_._4.equals("pv"))
      .map(x => (x._2, x._5, 1))
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, Long, Int)](){
        private val serialVersionUID = -742759155861320823L

        private var currentTimestamp = Long.MinValue

        override def getCurrentWatermark: Watermark = new Watermark(
          if (currentTimestamp == Long.MinValue) Long.MinValue
          else currentTimestamp - 1
        )

        override def extractTimestamp(t: (Long, Long, Int), l: Long): Long = t._2*1000
      })
      // itemId, timestamp, count
      .keyBy(0)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResultFunction)
      // itemId, w.getEnd, count
      .keyBy(_._2)
      .process(new TopNHotItems(3))
      .print()

    env.execute("Hot Items")
  }

  /** COUNT 统计的聚合函数实现，每出现一条记录加一 */
  class CountAgg extends AggregateFunction[(Long, Long, Int), Long, Long] {
    override def createAccumulator = 0L

    override def add(userBehavior: (Long, Long, Int), acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  /** 用于输出窗口的结果 */
  class WindowResultFunction extends WindowFunction[Long, (Long, Long, Long), Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[(Long, Long, Long)]): Unit = {
      val itemId: Long = key.getField(0)
      val count = input.iterator.next
      out.collect((itemId, window.getEnd, count))
    }
  }

  class TopNHotItems(topSize: Int) extends  KeyedProcessFunction[Long, (Long, Long, Long), String]{

    private var itemState: ListState[(Long, Long, Long)] = null

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      itemState = getRuntimeContext.getListState(new ListStateDescriptor[(Long, Long, Long)]("itemState-state", classOf[(Long, Long, Long)]))
    }


    override def processElement(i: (Long, Long, Long), context: KeyedProcessFunction[Long, (Long, Long, Long), String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i);
      context.timerService().registerEventTimeTimer(i._2+1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, (Long, Long, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)
      var allItems = new ArrayBuffer[(Long, Long, Long)]
      for (item <- itemState.get().iterator()){
        allItems.append(item)
      }

      itemState.clear()
      allItems.sort(new Comparator[(Long, Long, Long)](){
        override def compare(o1: (Long, Long, Long), o2: (Long, Long, Long)): Int = o2._3.toInt-o1._3.toInt
      })

      // 将排名信息格式化成 String, 便于打印
      val result = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      var i = 0
      while ( {
        i < allItems.size && i < topSize
      }) {
        val currentItem = allItems.get(i)
        // No1:  商品ID=12224  浏览量=2413
        result.append("No").append(i).append(":").append("  商品ID=")
          .append(currentItem._1).append("  浏览量=")
          .append(currentItem._3).append("\n")

        {
          i += 1; i - 1
        }
      }
      result.append("====================================\n\n")

      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)

      out.collect(result.toString)
    }

  }

}