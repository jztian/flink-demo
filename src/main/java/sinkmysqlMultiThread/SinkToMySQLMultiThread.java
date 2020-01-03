package sinkmysqlMultiThread;

import org.apache.flink.calcite.shaded.com.google.common.collect.Queues;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.concurrent.*;

/**
 * 多线程Sink
 */
public class SinkToMySQLMultiThread extends RichSinkFunction<Student> implements CheckpointedFunction {

    private Logger LOG = LoggerFactory.getLogger(SinkToMySQLMultiThread.class);

    // Client 线程的默认数量
    private final int DEFAULT_CLIENT_THREAD_NUM = 5;
    // 数据缓冲队列的默认容量
    private final int DEFAULT_QUEUE_CAPACITY = 5000;

    private LinkedBlockingQueue<Student> bufferQueue;
    private CyclicBarrier clientBarrier;


    private Connection connection = null;
    private PreparedStatement ps = null;

    /**
     * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        // new 一个容量为 DEFAULT_CLIENT_THREAD_NUM 的线程池
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(DEFAULT_CLIENT_THREAD_NUM, DEFAULT_CLIENT_THREAD_NUM,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        // new 一个容量为 DEFAULT_QUEUE_CAPACITY 的数据缓冲队列
        this.bufferQueue = Queues.newLinkedBlockingQueue(DEFAULT_QUEUE_CAPACITY);

        // barrier 需要拦截 (DEFAULT_CLIENT_THREAD_NUM + 1) 个线程
        this.clientBarrier = new CyclicBarrier(DEFAULT_CLIENT_THREAD_NUM + 1);

        // 创建并开启消费者线程
        MultiThreadConsumerClient consumerClient = new MultiThreadConsumerClient(bufferQueue, clientBarrier);
        for (int i=0; i < DEFAULT_CLIENT_THREAD_NUM; i++) {
            threadPoolExecutor.execute(consumerClient);
        }

    }


    @Override
    public void invoke(Student value, Context context) throws Exception {
        // 往 bufferQueue 的队尾添加数据
        bufferQueue.put(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        LOG.info("snapshotState : 所有的 client 准备 flush !!!");
        // barrier 开始等待
        clientBarrier.await();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

}
