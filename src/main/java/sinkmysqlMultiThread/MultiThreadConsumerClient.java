package sinkmysqlMultiThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 多线程
 * 每5条批量写一次数据库
 */
public class MultiThreadConsumerClient implements Runnable {

    private Logger LOG = LoggerFactory.getLogger(MultiThreadConsumerClient.class);

    private LinkedBlockingQueue<Student> bufferQueue;
    private CyclicBarrier barrier;

    private Connection connection = null;
    private PreparedStatement ps = null;

    public MultiThreadConsumerClient(
            LinkedBlockingQueue<Student> bufferQueue, CyclicBarrier barrier) {
        this.bufferQueue = bufferQueue;
        this.barrier = barrier;
    }

    @Override
    public void run() {

        try {
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://192.168.2.101:3306/test";
            String username = "root";
            String password = "root";
            //1.加载驱动
            Class.forName(driver);
            //2.创建连接
            connection = DriverManager.getConnection(url, username, password);
            String sql = "insert into Student(id,name,password,age)values(?,?,?,?);";
            //3.获得执行语句
            ps = connection.prepareStatement(sql);

            int batchSize = 0;

            Student entity;
            while (true){

                    // 从 bufferQueue 的队首消费数据，并设置 timeout
                    entity = bufferQueue.poll(50, TimeUnit.MILLISECONDS);
                    // entity != null 表示 bufferQueue 有数据
                    if(entity != null){

                        System.out.println(Thread.currentThread().getName());
                        System.out.println(batchSize);

                        // 执行 client 消费数据的逻辑
                        doSomething(entity);

                        batchSize ++ ;

                        if(batchSize > 5) {
                            ps.executeBatch();
                            batchSize = 0;
                        }


                    } else {
                        // entity == null 表示 bufferQueue 中已经没有数据了，
                        // 且 barrier wait 大于 0 表示当前正在执行 Checkpoint，
                        // client 需要执行 flush，保证 Checkpoint 之前的数据都消费完成
                        //System.out.println(barrier.getNumberWaiting());
                        if ( barrier.getNumberWaiting() > 0 ) {
                            LOG.info("MultiThreadConsumerClient 执行 flush, " +  "当前 wait 的线程数：" + barrier.getNumberWaiting());

                            // client 执行 flush 操作，防止丢数据
                            ps.executeBatch();
                            batchSize = 0;

                            barrier.await();
                        }
                    }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // client 消费数据的逻辑
    private void doSomething(Student entity) throws Exception{

            //4.批量插入
            ps.setInt(1, entity.getId());
            ps.setString(2, entity.getName());
            ps.setString(3, entity.getPassword());
            ps.setInt(4, entity.getAge());
            ps.addBatch();

    }

}
