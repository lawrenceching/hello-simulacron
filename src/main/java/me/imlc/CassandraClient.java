package me.imlc;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraClient {

  private static final Logger logger = LoggerFactory.getLogger(CassandraClient.class);
  private CqlSession session = CqlSession.builder().build();

  private static final AtomicInteger total = new AtomicInteger();
  private static final AtomicInteger last = new AtomicInteger();
  private static final AtomicLong lastLatencyInMs = new AtomicLong();
  private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private static boolean started = false;
  private static synchronized void start() {
    if(!started) {
      scheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          int total = CassandraClient.total.get();
          logger.info("Total={}, TPS={}, latency={}ms",
              total, total - last.get(), lastLatencyInMs.get());
          last.set(total);
        }
      }, 1, 1, TimeUnit.SECONDS);
    }
    started = true;
  }

  public void queryRepeatedly() throws InterruptedException {
    start();
    while(true) {
      long now = System.currentTimeMillis();
      ResultSet rs = session.execute("select * from transaction_history");
      lastLatencyInMs.set(System.currentTimeMillis() - now);
      total.incrementAndGet();
    }
  }

}
