package me.imlc;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;

import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import io.netty.util.HashedWheelTimer;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class Simulacron {

  public void start() {
    Server server = Server.builder()
        .withTimer(new HashedWheelTimer(100, TimeUnit.NANOSECONDS ))
        .build();

    ClusterSpec clusterSpec = ClusterSpec.builder().build();

    DataCenterSpec dataCenterSpec = clusterSpec.addDataCenter().build();

    NodeSpec node0 = dataCenterSpec.addNode().withAddress(new InetSocketAddress("127.0.0.1", 9042))
        .build();

    BoundCluster boundCluster = server.register(clusterSpec);

    boundCluster.prime(
        when("select * from transaction_history")
        .then(
            rows()
            .row("id", 12345678L)
            .columnTypes("id", "bigint")
        )
        .delay(3, TimeUnit.MILLISECONDS)
    );
  }

}
