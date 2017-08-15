/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.test;

import ch.qos.logback.classic.Level;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.QueryTestUtil;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKRegistrationHandle;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class Testshutdown {

  public static final Properties WEBSERVER_CONFIGURATION = new Properties() {
    {
      put(ExecConstants.HTTP_ENABLE, true);
    }
  };

  public FixtureBuilder enableWebServer(FixtureBuilder builder) {
    Properties props = new Properties();
    props.putAll(WEBSERVER_CONFIGURATION);
    builder.configBuilder.configProps(props);
    return builder;
  }


  @Test
  public void testOnlineEndPoints() throws  Exception {

    String[] drillbits = {"db1" ,"db2","db3", "db4", "db5", "db6"};
    FixtureBuilder builder = ClusterFixture.builder().withBits(drillbits).withLocalZk();


    try ( ClusterFixture cluster = builder.build();
          ClientFixture client = cluster.clientFixture()) {

      Drillbit drillbit = cluster.drillbit("db2");
        DrillbitEndpoint drillbitEndpoint =  drillbit.getRegistrationHandle().getEndPoint();
      new Thread(new Runnable() {
        public void run() {
          try {
            cluster.close_drillbit("db2");
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }).start();
      Thread.sleep(50);
      Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit().getContext()
              .getClusterCoordinator()
              .getOnlineEndPoints();
      Assert.assertFalse(drillbitEndpoints.contains(drillbitEndpoint));
    }
  }
  @Test
  public void testStateChange() throws  Exception {

    String[] drillbits = {"db1" ,"db2", "db3", "db4", "db5", "db6"};
    FixtureBuilder builder = ClusterFixture.builder().withBits(drillbits).withLocalZk();
    try ( ClusterFixture cluster = builder.build();
          ClientFixture client = cluster.clientFixture()) {
      Drillbit drillbit = cluster.drillbit("db2");
      DrillbitEndpoint drillbitEndpoint =  drillbit.getRegistrationHandle().getEndPoint();
      new Thread(new Runnable() {
        public void run() {
          try {
            cluster.close_drillbit("db2");
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }).start();
      Thread.sleep(50);
      Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit().getContext()
              .getClusterCoordinator()
              .getAvailableEndpoints();
      for (DrillbitEndpoint dbEndpoint : drillbitEndpoints) {
        if(drillbitEndpoint.getAddress().equals(dbEndpoint.getAddress()) && drillbitEndpoint.getUserPort() == dbEndpoint.getUserPort()) {
          assertNotEquals(dbEndpoint.getState(),DrillbitEndpoint.State.ONLINE);
        }
      }
    }
  }

  @Test
  public void testRestApi() throws Exception {

    FixtureBuilder builder = ClusterFixture.bareBuilder().clusterSize(6).withLocalZk();
    builder = enableWebServer(builder);
    int i = 8047;
    final String sql = "SELECT * FROM dfs.`/tmp/drill-test/` ORDER BY employee_id";
    try ( ClusterFixture cluster = builder.build();
          final ClientFixture client = cluster.clientFixture()) {
      Thread.sleep(1500);
      new Thread(new Runnable() {
        public void run() {
          try {
            final QueryBuilder.QuerySummary querySummary = client.queryBuilder().sql(sql).run();
            Assert.assertEquals(querySummary.finalState(), QueryState.COMPLETED);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }).start();

      while( i < 8052) {

        URL url = new URL("http://localhost:"+i+"/shutdown");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        System.out.println("port " + i);
        if (conn.getResponseCode() != 200) {
          throw new RuntimeException("Failed : HTTP error code : "
                  + conn.getResponseCode());
        }
        i++;
      }
      Thread.sleep(25000);
      Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit().getContext()
              .getClusterCoordinator()
              .getOnlineEndPoints();
      Assert.assertEquals(drillbitEndpoints.size(), 1);
    }
  }

  @Test
  public void testShutdown() throws Exception {


    String[] drillbits = {"db1" ,"db2", "db3", "db4", "db5", "db6"};
    FixtureBuilder builder = ClusterFixture.bareBuilder().withBits(drillbits).withLocalZk();
    final String sql = "SELECT * FROM dfs.`/tmp/drill-test/` ORDER BY employee_id";

    try (ClusterFixture cluster = builder.build();
         final ClientFixture client = cluster.clientFixture()) {

      cluster.defineWorkspace("dfs", "data", "/tmp/drill-test", "psv");

      new Thread(new Runnable() {
        public void run() {
          try {
            final QueryBuilder.QuerySummary querySummary = client.queryBuilder().sql(sql).run();
            Assert.assertEquals(querySummary.finalState(), QueryState.COMPLETED);
            System.out.println("after assert " + System.currentTimeMillis());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }).start();
      Thread.sleep(1000);
      shutdown(cluster,"db2");
      shutdown(cluster,"db3");
      shutdown(cluster,"db4");
      shutdown(cluster,"db5");
      shutdown(cluster,"db1");
      Thread.sleep(200);
      System.out.println(cluster.drillbit().getContext().getClusterCoordinator().getAvailableEndpoints());
      Thread.sleep(10000);
      System.out.println("in end" + System.currentTimeMillis());
    }
  }

  @Test
  public void fourthTest() throws Exception {

    for (int k = 0; k < 1; k++) {
      String[] drillbits = {"db1" ,"db2", "db3", "db4", "db5", "db6"};


      LogFixture.LogFixtureBuilder logBuilder = LogFixture.builder()
              // Log to the console for debugging convenience
              .toConsole()
              .logger("org.apache.drill.exec.server", Level.TRACE)
//              .logger("org.apache.drill.exec.work.foreman",Level.TRACE)
              ;

      FixtureBuilder builder = ClusterFixture.bareBuilder().withBits(drillbits).withLocalZk();
      try (LogFixture logs = logBuilder.build();
           ClusterFixture cluster = builder.build();
           ClientFixture client = cluster.clientFixture()) {
        Thread.sleep(10);
        cluster.defineWorkspace("dfs", "data", "/tmp/drill-test", "psv");
        String sql = "SELECT * FROM dfs.`/tmp/drill-test/` ORDER BY employee_id";
        QueryBuilder.QuerySummaryFuture[] listener = new QueryBuilder.QuerySummaryFuture[20];
        System.out.println("test thread" +Thread.currentThread());
        System.out.println("after first query ");
        for (int i = 0; i < 20; i++) {
          listener[i] = client.queryBuilder().sql(sql).futureSummary();
        }
        System.out.println("after last query ");
        Thread.sleep(15000);
      }
    }
  }

  public void shutdown(final ClusterFixture cluster, final String db) {
    new Thread(new Runnable() {
      public void run() {
        try {
          Thread.currentThread().setName( db);
          cluster.close_drillbit(db);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }).start();
  }


  @Test
  public void test() {
    String[] drillbits = {"db1", "db2", "db3", "db4", "db5", "db6"};
    FixtureBuilder builder = ClusterFixture.bareBuilder().withBits(drillbits).withLocalZk();
    builder = enableWebServer(builder);
    final String sql = "SELECT * FROM dfs.`/tmp/drill-test/` ORDER BY employee_id";

    try (ClusterFixture cluster = builder.build();
         final ClientFixture client = cluster.clientFixture()) {

      cluster.defineWorkspace("dfs", "data", "/tmp/drill-test", "psv");
      client.queryBuilder().sql(sql).printCsv();
      Thread.sleep(150);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
