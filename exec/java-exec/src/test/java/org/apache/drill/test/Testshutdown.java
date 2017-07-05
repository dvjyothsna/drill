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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class Testshutdown {
  @Test
  public void debugShutDown() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder().clusterSize(3);
//            .withLocalZk();
//    builder.clusterSize(3);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      System.out.println(cluster.config().getString(ExecConstants.ZK_CONNECTION));
//              configResource(ExecConstants.ZK_CONNECTION));
      final ClusterCoordinator coord;
      final String connect = cluster.config().getString(ExecConstants.ZK_CONNECTION);
//      coord = new ZKClusterCoordinator(cluster.config(),connect);

//      coord.start(10000);

//      System.out.println(coord.getAvailableEndpoints());
      DrillbitStatusListener drillbitStatusListener = new DrillbitStatusListener() {
        @Override
        public void drillbitUnregistered(Set<CoordinationProtos.DrillbitEndpoint> unregisteredDrillbits) {
          System.out.println("unregistered successfully");
        }

        @Override
        public void drillbitRegistered(Set<CoordinationProtos.DrillbitEndpoint> registeredDrillbits) {
          System.out.println("registered successfully");
        }
      };
//      coord.addDrillbitStatusListener(drillbitStatusListener);
//      client.queryBuilder().sql(sql).printCsv();


//      cluster.close_drillbit();

//      String sql = "SELECT id_i,name_s20 FROM `mock`.`employees_10000k` ORDER BY id_i";
      String sq = "SELECT * FROM `cp` where product_id = 27" ;
      client.queryBuilder().sql(sq).run();

    }
  }

}
