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
import org.apache.drill.common.util.RepeatTestRule;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Testshutdown {
  @Test
  public void debugShutDown() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder().clusterSize(3).withLocalZk();
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
      String sq = "SELECT * FROM `cp`.`employee.json` where product_id = 27" ;
      client.queryBuilder().sql(sq).run();

    }
  }


  @Test
  public void fourthTest() throws Exception {

    for (int k = 0; k < 1; k++) {
//      Thread.sleep(1000);
      String[] drillbits = {"db1" , "db2", "db3", "db4", "db5", "db6"};
//      , "db2", "db3", "db4",};

      FixtureBuilder builder = ClusterFixture.builder().withBits(drillbits).withLocalZk();


      try (ClusterFixture cluster = builder.build();
           ClientFixture client = cluster.clientFixture()) {
        for (int i = 0; i < 4000; i++) {
          setupFile(i);
        }
//        client.queryBuilder().sql("SELECT * FROM sys.BOOT").printCsv();

        cluster.defineWorkspace("dfs", "data", "/tmp/drill-test", "psv");
        String sql = "SELECT * FROM dfs.`/tmp/drill-test/` ORDER BY employee_id";
//        client.queryBuilder().sql(sql).run();

        System.out.println("after first query ");
        for (int i = 0; i < 30; i++) {
          QueryBuilder.QuerySummaryFuture listener = client.queryBuilder().sql(sql).futureSummary();
//          Thread.sleep(10000);
        }
        cluster.close_drillbit("db3");
        System.out.println("after shutdown ");
        Thread.sleep(10000);


//        System.out.println("result " + listener.get());
        client.queryBuilder().sql(sql).run();
//        client.queryBuilder().sql(sql).run();
//        client.queryBuilder().sql(sql).run();
//        client.queryBuilder().sql(sql).run();
//        client.queryBuilder().sql(sql).run();
//        client.queryBuilder().sql(sql).run();

        System.out.println("after last query ");

      }

    }
  }


  private void setupFile(int i) {
    File destFile = new File("/tmp/drill-test/example" + i + ".tbl");
    destFile.getParentFile().mkdirs();
    try (PrintWriter out = new PrintWriter(new FileWriter(destFile))) {
      out.println("{\"employee_id\":1,\"full_name\":\"Sheri Nowmer\",\"first_name\":\"Sheri\",\"last_name\":\"Nowmer\",\"position_id\":1,\"position_title\":\"President\",\"store_id\":0,\"department_id\":1,\"birth_date\":\"1961-08-26\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":80000.0000,\"supervisor_id\":0,\"education_level\":\"Graduate Degree\",\"marital_status\":\"S\",\"gender\":\"F\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":2,\"full_name\":\"Derrick Whelply\",\"first_name\":\"Derrick\",\"last_name\":\"Whelply\",\"position_id\":2,\"position_title\":\"VP Country Manager\",\"store_id\":0,\"department_id\":1,\"birth_date\":\"1915-07-03\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":40000.0000,\"supervisor_id\":1,\"education_level\":\"Graduate Degree\",\"marital_status\":\"M\",\"gender\":\"M\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":4,\"full_name\":\"Michael Spence\",\"first_name\":\"Michael\",\"last_name\":\"Spence\",\"position_id\":2,\"position_title\":\"VP Country Manager\",\"store_id\":0,\"department_id\":1,\"birth_date\":\"1969-06-20\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":40000.0000,\"supervisor_id\":1,\"education_level\":\"Graduate Degree\",\"marital_status\":\"S\",\"gender\":\"M\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":5,\"full_name\":\"Maya Gutierrez\",\"first_name\":\"Maya\",\"last_name\":\"Gutierrez\",\"position_id\":2,\"position_title\":\"VP Country Manager\",\"store_id\":0,\"department_id\":1,\"birth_date\":\"1951-05-10\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":35000.0000,\"supervisor_id\":1,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"M\",\"gender\":\"F\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":6,\"full_name\":\"Roberta Damstra\",\"first_name\":\"Roberta\",\"last_name\":\"Damstra\",\"position_id\":3,\"position_title\":\"VP Information Systems\",\"store_id\":0,\"department_id\":2,\"birth_date\":\"1942-10-08\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":25000.0000,\"supervisor_id\":1,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"M\",\"gender\":\"F\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":7,\"full_name\":\"Rebecca Kanagaki\",\"first_name\":\"Rebecca\",\"last_name\":\"Kanagaki\",\"position_id\":4,\"position_title\":\"VP Human Resources\",\"store_id\":0,\"department_id\":3,\"birth_date\":\"1949-03-27\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":15000.0000,\"supervisor_id\":1,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"M\",\"gender\":\"F\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":8,\"full_name\":\"Kim Brunner\",\"first_name\":\"Kim\",\"last_name\":\"Brunner\",\"position_id\":11,\"position_title\":\"Store Manager\",\"store_id\":9,\"department_id\":11,\"birth_date\":\"1922-08-10\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":10000.0000,\"supervisor_id\":5,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"S\",\"gender\":\"F\",\"management_role\":\"Store Management\"}\n" +
              "{\"employee_id\":9,\"full_name\":\"Brenda Blumberg\",\"first_name\":\"Brenda\",\"last_name\":\"Blumberg\",\"position_id\":11,\"position_title\":\"Store Manager\",\"store_id\":21,\"department_id\":11,\"birth_date\":\"1979-06-23\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":17000.0000,\"supervisor_id\":5,\"education_level\":\"Graduate Degree\",\"marital_status\":\"M\",\"gender\":\"F\",\"management_role\":\"Store Management\"}\n" +
              "{\"employee_id\":10,\"full_name\":\"Darren Stanz\",\"first_name\":\"Darren\",\"last_name\":\"Stanz\",\"position_id\":5,\"position_title\":\"VP Finance\",\"store_id\":0,\"department_id\":5,\"birth_date\":\"1949-08-26\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":50000.0000,\"supervisor_id\":1,\"education_level\":\"Partial College\",\"marital_status\":\"M\",\"gender\":\"M\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":11,\"full_name\":\"Jonathan Murraiin\",\"first_name\":\"Jonathan\",\"last_name\":\"Murraiin\",\"position_id\":11,\"position_title\":\"Store Manager\",\"store_id\":1,\"department_id\":11,\"birth_date\":\"1967-06-20\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":15000.0000,\"supervisor_id\":5,\"education_level\":\"Graduate Degree\",\"marital_status\":\"S\",\"gender\":\"M\",\"management_role\":\"Store Management\"}\n" +
              "{\"employee_id\":12,\"full_name\":\"Jewel Creek\",\"first_name\":\"Jewel\",\"last_name\":\"Creek\",\"position_id\":11,\"position_title\":\"Store Manager\",\"store_id\":5,\"department_id\":11,\"birth_date\":\"1971-10-18\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":8500.0000,\"supervisor_id\":5,\"education_level\":\"Graduate Degree\",\"marital_status\":\"S\",\"gender\":\"F\",\"management_role\":\"Store Management\"}\n" +
              "{\"employee_id\":13,\"full_name\":\"Peggy Medina\",\"first_name\":\"Peggy\",\"last_name\":\"Medina\",\"position_id\":11,\"position_title\":\"Store Manager\",\"store_id\":10,\"department_id\":11,\"birth_date\":\"1975-10-12\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":15000.0000,\"supervisor_id\":5,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"S\",\"gender\":\"F\",\"management_role\":\"Store Management\"}\n" +
              "{\"employee_id\":14,\"full_name\":\"Bryan Rutledge\",\"first_name\":\"Bryan\",\"last_name\":\"Rutledge\",\"position_id\":11,\"position_title\":\"Store Manager\",\"store_id\":8,\"department_id\":11,\"birth_date\":\"1912-07-09\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":17000.0000,\"supervisor_id\":5,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"M\",\"gender\":\"M\",\"management_role\":\"Store Management\"}\n" +
              "{\"employee_id\":15,\"full_name\":\"Walter Cavestany\",\"first_name\":\"Walter\",\"last_name\":\"Cavestany\",\"position_id\":11,\"position_title\":\"Store Manager\",\"store_id\":4,\"department_id\":11,\"birth_date\":\"1941-11-05\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":12000.0000,\"supervisor_id\":5,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"M\",\"gender\":\"M\",\"management_role\":\"Store Management\"}\n" +
              "{\"employee_id\":16,\"full_name\":\"Peggy Planck\",\"first_name\":\"Peggy\",\"last_name\":\"Planck\",\"position_id\":11,\"position_title\":\"Store Manager\",\"store_id\":12,\"department_id\":11,\"birth_date\":\"1919-06-02\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":17000.0000,\"supervisor_id\":5,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"S\",\"gender\":\"F\",\"management_role\":\"Store Management\"}");
//      out.println("10|abc");
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}





