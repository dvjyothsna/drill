/**
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
package org.apache.drill.exec.server.rest;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.annotation.JsonCreator;

@Path("/")
@PermitAll
public class DrillRoot {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRoot.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;
  @Inject Drillbit drillbit;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getClusterInfo() {
    System.out.println(" in here ***************");
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/index.ftl", sc, getClusterInfoJSON());
  }


  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, String> getDrillbitStatus(){
    Collection<DrillbitInfo> drillbits = getClusterInfoJSON().getDrillbits();
    Map<String, String> drillStatusMap = new HashMap<String ,String>();
    for (DrillbitInfo drillbit : drillbits) {
      drillStatusMap.put(drillbit.getAddress()+"-"+drillbit.getUserPort(),drillbit.getStatus());
    }
    return drillStatusMap;
  }

  @GET
  @Path("/graceperiod")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Integer> getGracePeriod(){

    final DrillConfig config = work.getContext().getConfig();
    final int gracePeriod = config.getInt(ExecConstants.GRACE_PERIOD);
    Map<String, Integer> gracePeriodMap = new HashMap<String, Integer>();
    gracePeriodMap.put("graceperiod",gracePeriod);
    return gracePeriodMap;
  }

  @POST
  @Path("/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  public String shutdownDrillbit() throws Exception {
    System.out.println("in shutdown get");
    new Thread(new Runnable() {
      public void run() {
        try {
         drillbit.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }).start();
    return "Shutdown request is triggered";
  }

  @GET
  @Path("/cluster.json")
  @Produces(MediaType.APPLICATION_JSON)
  public ClusterInfo getClusterInfoJSON() {
    final Collection<DrillbitInfo> drillbits = Sets.newTreeSet();
    final Collection<String> mismatchedVersions = Sets.newTreeSet();

    final DrillbitEndpoint currentDrillbit = work.getContext().getEndpoint();
    final String currentVersion = currentDrillbit.getVersion();

    final DrillConfig config = work.getContext().getConfig();
    final boolean userEncryptionEnabled = config.getBoolean(ExecConstants.USER_ENCRYPTION_SASL_ENABLED);
    final boolean bitEncryptionEnabled = config.getBoolean(ExecConstants.BIT_ENCRYPTION_SASL_ENABLED);


    for (DrillbitEndpoint endpoint : work.getContext().getBits()) {
      final DrillbitInfo drillbit = new DrillbitInfo(endpoint,
              currentDrillbit.equals(endpoint),
              currentVersion.equals(endpoint.getVersion()));
      if (!drillbit.isVersionMatch()) {
        mismatchedVersions.add(drillbit.getVersion());
      }
      drillbits.add(drillbit);
    }

    return new ClusterInfo(drillbits, currentVersion, mismatchedVersions,
      userEncryptionEnabled, bitEncryptionEnabled);
  }

  @XmlRootElement
  public static class ClusterInfo {
    private final Collection<DrillbitInfo> drillbits;
    private final String currentVersion;
    private final Collection<String> mismatchedVersions;
    private final boolean userEncryptionEnabled;
    private final boolean bitEncryptionEnabled;

    @JsonCreator
    public ClusterInfo(Collection<DrillbitInfo> drillbits,
                       String currentVersion,
                       Collection<String> mismatchedVersions,
                       boolean userEncryption,
                       boolean bitEncryption ) {
      this.drillbits = Sets.newTreeSet(drillbits);
      this.currentVersion = currentVersion;
      this.mismatchedVersions = Sets.newTreeSet(mismatchedVersions);
      this.userEncryptionEnabled = userEncryption;
      this.bitEncryptionEnabled = bitEncryption;
    }

    public Collection<DrillbitInfo> getDrillbits() {
      return Sets.newTreeSet(drillbits);
    }

    public String getCurrentVersion() {
      return currentVersion;
    }

    public Collection<String> getMismatchedVersions() {
      return Sets.newTreeSet(mismatchedVersions);
    }

    public boolean isUserEncryptionEnabled() { return userEncryptionEnabled; }

    public boolean isBitEncryptionEnabled() { return bitEncryptionEnabled; }

  }

  public static class DrillbitInfo implements Comparable<DrillbitInfo> {
    private final String address;
    private final String userPort;
    private final String controlPort;
    private final String dataPort;
    private final String version;
    private final boolean current;
    private final boolean versionMatch;


    private final String status;

    @JsonCreator
    public DrillbitInfo(DrillbitEndpoint drillbit, boolean current, boolean versionMatch) {
      this.address = drillbit.getAddress();
      this.userPort = String.valueOf(drillbit.getUserPort());
      this.controlPort = String.valueOf(drillbit.getControlPort());
      this.dataPort = String.valueOf(drillbit.getDataPort());
      this.version = Strings.isNullOrEmpty(drillbit.getVersion()) ? "Undefined" : drillbit.getVersion();
      this.current = current;
      this.versionMatch = versionMatch;
      this.status = String.valueOf(drillbit.getState());
    }

    public String getAddress() {
      return address;
    }

    public String getUserPort() { return userPort; }

    public String getControlPort() { return controlPort; }

    public String getDataPort() { return dataPort; }

    public String getVersion() {
      return version;
    }

    public boolean isCurrent() {
      return current;
    }

    public boolean isVersionMatch() {
      return versionMatch;
    }

    public String getStatus() {
      return status;
    }


    /**
     * Method used to sort drillbits. Current drillbit goes first.
     * Then drillbits with matching versions, after them drillbits with mismatching versions.
     * Matching drillbits are sorted according address natural order,
     * mismatching drillbits are sorted according version, address natural order.
     *
     * @param drillbitToCompare drillbit to compare against
     * @return -1 if drillbit should be before, 1 if after in list
     */
    @Override
    public int compareTo(DrillbitInfo drillbitToCompare) {
      if (this.isCurrent()) {
        return -1;
      }

      if (drillbitToCompare.isCurrent()) {
        return 1;
      }

      if (this.isVersionMatch() == drillbitToCompare.isVersionMatch()) {
        if (this.version.equals(drillbitToCompare.getVersion())) {
          {
            if(this.address.equals(drillbitToCompare.getAddress())) {
              return (this.controlPort.compareTo(drillbitToCompare.getControlPort()));
            }
            return (this.address.compareTo(drillbitToCompare.getAddress()));
          }
//          this.controlPort.compareTo(drillbitToCompare.getControlPort()));
        }
        return this.version.compareTo(drillbitToCompare.getVersion());
      }
      return this.versionMatch ? -1 : 1;
    }
  }

}
