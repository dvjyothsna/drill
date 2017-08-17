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

package org.apache.drill.exec.server;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint.State;


public class DrillbitStateManager {


  public DrillbitStateManager(DrillbitState currentState) {
    this.currentState = currentState;
  }

  public enum DrillbitState {
    STARTUP, GRACE, DRAINING, OFFLINE, SHUTDOWN
  }

  public DrillbitState getState() {
    return currentState;
  }

  private DrillbitState currentState;
  public void setState(DrillbitState state) {
    switch (state) {
      case GRACE:
        if (currentState == DrillbitState.STARTUP) {
          currentState = state;
        } else {
          throw new IllegalStateException("Cannot set drillbit to" + state + "from" + currentState);
        }
        break;
      case DRAINING:
        if (currentState == DrillbitState.GRACE) {
          currentState = state;
        } else {
          throw new IllegalStateException("Cannot set drillbit to" + state + "from" + currentState);
        }
        break;
      case OFFLINE:
        if (currentState == DrillbitState.DRAINING) {
          currentState = state;
        } else {
          throw new IllegalStateException("Cannot set drillbit to" + state + "from" + currentState);
        }
        break;
      case SHUTDOWN:
        if (currentState == DrillbitState.OFFLINE) {
          currentState = state;
        } else {
          throw new IllegalStateException("Cannot set drillbit to" + state + "from" + currentState);
        }
        break;
    }
  }

}

