/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.dac.cmd;

import com.dremio.attach.DremioAgent;
import com.google.common.annotations.VisibleForTesting;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

/**
 * Attach Tool
 */
public class DremioAttach {
  private static String DREMIO_PROCESS_NAME = "DremioDaemon";


  public static void main(String[] args) throws Exception {
    // find DremioDaemon Process
    String vmid = null;
    VirtualMachine vm = null;
    int dremioCount = 0;
    for (VirtualMachineDescriptor descriptor : VirtualMachine.list()) {
      if (isDremioDescriptor(descriptor)) {
        vmid = descriptor.id();
        dremioCount++;
      }
    }

    if (dremioCount != 1) {
      throw new UnsupportedOperationException("Failed to attach: couldn't find Dremio process to attach");
    }

    try {
      vm = VirtualMachine.attach(vmid);
      vm.loadAgent(DremioAgent.class.getProtectionDomain().getCodeSource().getLocation().getPath(), String.join("\t", args));
    } finally {
      if (vm != null) {
        vm.detach();
      }
    }
  }

  public static boolean isOffline() {
    int dremioCount = 0;
    for (VirtualMachineDescriptor descriptor : VirtualMachine.list()) {
      if (isDremioDescriptor(descriptor)) {
        dremioCount++;
      }
    }
    return dremioCount != 1;
  }

  private static boolean isDremioDescriptor(VirtualMachineDescriptor descriptor) {
    return descriptor.toString().contains(DREMIO_PROCESS_NAME);
  }

  @VisibleForTesting
  static void setDremioProcessName(String processName) {
    DREMIO_PROCESS_NAME = processName;
  }
}
