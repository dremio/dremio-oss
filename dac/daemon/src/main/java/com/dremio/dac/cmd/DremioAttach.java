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
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

/**
 * Attach Tool
 */
public class DremioAttach {
  private static String DREMIO_PROCESS_NAME = "DremioDaemon";

  public static void main(String[] args) throws Exception {
    // find DremioDaemon Process
    String vmid = getVmIdForProcess(DREMIO_PROCESS_NAME);

    main(vmid, args);
  }

  public static void main(String vmid, String[] args) throws Exception {
    VirtualMachine vm = null;
    try {
      vm = VirtualMachine.attach(vmid);
      vm.loadAgent(DremioAgent.class.getProtectionDomain().getCodeSource().getLocation().getPath(), String.join("\t", args));
    } finally {
      if (vm != null) {
        vm.detach();
      }
    }
  }

  static String getVmIdForProcess(String dremioProcess) throws Exception {
    int dremioCount = 0;
    String vmid = null;
    for (VirtualMachineDescriptor descriptor : VirtualMachine.list()) {
      if (descriptor.toString().contains(dremioProcess)) {
        vmid = descriptor.id();
        dremioCount++;
      }
    }

    if (dremioCount != 1) {
      throw new UnsupportedOperationException("Failed to attach: couldn't find Dremio process to attach");
    }
    return vmid;
  }
}
