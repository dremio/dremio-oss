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
package com.dremio.attach;

import com.dremio.dac.admin.LocalAdmin;
import com.dremio.dac.resource.ExportProfilesParams;
import com.google.common.base.Preconditions;
import java.lang.instrument.Instrumentation;

/** Agent */
public class DremioAgent {

  public static void agentmain(String args, Instrumentation inst) throws Exception {

    String[] argsArray = args.split("\t", 4);
    String command = argsArray[0];
    String option = argsArray[1];

    if ("export-profiles".equals(command)) {
      Preconditions.checkArgument(argsArray.length == 2, "Expected length of args of 2.");
      LocalAdmin.getInstance().exportProfiles(ExportProfilesParams.fromParamString(option));
    } else if ("backup".equals(command)) {
      Preconditions.checkArgument(argsArray.length == 4, "Expected length of args of 4.");
      LocalAdmin.getInstance().backup(option, argsArray[2], argsArray[3]);
    }
  }
}
