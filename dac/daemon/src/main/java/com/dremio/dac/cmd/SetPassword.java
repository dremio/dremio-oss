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

import java.util.Optional;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.dac.server.DACConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.users.SimpleUserService;

/**
 * Set password for a given user, used by admins.
 */
@AdminCommand(value = "set-password", description = "Sets passwords for Dremio users (non-LDAP)")
public class SetPassword {

  /**
   * Command line options for set-password.
   */
  @Parameters(separators = "=")
  private static final class SetPasswordOptions {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-u", "--username"}, description="username of user", required=true)
    private String userName = null;

    @Parameter(names= {"-p", "--password"}, description="password", password=true)
    private String password = null;

    public static SetPasswordOptions parse(String[] cliArgs) {
      SetPasswordOptions args = new SetPasswordOptions();
      JCommander jc = JCommander.newBuilder().addObject(args).build();
      jc.setProgramName("dremio-admin set-password");

      try {
        jc.parse(cliArgs);
      } catch (ParameterException p) {
        AdminLogger.log(p.getMessage());
        jc.usage();
        System.exit(1);
      }

      if(args.help) {
        jc.usage();
        System.exit(0);
      }

      if(args.userName != null && args.password == null) {
        char[] pwd = System.console().readPassword("password: ");
        args.password = new String(pwd);
      }
      return args;
    }
  }

  public static void resetPassword(DACConfig dacConfig, String userName, String password) throws Exception {
    Optional<LocalKVStoreProvider> providerOptional = CmdUtils.getKVStoreProvider(dacConfig.getConfig());
    if (!providerOptional.isPresent()) {
      AdminLogger.log("No KVStore detected.");
      return;
    }

    try(LocalKVStoreProvider kvStoreProvider = providerOptional.get()) {
      kvStoreProvider.start();
      new SimpleUserService(() -> kvStoreProvider.asLegacy()).setPassword(userName, password);
    }
    AdminLogger.log("Password changed");
  }

  public static void main(String[] args) {
    final DACConfig dacConfig = DACConfig.newConfig();
    final SetPasswordOptions options = SetPasswordOptions.parse(args);
    try {
      resetPassword(dacConfig, options.userName, options.password);
    } catch (Throwable t) {
      AdminLogger.log("set-password failed:", t);
      System.exit(1);
    }
  }
}
