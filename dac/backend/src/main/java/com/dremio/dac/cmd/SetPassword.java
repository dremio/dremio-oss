/*
 * Copyright (C) 2017 Dremio Corporation
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

import static java.lang.String.format;

import java.io.File;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.config.DremioConfig;
import com.dremio.dac.server.DacConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.users.SimpleUserService;

/**
 * Set password for a given user, used by admins.
 */
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

    @Parameter(names= {"-p", "--password"}, description="password must be at least 8 letters long, must contain at least one number and one letter", required=true)
    private String password = null;

    public static SetPasswordOptions parse(String[] cliArgs) {
      SetPasswordOptions args = new SetPasswordOptions();
      JCommander jc = new JCommander(args, cliArgs);
      if(args.help){
        jc.usage();
        System.exit(0);
      }
      return args;
    }
  }

  public static void resetPassword(DacConfig dacConfig, String userName, String password) throws Exception {
    final File dbDir = new File(dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING));

    if (dbDir.exists() && dbDir.isDirectory()) {
      if (!dbDir.canWrite()) {
        String user = System.getProperty("user.name");
        throw new IllegalAccessException(format("The current user %s doesn't have write access to the directory %s", user, dbDir.getAbsolutePath()));
      }

      try(final LocalKVStoreProvider kvStoreProvider = new LocalKVStoreProvider(ClassPathScanner.fromPrescan(SabotConfig.create()), dbDir.getAbsolutePath(), dacConfig.inMemoryStorage, true);
              ) {
        kvStoreProvider.start();
        new SimpleUserService(kvStoreProvider).setPassword(userName, password);
      }
    } else {
      throw new IllegalArgumentException("Invalid kvstore directory " + dbDir + " please check dremio config");
    }
    System.out.println("Password changed");
  }

  public static void main(String[] args) {
    final DacConfig dacConfig = DacConfig.newConfig();
    final SetPasswordOptions options = SetPasswordOptions.parse(args);
    try {
      resetPassword(dacConfig, options.userName, options.password);
    } catch (Throwable t) {
      t.printStackTrace();
      System.err.println("set-password failed " + t);
      System.exit(-1);
    }
  }
}
