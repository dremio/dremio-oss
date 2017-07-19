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
package com.dremio.dac.daemon;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Dremio command line options
 */
public class DremioStartupOptions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioStartupOptions.class);

  @Parameter(names = { "-h", "--help" }, description = "Provide description of usage.", help = true)
  private boolean help = false;

  @Parameter(names = { "-d", "--debug" }, description = "Whether you want to run the program in debug mode.", required = false)
  private boolean debug = false;

  @Parameter(names = { "-m", "--master" }, description = "What node is the master node. Required only when this node is not the master node.", required = false)
  private String master = "localhost";

  @Parameter(names = { "-c", "--config" }, description = "Configuration file you want to load.  Defaults to loading 'sabot-override.conf' from the classpath.", required = false)
  private String configLocation = null;

  @Parameter
  private List<String> exccess = new ArrayList<>();

  public boolean isDebug() {
    return debug;
  }

  public String getConfigLocation() {
    return configLocation;
  }

  public List<String> getExccess() {
    return exccess;
  }

  public String getMaster() {
    return master;
  }

  public static DremioStartupOptions parse(String[] cliArgs) {
    logger.debug("Parsing arguments.");
    DremioStartupOptions args = new DremioStartupOptions();
    JCommander jc = new JCommander(args, cliArgs);
    if (args.help) {
      jc.usage();
      System.exit(0);
    }
    return args;
  }
}
