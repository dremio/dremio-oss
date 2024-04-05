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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.server.DACConfig;
import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.LocalSecretCredentialsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.List;

/** Encrypt command line. */
@AdminCommand(value = "encrypt", description = "Encrypt a user supplied string")
public final class Encrypt {

  private Encrypt() {}

  /** Command line options for encrypt */
  @Parameters(separators = "=")
  static final class EncryptOptions {
    @Parameter(description = "Secret to be encrypted")
    private List<String> secrets;

    @Parameter(
        names = {"-h", "--help"},
        description = "show usage",
        help = true)
    private boolean help = false;
  }

  public static void main(String[] args) {
    final Encrypt.EncryptOptions options = new Encrypt.EncryptOptions();
    JCommander jc = JCommander.newBuilder().addObject(options).build();
    jc.setProgramName("dremio-admin encrypt");

    try {
      jc.parse(args);
    } catch (ParameterException p) {
      AdminLogger.log(p.getMessage());
      jc.usage();
      System.exit(1);
    }

    if (options.help) {
      jc.usage();
      System.exit(0);
    }

    final String secret;
    if (options.secrets == null || options.secrets.isEmpty()) {
      // Read from console as a fallback
      jc.getConsole().print("secret: ");
      secret = new String(jc.getConsole().readPassword(false));
    } else if (options.secrets.size() == 1) {
      secret = options.secrets.get(0);
    } else {
      secret = null;
      AdminLogger.log("too many arguments provided");
      System.exit(1);
    }

    if (Strings.isNullOrEmpty(secret)) {
      AdminLogger.log("no secret provided");
      System.exit(1);
    }

    try {
      DremioConfig config = DACConfig.newConfig().getConfig();
      ScanResult scanResult = ClassPathScanner.fromPrescan(config.getSabotConfig());

      String uri = encrypt(config, scanResult, secret);
      System.out.println(uri);
    } catch (CredentialsException e) {
      AdminLogger.log("Failed during secret encryption.", e);
      System.exit(1);
    }
  }

  @VisibleForTesting
  static String encrypt(DremioConfig config, ScanResult scanResult, String secret)
      throws CredentialsException {
    LocalSecretCredentialsProvider provider = LocalSecretCredentialsProvider.of(config, scanResult);
    return provider.encrypt(secret).toString();
  }
}
