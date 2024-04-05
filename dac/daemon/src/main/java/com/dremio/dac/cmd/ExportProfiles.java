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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.resource.ExportProfilesParams;
import com.dremio.dac.resource.ExportProfilesParams.ExportFormatType;
import com.dremio.dac.resource.ExportProfilesParams.WriteFileMode;
import com.dremio.dac.resource.ExportProfilesResource;
import com.dremio.dac.resource.ExportProfilesStats;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.admin.profile.ProfilesExporter;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceImpl;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import javax.inject.Provider;

/** Local date time parser. */
class LocalDateTimeConverter implements IStringConverter<LocalDateTime> {

  @Override
  public LocalDateTime convert(String value) {
    String dateFormat = "yyyy-MM-dd";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat + "'T'HH[:mm[:ss]]");

    if (value.length() == dateFormat.length()) {
      // see
      // https://stackoverflow.com/quetions/27454025/unable-to-obtain-localdatetime-from-temporalaccessor-when-parsing-localdatetime
      value += "T00"; // add hours to a string to make sure that parse will not fail for date string
      // without time
    }
    return LocalDateTime.parse(value, formatter);
  }
}

/** Export profiles command. */
@AdminCommand(value = "export-profiles", description = "Exports profiles of jobs from Dremio")
public class ExportProfiles {
  /** Command line options for export profiles */
  @Parameters(separators = "=")
  static final class ExportProfilesOptions {

    @Parameter(
        names = {"-h", "--help"},
        description = "show usage",
        help = true)
    private boolean help = false;

    @Parameter(
        names = {"--output"},
        description = "Path to generate profile chunks." + "Example: /tmp/profiles/",
        required = true)
    private String outputFilePath = null;

    @Parameter(
        names = {"--write-mode"},
        description = "Specifies how we should handle a case, when target file already exists.")
    private WriteFileMode writeMode = WriteFileMode.FAIL_IF_EXISTS;

    @Parameter(
        names = {"--from"},
        description =
            "Export profiles beginning from this date inclusively (job_end_time >= toDate)."
                + "Example: 2011-12-03T10:15:30 ",
        converter = LocalDateTimeConverter.class)
    private LocalDateTime fromDate = null;

    @Parameter(
        names = {"--to"},
        description =
            "Export profiles ending by this date exclusively (job_end_time < toDate)."
                + "Example: 2011-12-03T10:15:30",
        converter = LocalDateTimeConverter.class)
    private LocalDateTime toDate = null;

    @Parameter(
        names = {"-u", "--user"},
        description = "username (admin). Is required for online mode [--online]")
    private String userName = null;

    @Parameter(
        names = {"-p", "--password"},
        description = "password. Is required for online mode [--online]",
        password = true)
    private String password = null;

    @Parameter(
        names = {"-a", "--accept-all"},
        description = "accept all ssl certificates")
    private boolean acceptAll = false;

    @Parameter(
        names = {"--size"},
        description = "Chunk size for each zip file",
        hidden = true)
    private int chunkSize = 10;

    @Parameter(
        names = {"--local-path"},
        description = "Default local path to export",
        hidden = true)
    private String localPath = null;

    @Parameter(
        names = {"--format"},
        description =
            "Format in which profiles should be exported. .JSON and .ZIP(default) are supported.")
    private ExportFormatType outputFormatType = ExportFormatType.ZIP;

    @Parameter(
        names = {"-l", "--local-attach"},
        description =
            "Attach locally to Dremio JVM to authenticate user. Not compatible with user/password options")
    private boolean localAttach = false;

    @Parameter(
        names = {"-o", "--offline"},
        description = "Append this option to use offline export.")
    private boolean offlineMode = false;
  }

  private static boolean isTimeSet = true;

  public static void main(String[] args) throws Exception {
    final ExportProfilesOptions options = new ExportProfilesOptions();

    JCommander jc = JCommander.newBuilder().addObject(options).build();
    jc.setProgramName("dremio-admin export-profiles");
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

    if (options.localAttach && (options.userName != null || options.password != null)) {
      AdminLogger.log("Do not pass username or password when running in local-attach mode");
      jc.usage();
      System.exit(1);
    }

    setTime(options);
    setPath(options);

    final DACConfig dacConfig = DACConfig.newConfig();
    if (!dacConfig.isMaster) {
      throw new UnsupportedOperationException("Profile export should be ran on master node.");
    }

    if (options.localAttach) {
      String[] attachArg = {"export-profiles", getAPIExportParams(options).toParamString()};
      try {
        DremioAttach.main(attachArg);
      } catch (NoClassDefFoundError error) {
        AdminLogger.log(
            "A JDK is required to use local-attach mode. Please make sure JAVA_HOME is correctly configured");
      }
    } else {
      if (options.offlineMode) {
        exportOffline(options, dacConfig);
      } else {
        if (options.userName == null) {
          options.userName = System.console().readLine("username: ");
        }
        if (options.password == null) {
          char[] pwd = System.console().readPassword("password: ");
          options.password = new String(pwd);
        }
        if (!options.acceptAll) {
          final SabotConfig sabotConfig = dacConfig.getConfig().getSabotConfig();
          final ScanResult scanResult = ClassPathScanner.fromPrescan(sabotConfig);
          try (CredentialsService credentialsService =
              CredentialsServiceImpl.newInstance(dacConfig.getConfig(), scanResult)) {
            exportOnline(
                options,
                options.userName,
                options.password,
                options.acceptAll,
                dacConfig,
                () -> credentialsService);
          }
        } else {
          exportOnline(
              options,
              options.userName,
              options.password,
              options.acceptAll,
              dacConfig,
              () -> null);
        }
      }
    }
  }

  private static Long getMilliseconds(LocalDateTime date, ZoneOffset zoneOffset) {
    return date == null ? null : date.toInstant(zoneOffset).toEpochMilli();
  }

  static ExportProfilesParams getAPIExportParams(ExportProfilesOptions options) {
    ZoneOffset zoneOffset = OffsetDateTime.now().getOffset();
    return new ExportProfilesParams(
        options.outputFilePath,
        options.writeMode,
        getMilliseconds(options.fromDate, zoneOffset),
        getMilliseconds(options.toDate, zoneOffset),
        options.outputFormatType,
        options.chunkSize);
  }

  private static void exportOnline(
      ExportProfilesOptions options,
      String userName,
      String password,
      boolean acceptAll,
      DACConfig dacConfig,
      Provider<CredentialsService> credentialsServiceProvider)
      throws IOException, GeneralSecurityException {
    final WebClient client =
        new WebClient(dacConfig, credentialsServiceProvider, userName, password, !acceptAll);

    AdminLogger.log(
        client
            .buildPost(ExportProfilesStats.class, "/export-profiles", getAPIExportParams(options))
            .retrieveStats(options.fromDate, options.toDate, isTimeSet));
  }

  private static void exportOffline(ExportProfilesOptions options, DACConfig dacConfig)
      throws Exception {
    Optional<LocalKVStoreProvider> providerOptional =
        CmdUtils.getKVStoreProvider(dacConfig.getConfig());
    if (!providerOptional.isPresent()) {
      AdminLogger.log("No database found. Profiles are not exported");
      return;
    }
    try (LocalKVStoreProvider kvStoreProvider = providerOptional.get()) {
      kvStoreProvider.start();
      exportOffline(options, kvStoreProvider.asLegacy());
    }
  }

  static void exportOffline(ExportProfilesOptions options, LegacyKVStoreProvider provider)
      throws Exception {
    ProfilesExporter exporter = ExportProfilesResource.getExporter(getAPIExportParams(options));
    AdminLogger.log(
        exporter.export(provider).retrieveStats(options.fromDate, options.toDate, isTimeSet));
  }

  @VisibleForTesting
  static void setPath(ExportProfilesOptions options) {
    if (options.outputFilePath == null) {
      options.outputFilePath = options.localPath;
    }
  }

  @VisibleForTesting
  static void setTime(ExportProfilesOptions options) {
    if (options.toDate == null) {
      options.toDate = LocalDateTime.now();
      isTimeSet = false;
    }
    if (options.fromDate == null) {
      options.fromDate = options.toDate.minusDays(30);
      isTimeSet = false;
    }
    if (!options.fromDate.isBefore(options.toDate)) {
      throw new ParameterException(
          String.format(
              "'from' parameter (%s) should be less than 'to' parameter (%s)",
              options.fromDate, options.toDate));
    }
  }
}
