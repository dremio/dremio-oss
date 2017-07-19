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

import java.io.IOException;
import java.net.URI;
import java.security.KeyStore;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.http.HttpHeader;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.dremio.dac.daemon.NetworkUtil;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.server.DacConfig;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.HttpsConnectorGenerator;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.dac.util.BackupRestoreUtil.BackupStats;
import com.dremio.dac.util.JSONUtil;
import com.dremio.ssl.SSLHelper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * Backup command line.
 */
public class Backup {
  private static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;

  /**
   * Command line options for backup and restore
   */
  @Parameters(separators = "=")
  private static final class BackupManagerOptions {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-d", "--backupdir"}, description="backup directory path. for example, /mnt/dremio/backups or hdfs://$namenode:8020/dremio/backups", required=true)
    private String backupDir = null;

    @Parameter(names= {"-u", "--user"}, description="username (admin)", required=true, password=true,
      echoInput=true /* user is prompted when password=true and parameter is required, but passwords are hidden,
       so enable echoing input */)
    private String userName = null;

    @Parameter(names= {"-p", "--password"}, description="password", required=true, password=true)
    private String password = null;

    @Parameter(names= {"-a", "--accept-all"}, description="accept all ssl certificates")
    private boolean acceptAll = false;


    public static BackupManagerOptions parse(String[] cliArgs) {
      BackupManagerOptions args = new BackupManagerOptions();
      JCommander jc = JCommander.newBuilder()
        .addObject(args)
        .build();
      jc.parse(cliArgs);
      if(args.help){
        jc.usage();
        System.exit(0);
      }
      return args;
    }
  }

  private static <T> T readEntity(Class<T> entityClazz, Invocation invocation) throws IOException {
    Response response = invocation.invoke();
    try {
      response.bufferEntity();
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        if (response.hasEntity()) {
          // Try to parse error message as generic error message JSON type
          try {
            GenericErrorMessage message = response.readEntity(GenericErrorMessage.class);
            throw new IOException(format("Status %d (%s): %s (more info: %s)",
                response.getStatus(),
                response.getStatusInfo().getReasonPhrase(),
                message.getErrorMessage(),
                message.getMoreInfo()));
          } catch (ProcessingException e) {
            // Fallback to String if unparsing is unsuccessful
            throw new IOException(format("Status %d (%s)",
                response.getStatus(),
                response.getStatusInfo().getReasonPhrase(),
                response.readEntity(String.class)));
          }
        }
        throw new IOException(format("Status %d (%s)",
            response.getStatus(),
            response.getStatusInfo().getReasonPhrase()));
      }
      return response.readEntity(entityClazz);
    } finally {
      response.close();
    }
  }

  public static BackupStats createBackup(DacConfig dacConfig, String userName, String password, KeyStore trustStore, URI uri) throws IOException {
    final JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(JSONUtil.prettyMapper());
    ClientBuilder clientBuilder = ClientBuilder.newBuilder()
        .register(provider)
        .register(MultiPartFeature.class);

    if (trustStore != null) {
      clientBuilder.trustStore(trustStore);
    } else {
      SSLContext sslContext = SSLHelper.newAllTrustingSSLContext("SSL");
      HostnameVerifier verifier = SSLHelper.newAllValidHostnameVerifier();
      clientBuilder.hostnameVerifier(verifier);
      clientBuilder.sslContext(sslContext);
    }

    final Client client = clientBuilder.build();
    WebTarget target = client.target(format("%s://%s:%d",
        dacConfig.webSSLEnabled ? "https" : "http", dacConfig.masterNode, dacConfig.getHttpPort())).path("apiv2");

    final UserLogin userLogin = new UserLogin(userName, password);
    final UserLoginSession userLoginSession = readEntity(UserLoginSession.class, target.path("/login").request(JSON).buildPost(Entity.json(userLogin)));


    return readEntity(BackupStats.class, target.path("/backup").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      TokenUtils.AUTH_HEADER_PREFIX + userLoginSession.getToken()).buildPost(Entity.json(uri.toString())));
  }

  public static void main(String[] args) {
    final DacConfig dacConfig = DacConfig.newConfig();
    final BackupManagerOptions options = BackupManagerOptions.parse(args);
    try {
      if (!NetworkUtil.addressResolvesToThisNode(dacConfig.getMasterNode())) {
        throw new UnsupportedOperationException("Backup should be ran on master node " + dacConfig.getMasterNode());
      }

      // Make sure that unqualified paths are resolved locally first, and default filesystem
      // is pointing to file
      Path backupDir = new Path(options.backupDir);
      final String scheme = backupDir.toUri().getScheme();
      if (scheme == null || "file".equals(scheme)) {
        backupDir = backupDir.makeQualified(URI.create("file:///"), FileSystem.getLocal(new Configuration()).getWorkingDirectory());
      }
      URI target = backupDir.toUri();

      KeyStore trustStore = options.acceptAll ? null : new HttpsConnectorGenerator().getTrustStore(dacConfig.getConfig());

      BackupStats backupStats = createBackup(dacConfig, options.userName, options.password, trustStore, target);
      System.out.println(format("Backup created at %s, dremio tables %d, uploaded files %d",
        backupStats.getBackupPath(), backupStats.getTables(), backupStats.getFiles()));
    } catch(IOException e) {
      System.err.println(format("Failed to create backup at %s: %s ", options.backupDir, e.getMessage()));
      System.exit(-1);
    } catch (Exception e) {
      System.err.println(format("Failed to create backup at %s: %s ", options.backupDir, e));
      System.exit(-1);
    }
  }
}
