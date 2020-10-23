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
package com.dremio.dac.server;

import static com.dremio.config.DremioConfig.SSL_AUTO_GENERATED_CERTIFICATE;
import static com.dremio.config.DremioConfig.SSL_KEY_STORE_PASSWORD;
import static com.dremio.config.DremioConfig.SSL_KEY_STORE_PATH;
import static com.dremio.config.DremioConfig.SSL_TRUST_STORE_PASSWORD;
import static com.dremio.config.DremioConfig.SSL_TRUST_STORE_PATH;
import static com.dremio.config.DremioConfig.WEB_SSL_PREFIX;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.internal.HttpUrlConnector;
import org.glassfish.jersey.client.spi.Connector;
import org.glassfish.jersey.internal.util.collection.LazyValue;
import org.glassfish.jersey.internal.util.collection.Value;
import org.glassfish.jersey.internal.util.collection.Values;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.AutoCloseables;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.test.DremioTest;
import com.google.common.base.Preconditions;

/**
 * Base HTTPS Test
 */
public class BaseHttpsTestServer extends BaseClientUtils {
  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private DACDaemon currentDremioDaemon;
  private Client client;
  private WebTarget apiV2;

  @Before
  public void setup() throws Exception {
    // hostname doesn't matter as we redirect all requests to localhost
    final String hostname = "test.dremio.test";
    final String localWritePathString = tempFolder.getRoot().getAbsolutePath();
    currentDremioDaemon = DACDaemon.newDremioDaemon(
      DACConfig
        .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
        .autoPort(true)
        .allowTestApis(true)
        .serveUI(false)
        .webSSLEnabled(true)
        .with(WEB_SSL_PREFIX + SSL_AUTO_GENERATED_CERTIFICATE, false)
        .with(WEB_SSL_PREFIX + SSL_KEY_STORE_PATH, getJKSFile())
        .with(WEB_SSL_PREFIX + SSL_KEY_STORE_PASSWORD, "changeme")
        .with(WEB_SSL_PREFIX + SSL_TRUST_STORE_PATH, getJKSFile())
        .with(WEB_SSL_PREFIX + SSL_TRUST_STORE_PASSWORD, "changeme")
        .inMemoryStorage(true)
        .addDefaultUser(true)
        .writePath(localWritePathString)
        .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
        .clusterMode(DACDaemon.ClusterMode.LOCAL),
      DremioTest.CLASSPATH_SCAN_RESULT
    );

    currentDremioDaemon.init();
    initClient(hostname);
  }

  public String getJKSFile() {
    return null;
  }

  protected void initClient(final String hostname) {
    // Use our own Jersey connector provider so that any requests to any hosts are made against local ip.
    final HttpUrlConnectorProvider provider = new HttpUrlConnectorProvider() {
      private final LazyValue<SSLSocketFactory> sslSocketFactory = Values.lazy(new Value<SSLSocketFactory>() {
        @Override
        public SSLSocketFactory get() {
          return new LocalOnlySSLSocketFactory(client.getSslContext().getSocketFactory());
        }
      });

      @Override
      protected Connector createHttpUrlConnector(Client client, ConnectionFactory connectionFactory, int chunkSize,
                                                 boolean fixLengthStreaming, boolean setMethodWorkaround) {
        return new HttpUrlConnector(
          client,
          connectionFactory,
          chunkSize,
          fixLengthStreaming,
          setMethodWorkaround) {
          @Override
          protected void secureConnection(JerseyClient client, HttpURLConnection uc) {
            if (uc instanceof HttpsURLConnection) {
              HttpsURLConnection suc = (HttpsURLConnection) uc;

              final HostnameVerifier verifier = client.getHostnameVerifier();
              if (verifier != null) {
                suc.setHostnameVerifier(verifier);
              }

              if (HttpsURLConnection.getDefaultSSLSocketFactory() == suc.getSSLSocketFactory()) {
                // indicates that the custom socket factory was not set
                suc.setSSLSocketFactory(sslSocketFactory.get());
              }
            }
          }
        };
      }
    };

    final ClientConfig config = new ClientConfig()
      .connectorProvider(provider);

    client = ClientBuilder.newBuilder()
      .withConfig(config)
      .trustStore(currentDremioDaemon.getWebServer().getTrustStore())
      .register(MultiPartFeature.class)
      .build();

    apiV2 = client
      .target(String.format("https://%s:%d", hostname, currentDremioDaemon.getWebServer().getPort()))
      .path("apiv2");
  }

  @Test
  public void basic() throws Exception {
    expectSuccess(apiV2.path("server_status").request(MediaType.APPLICATION_JSON_TYPE).buildGet());
  }

  @Test
  public void ensureServerIsAwareSSLIsEnabled() throws Exception {
    expectSuccess(apiV2.path("test").path("isSecure").request(MediaType.APPLICATION_JSON_TYPE).buildGet());
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(
      () -> {
        if (client != null) {
          client.close();
        }
      },
      currentDremioDaemon
    );
  }

  /**
   *  Points all sockets to the local hostname.
   */
  private static class LocalOnlySSLSocketFactory extends SSLSocketFactory {
    /**
     * SSLSocketWrapper
     */
    private static final class SSLSocketWrapper extends SSLSocket {
      private final SSLSocket delegate;
      private volatile InetSocketAddress address;

      private SSLSocketWrapper(SSLSocket delegate) {
        this.delegate = delegate;
      }

      @Override
      public void addHandshakeCompletedListener(HandshakeCompletedListener listener) {
        delegate.addHandshakeCompletedListener(listener);
      }

      @Override
      public void connect(SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
      }

      @Override
      public void connect(SocketAddress endpoint, int timeout) throws IOException {
        Preconditions.checkArgument(endpoint instanceof InetSocketAddress);
        this.address = (InetSocketAddress) endpoint;

        // Prevent address resolution by using the local ip.
        InetAddress fakeAddress = InetAddress.getByAddress(this.address.getHostName(), InetAddress.getLocalHost().getAddress());
        InetSocketAddress newEndpoint = new InetSocketAddress(fakeAddress, this.address.getPort());
        delegate.connect(newEndpoint, timeout);
      }

      @Override
      public void bind(SocketAddress bindpoint) throws IOException {
        delegate.bind(bindpoint);
      }

      @Override
      public InetAddress getInetAddress() {
        return delegate.getInetAddress();
      }

      @Override
      public InetAddress getLocalAddress() {
        return delegate.getLocalAddress();
      }

      @Override
      public int getPort() {
        return delegate.getPort();
      }

      @Override
      public int getLocalPort() {
        return delegate.getLocalPort();
      }

      @Override
      public SocketAddress getRemoteSocketAddress() {
        return delegate.getRemoteSocketAddress();
      }

      @Override
      public SocketAddress getLocalSocketAddress() {
        return delegate.getLocalSocketAddress();
      }

      @Override
      public SocketChannel getChannel() {
        return delegate.getChannel();
      }

      @Override
      public OutputStream getOutputStream() throws IOException {
        return delegate.getOutputStream();
      }

      @Override
      public SSLParameters getSSLParameters() {
        return delegate.getSSLParameters();
      }

      @Override
      public boolean getTcpNoDelay() throws SocketException {
        return delegate.getTcpNoDelay();
      }

      @Override
      public int getSoLinger() throws SocketException {
        return delegate.getSoLinger();
      }

      @Override
      public int getSoTimeout() throws SocketException {
        return delegate.getSoTimeout();
      }

      @Override
      public boolean getKeepAlive() throws SocketException {
        return delegate.getKeepAlive();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public boolean getEnableSessionCreation() {
        return delegate.getEnableSessionCreation();
      }

      @Override
      public String[] getEnabledCipherSuites() {
        return delegate.getEnabledCipherSuites();
      }

      @Override
      public String[] getEnabledProtocols() {
        return delegate.getEnabledProtocols();
      }

      @Override
      public SSLSession getHandshakeSession() {
        return delegate.getHandshakeSession();
      }

      @Override
      public InputStream getInputStream() throws IOException {
        return delegate.getInputStream();
      }

      @Override
      public boolean getNeedClientAuth() {
        return delegate.getNeedClientAuth();
      }

      @Override
      public boolean getOOBInline() throws SocketException {
        return delegate.getOOBInline();
      }

      @Override
      public int getSendBufferSize() throws SocketException {
        return delegate.getSendBufferSize();
      }

      @Override
      public int getReceiveBufferSize() throws SocketException {
        return delegate.getReceiveBufferSize();
      }

      @Override
      public boolean getReuseAddress() throws SocketException {
        return delegate.getReuseAddress();
      }

      @Override
      public SSLSession getSession() {
        return delegate.getSession();
      }

      @Override
      public String[] getSupportedCipherSuites() {
        return delegate.getSupportedCipherSuites();
      }

      @Override
      public String[] getSupportedProtocols() {
        return delegate.getSupportedProtocols();
      }

      @Override
      public int getTrafficClass() throws SocketException {
        return delegate.getTrafficClass();
      }

      @Override
      public boolean getUseClientMode() {
        return delegate.getUseClientMode();
      }

      @Override
      public boolean getWantClientAuth() {
        return delegate.getWantClientAuth();
      }

      @Override
      public void setTcpNoDelay(boolean on) throws SocketException {
        delegate.setTcpNoDelay(on);
      }

      @Override
      public void setSoLinger(boolean on, int linger) throws SocketException {
        delegate.setSoLinger(on, linger);
      }

      @Override
      public void sendUrgentData(int data) throws IOException {
        delegate.sendUrgentData(data);
      }

      @Override
      public void setOOBInline(boolean on) throws SocketException {
        delegate.setOOBInline(on);
      }

      @Override
      public void setSoTimeout(int timeout) throws SocketException {
        delegate.setSoTimeout(timeout);
      }

      @Override
      public void setSendBufferSize(int size) throws SocketException {
        delegate.setSendBufferSize(size);
      }

      @Override
      public void setReceiveBufferSize(int size) throws SocketException {
        delegate.setReceiveBufferSize(size);
      }

      @Override
      public void setKeepAlive(boolean on) throws SocketException {
        delegate.setKeepAlive(on);
      }

      @Override
      public void setTrafficClass(int tc) throws SocketException {
        delegate.setTrafficClass(tc);
      }

      @Override
      public void setReuseAddress(boolean on) throws SocketException {
        delegate.setReuseAddress(on);
      }

      @Override
      public void shutdownInput() throws IOException {
        delegate.shutdownInput();
      }

      @Override
      public void shutdownOutput() throws IOException {
        delegate.shutdownOutput();
      }

      @Override
      public String toString() {
        return delegate.toString();
      }

      @Override
      public boolean isConnected() {
        return delegate.isConnected();
      }

      @Override
      public boolean isBound() {
        return delegate.isBound();
      }

      @Override
      public boolean isClosed() {
        return delegate.isClosed();
      }

      @Override
      public boolean isInputShutdown() {
        return delegate.isInputShutdown();
      }

      @Override
      public boolean isOutputShutdown() {
        return delegate.isOutputShutdown();
      }

      @Override
      public void removeHandshakeCompletedListener(HandshakeCompletedListener listener) {
        delegate.removeHandshakeCompletedListener(listener);
      }

      @Override
      public void setEnableSessionCreation(boolean flag) {
        delegate.setEnableSessionCreation(flag);
      }

      @Override
      public void setEnabledCipherSuites(String[] suites) {
        delegate.setEnabledCipherSuites(suites);
      }

      @Override
      public void setEnabledProtocols(String[] protocols) {
        delegate.setEnabledProtocols(protocols);
      }

      @Override
      public void setNeedClientAuth(boolean need) {
        delegate.setNeedClientAuth(need);
      }

      @Override
      public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
        delegate.setPerformancePreferences(connectionTime, latency, bandwidth);
      }

      @Override
      public void setSSLParameters(SSLParameters params) {
        delegate.setSSLParameters(params);
      }

      @Override
      public void setUseClientMode(boolean mode) {
        delegate.setUseClientMode(mode);
      }

      @Override
      public void setWantClientAuth(boolean want) {
        delegate.setWantClientAuth(want);
      }

      @Override
      public void startHandshake() throws IOException {
        delegate.startHandshake();
      }


    }
    private final SSLSocketFactory delegate;

    public LocalOnlySSLSocketFactory(SSLSocketFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public Socket createSocket() throws IOException {
      return new SSLSocketWrapper((SSLSocket) delegate.createSocket());
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
      Socket socket = createSocket();
      socket.bind(new InetSocketAddress(localAddress, localPort));
      socket.connect(new InetSocketAddress(address, port));
      return socket;
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
      Socket socket = createSocket();
      socket.connect(new InetSocketAddress(host, port));
      return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException, UnknownHostException {
      Socket socket = createSocket();
      socket.connect(new InetSocketAddress(host, port));
      return socket;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
      Socket socket = createSocket();
      socket.connect(new InetSocketAddress(host, port));
      return socket;
    }

    @Override
    public Socket createSocket(Socket s, InputStream consumed, boolean autoClose) throws IOException {
      return delegate.createSocket(s, consumed, autoClose);
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
      return delegate.createSocket(s, host, port, autoClose);
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return delegate.getSupportedCipherSuites();
    }
  }
}
