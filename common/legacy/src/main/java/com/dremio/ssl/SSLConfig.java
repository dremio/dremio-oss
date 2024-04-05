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
package com.dremio.ssl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.security.KeyStore;
import java.util.Optional;
import java.util.Properties;

/**
 * SSL configuration.
 *
 * <p>Use the static factory methods, {@link #newBuilderForClient()}, {@link #newBuilderForServer()}
 * or {@link #of}, to create instances.
 */
public final class SSLConfig {

  // SSL related connection properties
  public static final String ENABLE_SSL = "ssl";
  public static final String TRUST_STORE_TYPE = "trustStoreType";
  public static final String TRUST_STORE_PATH = "trustStore";
  public static final String TRUST_STORE_PASSWORD = "trustStorePassword";
  public static final String DISABLE_CERT_VERIFICATION = "disableCertificateVerification";
  public static final String DISABLE_HOST_VERIFICATION = "disableHostVerification";
  public static final String USE_SYSTEM_TRUST_STORE = "useSystemTrustStore";

  // if set to this value, default behavior is employed
  @VisibleForTesting public static final String UNSPECIFIED = "";

  private final String keyStoreType;
  private final String keyStorePath;
  private final String keyStorePassword;
  private final String keyPassword;

  private final String trustStoreType;
  private final String trustStorePath;
  private final String trustStorePassword;

  // Since a server always presents its certificate, for a client "true" means "do
  // not verify the server certificate". And for a server "true" means "do not require
  // the client to present a certificate".
  private final boolean disablePeerVerification;

  private final boolean disableHostVerification;

  private final boolean useSystemTrustStore;

  // TODO(DX-12921): add other frequently used parameters (e.g. certificate alias)

  private SSLConfig(Builder builder) {
    this.keyStoreType = builder.keyStoreType;
    this.keyStorePath = builder.keyStorePath;
    this.keyStorePassword = builder.keyStorePassword;
    this.keyPassword = builder.keyPassword == null ? builder.keyStorePassword : builder.keyPassword;
    this.trustStoreType = builder.trustStoreType;
    this.trustStorePath = builder.trustStorePath;
    this.trustStorePassword = builder.trustStorePassword;
    this.disablePeerVerification = builder.disablePeerVerification;
    this.disableHostVerification = builder.disableHostVerification;
    this.useSystemTrustStore = builder.useSystemTrustStore;
  }

  public String getKeyStoreType() {
    return keyStoreType;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getKeyPassword() {
    return keyPassword;
  }

  public boolean useDefaultTrustStore() {
    return Strings.isNullOrEmpty(trustStoreType) && Strings.isNullOrEmpty(trustStorePath);
  }

  public String getTrustStoreType() {
    if (!Strings.isNullOrEmpty(trustStoreType)) {
      return trustStoreType;
    }
    return KeyStore.getDefaultType();
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public boolean disablePeerVerification() {
    return disablePeerVerification;
  }

  public boolean disableHostVerification() {
    return disableHostVerification;
  }

  public boolean useSystemTrustStore() {
    return useSystemTrustStore;
  }

  public static final class Builder {

    private String keyStoreType = KeyStore.getDefaultType();
    private String keyStorePath = UNSPECIFIED;
    // From https://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html
    // If there is no keystore password specified, it is assumed to be "".
    private String keyStorePassword = UNSPECIFIED;
    private String keyPassword = null; // defaults to 'keyStorePassword'

    private String trustStoreType = UNSPECIFIED;
    private String trustStorePath = UNSPECIFIED;
    // From https://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html
    // If there is no truststore password specified, it is assumed to be "".
    private String trustStorePassword = UNSPECIFIED;

    private boolean disablePeerVerification = false;
    private boolean disableHostVerification = false;

    private boolean useSystemTrustStore = false;

    private Builder() {}

    /**
     * Set key store type. Defaults to {@link KeyStore#getDefaultType}.
     *
     * @param keyStoreType key store type
     * @return this builder
     */
    public Builder setKeyStoreType(String keyStoreType) {
      this.keyStoreType = keyStoreType;
      return this;
    }

    /**
     * Set key store path. Required on server-side.
     *
     * @param keyStorePath key store path
     * @return this builder
     */
    public Builder setKeyStorePath(String keyStorePath) {
      this.keyStorePath = keyStorePath;
      return this;
    }

    /**
     * Set key store password. Very likely required on server-side. Defaults to empty string ("").
     *
     * @param keyStorePassword key store password
     * @return this builder
     */
    public Builder setKeyStorePassword(String keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    /**
     * Set key password. Default to the value set by {@link #setKeyStorePassword}.
     *
     * @param keyPassword key password
     * @return this builder
     */
    public Builder setKeyPassword(String keyPassword) {
      this.keyPassword = keyPassword;
      return this;
    }

    /**
     * Set trust store type. Defaults to {@link KeyStore#getDefaultType}.
     *
     * @param trustStoreType trust store type
     * @return this builder
     */
    public Builder setTrustStoreType(String trustStoreType) {
      this.trustStoreType = trustStoreType;
      return this;
    }

    /**
     * Set trust store path. Very likely required on client-side.
     *
     * @param trustStorePath trust store password
     * @return this builder
     */
    public Builder setTrustStorePath(String trustStorePath) {
      this.trustStorePath = trustStorePath;
      return this;
    }

    /**
     * Set trust store password. Very likely required on client-side. Defaults to empty string ("").
     *
     * @param trustStorePassword trust store password
     * @return this builder
     */
    public Builder setTrustStorePassword(String trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
    }

    /**
     * Disable verifing the peer. Defaults to {@code false}.
     *
     * @param disable whether to disable
     * @return this builder
     */
    public Builder setDisablePeerVerification(boolean disable) {
      this.disablePeerVerification = disable;
      return this;
    }

    /**
     * Disable host verification. Defaults to {@code false}.
     *
     * @param disable whether to disable
     * @return this builder
     */
    public Builder setDisableHostVerification(boolean disable) {
      this.disableHostVerification = disable;
      return this;
    }

    /**
     * Use the system trust store if trustStoreType is not specified. Only permitted for clients.
     * Defaults to {@code false for servers, true for clients}.
     *
     * @param enable whether to enable
     * @return this builder
     */
    public Builder useSystemTrustStore(boolean enable) {
      useSystemTrustStore = enable;
      return this;
    }

    /**
     * Build a new {@link SSLConfig} instance based on the parameters.
     *
     * @return SSL config
     */
    public SSLConfig build() {
      return new SSLConfig(this);
    }
  }

  /**
   * Static factory method for server SSL configurations.
   *
   * @return new builder
   */
  public static Builder newBuilderForServer() {
    return new Builder().useSystemTrustStore(false);
  }

  /**
   * Static factory method for server SSL configurations.
   *
   * @return new builder
   */
  public static Builder newBuilderForClient() {
    return new Builder().useSystemTrustStore(true);
  }

  /**
   * Creates a {@link SSLConfig} from properties for a client.
   *
   * @param properties connection properties
   * @return SSL config, empty if not enabled
   */
  public static Optional<SSLConfig> of(final Properties properties) {
    if (properties == null) {
      return Optional.empty();
    }

    final Properties canonicalProperties = new Properties();
    properties
        .stringPropertyNames()
        .forEach(s -> canonicalProperties.setProperty(s.toLowerCase(), properties.getProperty(s)));

    final Optional<Boolean> enabledOption = getBooleanProperty(canonicalProperties, ENABLE_SSL);
    return enabledOption
        .filter(Boolean::booleanValue)
        .map(
            ignored -> {
              final SSLConfig.Builder builder = SSLConfig.newBuilderForClient();
              getStringProperty(canonicalProperties, TRUST_STORE_TYPE)
                  .ifPresent(builder::setTrustStoreType);
              getStringProperty(canonicalProperties, TRUST_STORE_PATH)
                  .ifPresent(builder::setTrustStorePath);
              getStringProperty(canonicalProperties, TRUST_STORE_PASSWORD)
                  .ifPresent(builder::setTrustStorePassword);
              getBooleanProperty(canonicalProperties, DISABLE_CERT_VERIFICATION)
                  .ifPresent(builder::setDisablePeerVerification);
              getBooleanProperty(canonicalProperties, DISABLE_HOST_VERIFICATION)
                  .ifPresent(builder::setDisableHostVerification);
              getBooleanProperty(canonicalProperties, USE_SYSTEM_TRUST_STORE)
                  .ifPresent(builder::useSystemTrustStore);
              return builder.build();
            });
  }

  private static Optional<Boolean> getBooleanProperty(Properties canonicalProperties, String key) {
    return Optional.ofNullable(canonicalProperties.getProperty(key.toLowerCase()))
        .map(Boolean::parseBoolean);
  }

  private static Optional<String> getStringProperty(Properties canonicalProperties, String key) {
    return Optional.ofNullable(canonicalProperties.getProperty(key.toLowerCase()));
  }
}
