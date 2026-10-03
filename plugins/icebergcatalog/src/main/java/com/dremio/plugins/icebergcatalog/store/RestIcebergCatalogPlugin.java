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
package com.dremio.plugins.icebergcatalog.store;

import static com.dremio.exec.catalog.CatalogOptions.RESTCATALOG_FOLDERS_SUPPORTED;
import static com.dremio.exec.catalog.CatalogOptions.RESTCATALOG_VIEWS_SUPPORTED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_ENABLED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_MUTABLE_ENABLED;
import static com.dremio.plugins.icebergcatalog.store.IcebergCatalogPluginUtils.NAMESPACE_SEPARATOR;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.catalog.exception.CatalogEntityAlreadyExistsException;
import com.dremio.catalog.exception.CatalogEntityForbiddenException;
import com.dremio.catalog.exception.CatalogEntityNotFoundException;
import com.dremio.catalog.exception.CatalogFolderNotEmptyException;
import com.dremio.catalog.exception.CatalogUnsupportedOperationException;
import com.dremio.catalog.exception.InvalidStorageUriException;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.CatalogFolder;
import com.dremio.catalog.model.ImmutableCatalogFolder;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.exec.catalog.AlterTableOption;
import com.dremio.exec.catalog.CreateTableOptions;
import com.dremio.exec.catalog.FolderListing;
import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.conf.DefaultCtasFormatSelection;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.IcebergMetrics;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.CreateParquetTableEntry;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergFeatureManager;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SupportsFsCreation;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.plugins.icebergcatalog.dfs.DatasetFileSystemCache;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.inject.Provider;
import javax.net.ssl.SSLException;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

public class RestIcebergCatalogPlugin extends IcebergCatalogPlugin {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RestIcebergCatalogPlugin.class);

  /**
   * Catalog property that makes the Iceberg REST client send the {@code
   * X-Iceberg-Access-Delegation} header (Iceberg REST spec) on every request.
   */
  public static final String ACCESS_DELEGATION_HEADER_PROPERTY =
      "header.X-Iceberg-Access-Delegation";

  /** Access delegation mode requested when vended credentials are enabled. */
  public static final String VENDED_CREDENTIALS_DELEGATION_MODE = "vended-credentials";

  /** Hadoop S3A property naming the AWS credentials provider of Dremio's S3 file system. */
  private static final String S3A_CREDENTIALS_PROVIDER =
      VendedStorageCredentials.FS_S3A_CREDENTIALS_PROVIDER;

  /**
   * Hadoop's built-in value of {@link #S3A_CREDENTIALS_PROVIDER} (core-default.xml): a chain of
   * providers. Dremio copies the {@code fs.*} defaults into every file system configuration.
   */
  private static final Supplier<String> HADOOP_DEFAULT_CREDENTIALS_PROVIDER =
      Suppliers.memoize(
          () -> {
            Configuration defaults = new Configuration(false);
            defaults.addResource("core-default.xml");
            return defaults.getTrimmed(S3A_CREDENTIALS_PROVIDER);
          });

  /**
   * Property keys (lower case) whose values are secrets and therefore belong in the secret property
   * list, not in the plain property list.
   */
  @VisibleForTesting
  static final Set<String> SENSITIVE_PROPERTY_KEYS =
      ImmutableSet.of(
          "credential",
          "token",
          "header.authorization",
          "fs.s3a.access.key",
          "fs.s3a.secret.key",
          "fs.s3a.session.token",
          "s3.access-key-id",
          "s3.secret-access-key",
          "s3.session-token");

  /** Parts (lower case) of property keys whose values are secrets, see above. */
  @VisibleForTesting
  static final List<String> SENSITIVE_KEY_SUBSTRINGS =
      ImmutableList.of(
          "secret", "password", "account.key", "private.key", "private-key", "sas-token");

  /** Suffixes (lower case) of property keys whose values are secrets, see above. */
  @VisibleForTesting
  static final List<String> SENSITIVE_KEY_SUFFIXES = ImmutableList.of(".token", "-token", "_token");

  /**
   * Property keys (lower case) that only configure the Iceberg REST client (authentication and
   * request headers). They are passed to the catalog but never copied into the Hadoop
   * configuration, so that OAuth2 secrets do not travel with every FileSystem/FileIO configuration.
   * Keys starting with {@link #REST_CLIENT_ONLY_PROPERTY_PREFIXES} are excluded as well.
   */
  @VisibleForTesting
  static final Set<String> REST_CLIENT_ONLY_PROPERTY_KEYS =
      ImmutableSet.of(
          "credential",
          "token",
          "scope",
          "oauth2-server-uri",
          "audience",
          "resource",
          "token-exchange-enabled",
          "token-refresh-enabled",
          "token-expires-in-ms",
          // AWS SigV4 signing of REST requests (rest.sigv4-enabled): credentials of the REST
          // client, not of the storage.
          "rest.access-key-id",
          "rest.secret-access-key",
          "rest.session-token");

  /**
   * Prefixes (lower case) of REST-client-only property keys, see above. {@code rest.client.} covers
   * the HTTP client settings (timeouts, retries).
   */
  private static final List<String> REST_CLIENT_ONLY_PROPERTY_PREFIXES =
      Collections.unmodifiableList(
          Arrays.asList(
              "header.", "rest.auth.", "rest.client.", "urn:ietf:params:oauth:token-type:"));

  private static final String REDACTED = "****";

  /** Secret values (or parts) shorter than this are never redacted: they would mangle messages. */
  private static final int MIN_REDACTED_LENGTH = 4;

  /**
   * Secret values at least this long are redacted wherever they occur. Shorter ones are only
   * redacted as a whole token (not adjacent to a letter or digit), so that e.g. an access key
   * {@code minio} does not turn a bucket name {@code myminio} into {@code my****}.
   */
  private static final int MIN_SUBSTRING_REDACTED_LENGTH = 8;

  /**
   * OAuth2 error codes (RFC 6749 section 5.2) that the Iceberg REST client reports in the message
   * of the exception it throws when a token request fails.
   */
  private static final Pattern OAUTH2_ERROR_CODE =
      Pattern.compile(
          "\\b(invalid_client|unauthorized_client|invalid_grant|invalid_scope"
              + "|unsupported_grant_type|invalid_request)\\b");

  /**
   * The most recent connection failure reported by a started instance, per source (see {@link
   * #failureKey}). When creating a source fails, Dremio closes the failed instance and puts back
   * the source's initial plugin instance, which was never started, and the API error shows the
   * suggested user action of <em>that</em> instance's state. The never-started instance returns the
   * recorded failure so that the user sees the actual cause instead of a generic message. Values
   * are redacted states; entries expire quickly and are removed when a check succeeds.
   */
  private static final Cache<String, SourceState> RECENT_FAILURES =
      Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(2, TimeUnit.MINUTES).build();

  private final List<String> allowedNamespaces;
  private final boolean isRecursiveAllowedNamespaces;
  private final String restEndpoint;
  private final OptionManager optionManager;
  private final Provider<StoragePluginId> pluginIdProvider;
  private final List<Property> configPropertyList;
  private final boolean isUsingVendedCredentials;
  private final List<String> sensitiveKeysInPropertyList;

  /** Secret values to redact from user-visible messages, longest first. */
  private final List<String> secretValues;

  private final String name;

  private final PluginSabotContext sabotContext;

  /** Key of this source in {@link #RECENT_FAILURES}. */
  private final String failureKey;

  /** Whether {@link #start()} was called on this instance. */
  private volatile boolean started;

  /**
   * Per-table storage credentials vended by the catalog. Null unless the source uses vended
   * credentials (created with the file system cache in {@link #start()}).
   */
  private volatile VendedCredentialsCache vendedCredentials;

  public RestIcebergCatalogPlugin(
      RestIcebergCatalogPluginConfig pluginConfig,
      PluginSabotContext sabotContext,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    super(pluginConfig, sabotContext, name);
    this.pluginIdProvider = pluginIdProvider;
    this.allowedNamespaces = pluginConfig.allowedNamespaces;
    this.isRecursiveAllowedNamespaces = pluginConfig.isRecursiveAllowedNamespaces;
    this.restEndpoint = pluginConfig.getRestEndpointURI(sabotContext.getDremioConfig());
    this.optionManager = sabotContext.getOptionManager();
    this.configPropertyList = getConfigPropertyList(pluginConfig);
    this.isUsingVendedCredentials = pluginConfig.isUsingVendedCredentials;
    this.sensitiveKeysInPropertyList = findSensitivePropertyKeys(pluginConfig.propertyList);
    this.secretValues = collectSecretValues(pluginConfig);
    this.name = name;
    this.sabotContext = sabotContext;
    this.failureKey = name + '\n' + restEndpoint;
  }

  /**
   * Merges the plain and secret property lists. Null entries (e.g. left behind by {@code
   * ConnectionConf.applySecretsFrom} when a masked secret was renamed), entries with a blank name
   * and entries with a null value are skipped, since they cannot be passed to the catalog or to the
   * Hadoop configuration. Only key names are logged, never values.
   */
  @VisibleForTesting
  static List<Property> getConfigPropertyList(RestIcebergCatalogPluginConfig pluginConfig) {
    List<Property> props = Lists.newArrayList();
    addValidProperties(props, pluginConfig.propertyList, "propertyList");
    addValidProperties(props, pluginConfig.secretPropertyList, "secretPropertyList");
    return props;
  }

  private static void addValidProperties(
      List<Property> target, @Nullable List<Property> source, String listName) {
    if (source == null) {
      return;
    }
    for (Property p : source) {
      if (p == null) {
        logger.warn(
            "Ignoring an empty entry in {} of the Iceberg REST catalog source. If a credential was"
                + " renamed, re-enter its value.",
            listName);
        continue;
      }
      if (StringUtils.isBlank(p.name)) {
        logger.warn("Ignoring an entry without a name in {}.", listName);
        continue;
      }
      if (p.value == null) {
        logger.warn("Ignoring property '{}' in {} because it has no value.", p.name, listName);
        continue;
      }
      target.add(p);
    }
  }

  /**
   * Returns true if the property key names a secret (credential, token, key, password...). Keep in
   * sync with {@code isSensitivePropertyKey} in {@code dac/ui/src/utils/sourceUtils.ts}, which
   * blocks these keys in the plain property list when a source is saved.
   */
  @VisibleForTesting
  static boolean isSensitivePropertyKey(@Nullable String key) {
    if (key == null) {
      return false;
    }
    String k = key.trim().toLowerCase(Locale.ROOT);
    return SENSITIVE_PROPERTY_KEYS.contains(k)
        || SENSITIVE_KEY_SUBSTRINGS.stream().anyMatch(k::contains)
        || SENSITIVE_KEY_SUFFIXES.stream().anyMatch(k::endsWith);
  }

  /** Returns the names (never values) of sensitive keys present in the given property list. */
  @VisibleForTesting
  static List<String> findSensitivePropertyKeys(@Nullable List<Property> propertyList) {
    if (propertyList == null) {
      return Collections.emptyList();
    }
    return propertyList.stream()
        .filter(p -> p != null && isSensitivePropertyKey(p.name))
        .map(p -> p.name)
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Values that must never appear in user-visible messages (used for redaction only), sorted
   * longest first so that a full {@code id:secret} value is redacted before its parts. Values
   * shorter than {@link #MIN_REDACTED_LENGTH} are left out.
   */
  private static List<String> collectSecretValues(RestIcebergCatalogPluginConfig pluginConfig) {
    Set<String> values = new HashSet<>();
    List<Property> candidates = new ArrayList<>();
    if (pluginConfig.secretPropertyList != null) {
      candidates.addAll(pluginConfig.secretPropertyList);
    }
    if (pluginConfig.propertyList != null) {
      for (Property p : pluginConfig.propertyList) {
        if (p != null && isSensitivePropertyKey(p.name)) {
          candidates.add(p);
        }
      }
    }
    for (Property p : candidates) {
      if (p == null || StringUtils.isBlank(p.value)) {
        continue;
      }
      addRedactionCandidate(values, p.value);
      // OAuth2 client credentials are "<client_id>:<client_secret>"; also redact the secret part.
      int idx = p.value.indexOf(':');
      if (idx >= 0 && idx < p.value.length() - 1) {
        addRedactionCandidate(values, p.value.substring(idx + 1));
      }
    }
    List<String> sorted = new ArrayList<>(values);
    sorted.sort(Comparator.comparingInt(String::length).reversed());
    return Collections.unmodifiableList(sorted);
  }

  private static void addRedactionCandidate(Set<String> values, String candidate) {
    if (candidate.trim().length() >= MIN_REDACTED_LENGTH) {
      values.add(candidate);
    }
  }

  /** Returns true if the property only configures the REST client (see constants above). */
  @VisibleForTesting
  static boolean isRestClientOnlyPropertyKey(@Nullable String key) {
    if (key == null) {
      return false;
    }
    String k = key.trim().toLowerCase(Locale.ROOT);
    if (REST_CLIENT_ONLY_PROPERTY_KEYS.contains(k)) {
      return true;
    }
    for (String prefix : REST_CLIENT_ONLY_PROPERTY_PREFIXES) {
      if (k.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void start() throws IOException {
    started = true;
    if (!sensitiveKeysInPropertyList.isEmpty()) {
      logger.warn(
          "Iceberg REST catalog source {} has sensitive keys {} in its plain catalog properties."
              + " Their values are stored and returned unmasked; move them to the catalog"
              + " credentials (secret properties) instead.",
          name,
          sensitiveKeysInPropertyList);
    }
    if (isUsingVendedCredentials) {
      logger.info(
          "Iceberg REST catalog source {} requests vended credentials ({}={}). Table files are"
              + " accessed with the S3 credentials the catalog vends for each table; tables"
              + " without vended credentials use the storage properties of the source. Endpoint,"
              + " region and other connection settings always come from the source.",
          name,
          ACCESS_DELEGATION_HEADER_PROPERTY,
          VENDED_CREDENTIALS_DELEGATION_MODE);
    }
    super.start();
  }

  /**
   * With vended credentials, file systems are created and cached per table, with the credentials
   * the catalog vends for that table (see {@link #getFsConfForDataset}).
   */
  @Override
  protected DatasetFileSystemCache createFSCache() {
    if (!isUsingVendedCredentials) {
      return super.createFSCache();
    }
    vendedCredentials =
        new VendedCredentialsCache(this::loadVendedTableProperties, name, this::redactSecrets);
    return new DatasetFileSystemCache(this::getFsConfForDataset, optionManager, true);
  }

  /**
   * The FileIO properties the catalog returns for the table, without the source's own catalog
   * properties (the Iceberg REST client merges them in). Credentials for a table that does not
   * exist yet come from a staged creation only if the source has no S3 credentials of its own: a
   * source with static keys writes new tables with them, as it does without vended credentials,
   * also outside the catalog's default table location.
   */
  private Map<String, String> loadVendedTableProperties(TableIdentifier table) {
    boolean stageNewTable = !hasOwnS3Credentials(getFsConfCopy());
    return withoutSourceProperties(
        getCatalogAccessor().loadTableStorageProperties(table, stageNewTable), configPropertyList);
  }

  /**
   * Removes the entries that are the source's own catalog properties (same key and value). The
   * Iceberg REST client builds a table's FileIO properties from the catalog properties, which
   * include every source property, merged with the table config of the catalog's response; e.g.
   * {@code s3.access-key-id} configured on the source must not be taken for a vended credential.
   */
  @VisibleForTesting
  static Map<String, String> withoutSourceProperties(
      Map<String, String> tableProperties, List<Property> sourceProperties) {
    if (tableProperties.isEmpty() || sourceProperties.isEmpty()) {
      return tableProperties;
    }
    Map<String, String> result = new HashMap<>(tableProperties);
    for (Property p : sourceProperties) {
      result.remove(p.name, p.value);
    }
    return result;
  }

  /**
   * Whether the given file system configuration of the source carries S3 credentials of its own: an
   * access key, or a credentials provider that needs none (e.g. an instance profile or an assumed
   * role). Hadoop's built-in provider chain, which Dremio's S3 file system rejects, and the
   * key-based providers without a key do not count.
   */
  @VisibleForTesting
  static boolean hasOwnS3Credentials(Configuration conf) {
    if (hasS3AccessKey(conf)) {
      return true;
    }
    String provider = conf.getTrimmed(S3A_CREDENTIALS_PROVIDER);
    return StringUtils.isNotEmpty(provider)
        && !provider.equals(HADOOP_DEFAULT_CREDENTIALS_PROVIDER.get())
        && !VendedStorageCredentials.SIMPLE_CREDENTIALS_PROVIDER.equals(provider)
        && !VendedStorageCredentials.TEMPORARY_CREDENTIALS_PROVIDER.equals(provider);
  }

  /** Whether an S3 access key is set, for all buckets or for one. */
  private static boolean hasS3AccessKey(Configuration conf) {
    if (StringUtils.isNotBlank(conf.getTrimmed(VendedStorageCredentials.FS_S3A_ACCESS_KEY))) {
      return true;
    }
    for (Map.Entry<String, String> e :
        conf.getPropsWithPrefix(VendedStorageCredentials.FS_S3A_BUCKET_PREFIX).entrySet()) {
      if (e.getKey().endsWith(".access.key") && StringUtils.isNotBlank(e.getValue())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Forgets the vended credentials of a table on this node, and the file systems created with them,
   * after the table was created or dropped: a table created again under the same name may live
   * elsewhere, and the credentials of a new table (or their absence) were looked up before it
   * existed. Other nodes replace theirs when they expire.
   */
  private void forgetTableStorage(List<String> dataset) {
    VendedCredentialsCache credentialsCache = vendedCredentials;
    TableIdentifier table = tableIdentifierOf(dataset);
    if (credentialsCache == null || table == null) {
      return;
    }
    credentialsCache.invalidate(table);
    DatasetFileSystemCache fsCache = getHadoopFileSystemCache();
    if (fsCache != null) {
      fsCache.invalidateDatasets(other -> table.equals(tableIdentifierOf(other)));
    }
  }

  /**
   * The configuration of a file system for the given dataset (full table path, its first element
   * may be null): the source's configuration, with the S3 credentials the catalog vends for the
   * table if it vends any. The file system is replaced shortly before the credentials expire (see
   * {@link DatasetFileSystemCache#FS_EXPIRES_AT_MILLIS}).
   */
  @VisibleForTesting
  Configuration getFsConfForDataset(@Nullable List<String> dataset) {
    Configuration conf = getFsConfCopy();
    VendedCredentialsCache credentialsCache = vendedCredentials;
    TableIdentifier table = tableIdentifierOf(dataset);
    if (credentialsCache == null || table == null) {
      return conf;
    }
    VendedCredentialsCache.Result result = credentialsCache.get(table);
    result.getCredentials().ifPresent(credentials -> credentials.applyTo(conf));
    conf.setLong(DatasetFileSystemCache.FS_EXPIRES_AT_MILLIS, result.getValidUntilMillis());
    return conf;
  }

  /** The table of a dataset path (source, namespace levels, table name), or null if it is none. */
  @VisibleForTesting
  @Nullable
  static TableIdentifier tableIdentifierOf(@Nullable List<String> dataset) {
    if (dataset == null || dataset.size() < 3) {
      return null;
    }
    List<String> levels = dataset.subList(1, dataset.size());
    if (levels.contains(null)) {
      return null;
    }
    return TableIdentifier.of(levels.toArray(new String[0]));
  }

  @Override
  public void close() throws Exception {
    try {
      super.close();
    } finally {
      VendedCredentialsCache credentialsCache = vendedCredentials;
      if (credentialsCache != null) {
        credentialsCache.invalidateAll();
      }
    }
  }

  /**
   * Copies the source's catalog properties (including {@code fs.*} storage settings) into the given
   * Hadoop configuration. Called eagerly from {@link #createCatalog(Configuration)} (which runs in
   * {@code start()} before the file system cache is created) so that storage settings are present
   * without waiting for the lazily built REST catalog. REST-client-only properties (OAuth2
   * credential/token/scope, request headers including the vended-credentials header) are passed to
   * the catalog only and never copied here.
   */
  @VisibleForTesting
  void applyConfigPropertiesToFsConf(Configuration conf) {
    conf.set(IcebergUtils.ENABLE_AZURE_ABFSS_SCHEME, "true");
    replaceHadoopDefaultCredentialsProvider(conf);
    for (Property p : configPropertyList) {
      if (!isRestClientOnlyPropertyKey(p.name)) {
        conf.set(p.name, p.value);
      }
    }
  }

  /**
   * Replaces Hadoop's built-in value of {@code fs.s3a.aws.credentials.provider} with {@code
   * SimpleAWSCredentialsProvider} (access keys). The built-in value (core-default.xml) is a
   * comma-separated chain of providers that Dremio copies into every file system configuration.
   * Dremio's S3 client rejects it ("Invalid AWSCredentialsProvider provided": it takes exactly one
   * provider), while Hadoop S3A, which Dremio uses for metadata and writes, would walk it and fall
   * back to the Dremio host's identity (the {@code AWS_*} environment variables, the EC2 instance
   * profile) when the source has no keys.
   *
   * <p>With the replacement, a source with an access key works without setting the provider, and a
   * source (or a table without vended credentials) with no S3 credentials fails on both clients
   * instead of using the host's identity. A provider set on the source is applied afterwards and
   * wins, including an empty value, with which Dremio derives the provider ({@code
   * FileSystemConfUtil}: access keys, else the {@code AWS_*} environment variables, else the EC2
   * instance profile). Any other value, e.g. one set in core-site.xml, is kept.
   */
  @VisibleForTesting
  static void replaceHadoopDefaultCredentialsProvider(Configuration conf) {
    String value = conf.getTrimmed(S3A_CREDENTIALS_PROVIDER);
    if (value != null && value.equals(HADOOP_DEFAULT_CREDENTIALS_PROVIDER.get())) {
      conf.set(S3A_CREDENTIALS_PROVIDER, VendedStorageCredentials.SIMPLE_CREDENTIALS_PROVIDER);
    }
  }

  /**
   * Every copy carries the source's catalog properties, independently of whether the REST catalog
   * has been built yet (e.g. on executors that never contact the catalog).
   */
  @Override
  public Configuration getFsConfCopy() {
    Configuration conf = super.getFsConfCopy();
    if (configPropertyList != null) {
      applyConfigPropertiesToFsConf(conf);
    }
    return conf;
  }

  @Override
  public SourceState getState() {
    final CatalogAccessor accessor;
    try {
      accessor = getCatalogAccessor();
    } catch (UserException e) {
      if (!started) {
        // The initial instance Dremio falls back to after a failed create: report the failure of
        // the instance that was actually started (see RECENT_FAILURES).
        SourceState recent = RECENT_FAILURES.getIfPresent(failureKey);
        if (recent != null) {
          return recent;
        }
      }
      // Not started or already closed: keep the generic behavior.
      return super.getState();
    }
    try {
      accessor.checkState();
      RECENT_FAILURES.invalidate(failureKey);
      return SourceState.GOOD;
    } catch (Exception ex) {
      IcebergRestCatalogAccessor.NamespaceListingForbiddenException listingForbidden =
          findCause(ex, IcebergRestCatalogAccessor.NamespaceListingForbiddenException.class);
      if (listingForbidden != null) {
        RECENT_FAILURES.invalidate(failureKey);
        return namespaceListingForbiddenState(listingForbidden);
      }
      String description = describeConnectionFailure(ex);
      logger.debug(
          "Iceberg REST catalog source {} is not reachable or rejected the request: {}",
          name,
          description);
      // The suggested user action is what the source create/update API returns as its error
      // message, so it carries the actionable description.
      SourceState state =
          SourceState.badState(
              String.format("Could not connect to %s. %s", name, description),
              String.format("Failure connecting to source: %s", description));
      RECENT_FAILURES.put(failureKey, state);
      return state;
    }
  }

  /**
   * The catalog accepted the credentials but denied listing the top-level namespaces (HTTP 403).
   * That only matters when the source discovers every namespace; with allowed namespaces the
   * principal may legitimately lack privileges on the catalog root.
   */
  private SourceState namespaceListingForbiddenState(RuntimeException failure) {
    String detail =
        redactSecrets(
            failure.getMessage() != null
                ? failure.getMessage()
                : failure.getClass().getSimpleName());
    if (!discoversAllNamespaces()) {
      logger.debug(
          "Iceberg REST catalog source {} may not list the top-level namespaces; it only uses its"
              + " allowed namespaces. Details: {}",
          name,
          detail);
      return SourceState.GOOD;
    }
    String hint =
        "Connected to the Iceberg REST catalog, but it denied listing the top-level namespaces (HTTP"
            + " 403), so no tables will be discovered. Check the privileges granted to the"
            + " configured principal and the OAuth2 'scope' catalog property, or restrict the source"
            + " to the namespaces the principal may access with Allowed Namespaces.";
    logger.debug("Iceberg REST catalog source {}: {} Details: {}", name, hint, detail);
    return SourceState.warnState(
        String.format("%s: %s", name, hint),
        String.format("Failure listing namespaces: %s Details: %s", hint, detail));
  }

  private boolean discoversAllNamespaces() {
    List<String> allowed = getAllowedNamespaces();
    return allowed == null || allowed.stream().allMatch(StringUtils::isBlank);
  }

  private boolean hasConfigProperty(String key) {
    for (Property p : configPropertyList) {
      if (key.equalsIgnoreCase(p.name.trim()) && StringUtils.isNotBlank(p.value)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds a user-facing description of a catalog connection/authentication failure, with a hint
   * based on the Iceberg REST error type and, for token requests, the OAuth2 error code. Configured
   * secret values are redacted.
   */
  @VisibleForTesting
  String describeConnectionFailure(Throwable failure) {
    String hint;
    Throwable reported = failure;
    Throwable networkCause = findNetworkCause(failure);
    SSLException tlsFailure = findCause(failure, SSLException.class);
    InterruptedIOException timeout = findCause(failure, InterruptedIOException.class);
    NotAuthorizedException notAuthorized = findCause(failure, NotAuthorizedException.class);
    ForbiddenException forbidden = findCause(failure, ForbiddenException.class);
    BadRequestException badRequest = findCause(failure, BadRequestException.class);
    ServiceFailureException serviceFailure = findCause(failure, ServiceFailureException.class);
    ServiceUnavailableException unavailable = findCause(failure, ServiceUnavailableException.class);
    NotFoundException notFound = findCause(failure, NotFoundException.class);
    RESTException restException = findCause(failure, RESTException.class);
    RuntimeException tokenFailure = notAuthorized != null ? notAuthorized : badRequest;
    String oauth2Error = tokenFailure != null ? findOAuth2ErrorCode(tokenFailure) : null;
    if (networkCause != null) {
      reported = networkCause;
      hint =
          String.format(
              "Unable to reach the Iceberg REST catalog at %s. Check the endpoint URI and network"
                  + " connectivity.",
              sanitizeEndpoint(restEndpoint));
    } else if (tlsFailure != null) {
      // Checked before timeouts and HTTP errors: the TLS handshake fails before any request (and
      // therefore any credential) is sent.
      reported = tlsFailure;
      hint =
          String.format(
              "The TLS connection to the Iceberg REST catalog at %s failed. Check that the"
                  + " endpoint scheme (http or https) matches the catalog service and, if the"
                  + " catalog uses a self-signed or private CA certificate, that the certificate"
                  + " is trusted by the Dremio JVM (javax.net.ssl.trustStore).",
              sanitizeEndpoint(restEndpoint));
    } else if (timeout != null) {
      reported = timeout;
      hint =
          String.format(
              "The Iceberg REST catalog at %s did not respond in time. Check that the catalog"
                  + " service is up and responsive. The client timeouts can be set with the"
                  + " catalog properties 'rest.client.connection-timeout-ms' and"
                  + " 'rest.client.socket-timeout-ms'.",
              sanitizeEndpoint(restEndpoint));
    } else if (oauth2Error != null) {
      reported = tokenFailure;
      hint = describeOAuth2Error(oauth2Error, notAuthorized != null);
    } else if (notAuthorized != null) {
      reported = notAuthorized;
      hint =
          "The Iceberg REST catalog rejected the credentials (HTTP 401). Check the OAuth2"
              + " credential or token and the OAuth2 server URI.";
    } else if (forbidden != null) {
      reported = forbidden;
      hint =
          "The Iceberg REST catalog denied access (HTTP 403). Check the privileges granted to the"
              + " configured principal.";
    } else if (badRequest != null) {
      reported = badRequest;
      if (mentionsWarehouse(badRequest) && !hasConfigProperty("warehouse")) {
        hint =
            "The Iceberg REST catalog rejected the request (HTTP 400) because the 'warehouse'"
                + " catalog property is not set. Set it to the name of the catalog to use (for"
                + " Apache Polaris, the catalog name).";
      } else {
        hint =
            "The Iceberg REST catalog rejected the request (HTTP 400). Check the catalog"
                + " properties required by the catalog, such as 'warehouse' and the OAuth2 'scope'"
                + " and 'credential'.";
      }
    } else if (serviceFailure != null) {
      reported = serviceFailure;
      hint =
          "The Iceberg REST catalog reported a server error (HTTP 5xx). Check the health and logs"
              + " of the catalog service, then retry.";
    } else if (unavailable != null) {
      reported = unavailable;
      hint =
          "The Iceberg REST catalog is unavailable (HTTP 503), also after the client retried the"
              + " request ('rest.client.max-retries' catalog property, 5 by default). Check the"
              + " health and logs of the catalog service, then retry.";
    } else if (mentionsWarehouse(notFound) || mentionsWarehouse(restException)) {
      reported = notFound != null ? notFound : restException;
      hint =
          "The Iceberg REST catalog did not accept the warehouse named by the 'warehouse' catalog"
              + " property. Check its value (for Apache Polaris, the name of an existing catalog).";
    } else if (restException != null) {
      reported = restException;
      hint =
          "The Iceberg REST catalog returned an unexpected HTTP error (not 400, 401, 403, 500 or"
              + " 503), for example 404 when the endpoint URI or the 'warehouse' catalog property"
              + " is wrong, 409, 429 when the catalog limits the request rate, or 502/504 from a"
              + " proxy (429, 502, 503 and 504 are retried up to 'rest.client.max-retries' times"
              + " first). Check the endpoint URI, the 'warehouse' catalog property and the logs of"
              + " the catalog service.";
    } else {
      hint = "Could not initialize the Iceberg REST catalog client.";
    }
    String detail =
        reported.getMessage() != null ? reported.getMessage() : reported.getClass().getSimpleName();
    return redactSecrets(hint + " Details: " + abbreviateDetail(redactSecrets(detail)));
  }

  /**
   * Makes a server-provided error text fit for a one-line message: drops HTML markup (e.g. the
   * error page of a proxy), collapses whitespace and cuts it to 300 characters. Callers redact
   * secrets before (so that a cut cannot leave part of a secret behind) and after.
   */
  @VisibleForTesting
  static String abbreviateDetail(String detail) {
    return RestCatalogExceptionMapper.abbreviateDetail(detail);
  }

  private String describeOAuth2Error(String code, boolean unauthorized) {
    switch (code) {
      case "invalid_client":
      case "unauthorized_client":
        return String.format(
            "The Iceberg REST catalog rejected the credentials (%s). Check the 'credential' catalog"
                + " credential (client ID and client secret separated by ':') and, if set, the"
                + " 'oauth2-server-uri' catalog property.",
            unauthorized ? "HTTP 401" : "OAuth2 error '" + code + "'");
      case "invalid_scope":
        return hasConfigProperty("scope")
            ? "The OAuth2 server rejected the requested scope (OAuth2 error 'invalid_scope')."
                + " Check the 'scope' catalog property: it must be a scope the principal may"
                + " request (for Apache Polaris, PRINCIPAL_ROLE:ALL or PRINCIPAL_ROLE:<role>)."
            : "The OAuth2 server requires a scope (OAuth2 error 'invalid_scope'). Set the 'scope'"
                + " catalog property (for Apache Polaris, PRINCIPAL_ROLE:ALL or"
                + " PRINCIPAL_ROLE:<role>).";
      case "invalid_grant":
        return "The OAuth2 server rejected the grant (OAuth2 error 'invalid_grant'): the credential"
            + " or token is invalid, expired or revoked. Check the 'credential' or 'token' catalog"
            + " credential.";
      case "unsupported_grant_type":
        return "The OAuth2 server does not support the requested grant type (OAuth2 error"
            + " 'unsupported_grant_type'). Check the 'oauth2-server-uri' catalog property and that"
            + " the server supports the client credentials grant.";
      default:
        return String.format(
            "The OAuth2 server rejected the token request (OAuth2 error '%s'). Check the"
                + " 'credential', 'scope' and 'oauth2-server-uri' catalog properties.",
            code);
    }
  }

  @Nullable
  private static String findOAuth2ErrorCode(RuntimeException e) {
    String message = e.getMessage();
    if (message == null) {
      return null;
    }
    Matcher matcher = OAUTH2_ERROR_CODE.matcher(message);
    return matcher.find() ? matcher.group(1) : null;
  }

  private static boolean mentionsWarehouse(@Nullable Throwable t) {
    return t != null
        && t.getMessage() != null
        && t.getMessage().toLowerCase(Locale.ROOT).contains("warehouse");
  }

  @VisibleForTesting
  String redactSecrets(@Nullable String message) {
    if (message == null || secretValues.isEmpty()) {
      return message;
    }
    String result = message;
    for (String secret : secretValues) {
      if (secret.length() >= MIN_SUBSTRING_REDACTED_LENGTH) {
        result = result.replace(secret, REDACTED);
      } else {
        result =
            Pattern.compile("(?<!\\p{Alnum})" + Pattern.quote(secret) + "(?!\\p{Alnum})")
                .matcher(result)
                .replaceAll(Matcher.quoteReplacement(REDACTED));
      }
    }
    return result;
  }

  @Nullable
  private static Throwable findNetworkCause(Throwable failure) {
    Throwable t = failure;
    int depth = 0;
    while (t != null && depth++ < 20) {
      if (t instanceof ConnectException
          || t instanceof UnknownHostException
          || t instanceof NoRouteToHostException) {
        return t;
      }
      t = t.getCause();
    }
    return null;
  }

  @Nullable
  private static <T extends Throwable> T findCause(Throwable failure, Class<T> type) {
    Throwable t = failure;
    int depth = 0;
    while (t != null && depth++ < 20) {
      if (type.isInstance(t)) {
        return type.cast(t);
      }
      t = t.getCause();
    }
    return null;
  }

  /** Drops user info, query and fragment from the endpoint so it is safe to show. */
  private static String sanitizeEndpoint(@Nullable String endpoint) {
    if (StringUtils.isBlank(endpoint)) {
      return "the configured endpoint";
    }
    try {
      URI uri = new URI(endpoint.trim());
      if (uri.getHost() == null) {
        return "the configured endpoint";
      }
      return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), null, null)
          .toString();
    } catch (URISyntaxException e) {
      return "the configured endpoint";
    }
  }

  @Override
  public CatalogAccessor createCatalog(Configuration config) {
    // Make the storage settings (fs.s3a.* etc.) visible in the plugin's Hadoop configuration right
    // away, instead of only as a side effect of the lazy REST catalog build.
    applyConfigPropertiesToFsConf(config);
    try {
      return new IcebergRestCatalogAccessor(
          createRestCatalog(config),
          optionManager,
          getAllowedNamespaces(),
          isRecursiveAllowedNamespaces(),
          this::redactSecrets);
    } catch (Exception e) {
      // Only setup errors end up here: the REST catalog itself is built lazily, on first use.
      throw UserException.connectionError(e)
          .message(
              "Can't create the %s client for source %s. %s",
              restCatalogImpl().getSimpleName(), name, describeConnectionFailure(e))
          .buildSilently();
    }
  }

  @Override
  public BooleanValidator getEnableOption() {
    return RESTCATALOG_PLUGIN_ENABLED;
  }

  @Override
  public boolean viewsEnabled() {
    return optionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED);
  }

  @Override
  public String errorMessageWhenSupportKeyIsDisabled() {
    return "Iceberg Catalog Source is not supported.";
  }

  @Override
  public Optional<CatalogFolder> createFolder(CatalogEntityKey key, @Nullable String storageUri)
      throws CatalogEntityAlreadyExistsException, CatalogEntityForbiddenException {
    if (!optionManager.getOption(RESTCATALOG_FOLDERS_SUPPORTED)) {
      throw new UnsupportedOperationException(
          "Iceberg Catalog sources do not support creating folders.");
    }

    Map<String, String> propertiesToCreate = new HashMap<>();
    if (storageUri != null) {
      // TODO: DX-99112 - Handle the case of a non-null blank storageUri.
      propertiesToCreate.put("location", storageUri);
    }
    try {
      Map<String, String> updatedProperties =
          getCatalogAccessor().createFolder(key.getKeyComponents(), propertiesToCreate);
      CatalogFolder catalogFolder =
          new ImmutableCatalogFolder.Builder()
              .setFullPath(key.getKeyComponents())
              .setStorageUri(updatedProperties.get("location"))
              .build();
      return Optional.of(catalogFolder);
    } catch (AlreadyExistsException e) {
      throw new CatalogEntityAlreadyExistsException(
          String.format("Folder [%s] already exists.", key.getKeyComponents().toString()), e);
    } catch (ForbiddenException e) {
      throw new CatalogEntityForbiddenException(
          String.format(
              "Storage URI [%s] at folder %s conflicts with other existing namespace",
              storageUri, key.getKeyComponents().toString()),
          e);
    } catch (UserException e) {
      // The accessor maps an HTTP 403 to a permission error that carries the server's reason;
      // report it as forbidden (HTTP 403 in the REST API) instead of a generic conflict.
      if (e.getErrorType() == ErrorType.PERMISSION) {
        throw new CatalogEntityForbiddenException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Optional<CatalogFolder> updateFolder(CatalogEntityKey key, @Nullable String storageUri)
      throws CatalogEntityNotFoundException,
          InvalidStorageUriException,
          CatalogEntityForbiddenException {
    if (!optionManager.getOption(RESTCATALOG_FOLDERS_SUPPORTED)) {
      throw new UnsupportedOperationException(
          "Iceberg Catalog sources do not support updating folders.");
    }
    validateStorageUri(storageUri);
    Map<String, String> propertiesToUpdate = new HashMap<>();
    Set<String> propertiesToRemove = ImmutableSet.of();

    if (storageUri == null) {
      propertiesToRemove = ImmutableSet.of("location");
    } else {
      propertiesToUpdate.put("location", storageUri);
    }
    try {
      Map<String, String> updatedProperties =
          getCatalogAccessor()
              .updateFolder(key.getKeyComponents(), propertiesToUpdate, propertiesToRemove);
      CatalogFolder catalogFolder =
          new ImmutableCatalogFolder.Builder()
              .setFullPath(key.getKeyComponents())
              .setStorageUri(updatedProperties.get("location"))
              .build();
      return Optional.of(catalogFolder);
    } catch (NoSuchNamespaceException e) {
      throw new CatalogEntityNotFoundException(
          String.format("Folder [%s] does not exist.", key.getKeyComponents().toString()), e);
    } catch (ForbiddenException e) {
      throw new CatalogEntityForbiddenException(
          String.format(
              "Storage URI [%s] at folder %s conflicts with other existing namespace",
              storageUri, key.getKeyComponents().toString()),
          e);
    }
  }

  protected void validateStorageUri(String storageUriString) throws InvalidStorageUriException {
    if (storageUriString != null) {
      throw new UnsupportedOperationException("Storage URI is not supported for folders");
    }
  }

  @Override
  public void deleteFolder(CatalogEntityKey key)
      throws ReferenceNotFoundException,
          UserException,
          CatalogFolderNotEmptyException,
          CatalogEntityNotFoundException {
    if (!optionManager.getOption(RESTCATALOG_FOLDERS_SUPPORTED)) {
      throw new UnsupportedOperationException(
          "Iceberg Catalog sources do not support deleting folders.");
    }
    try {
      boolean isDeleted = getCatalogAccessor().dropFolder(key.getKeyComponents());
      if (!isDeleted) {
        // TODO: DX-99112 - Throw the proper checked exception here and see if this happens only
        // when the folder does not exist.
        throw UserException.validationError()
            .message(
                String.format(
                    "Can not delete folder [%s]. There was an Catalog error.",
                    key.getKeyComponents().toString()))
            .buildSilently();
      }
    } catch (NamespaceNotEmptyException e) {
      // A validation error rather than CatalogFolderNotEmptyException: DROP FOLDER
      // (DropFolderHandler) does not handle the latter and would report a system error. The REST
      // API reports both as a validation error.
      throw RestCatalogExceptionMapper.namespaceNotEmpty(
          e,
          Namespace.of(
              key.getKeyComponents()
                  .subList(1, key.getKeyComponents().size())
                  .toArray(new String[0])));
    } catch (NoSuchNamespaceException e) {
      throw new CatalogEntityNotFoundException(
          String.format(
              "Can not delete folder [%s]. Folder does not exist.",
              key.getKeyComponents().toString()),
          e);
    }
  }

  protected Class<?> restCatalogImpl() {
    return RESTCatalog.class;
  }

  protected Map<String, String> buildCatalogProperties(Configuration config) {
    Map<String, String> properties = new HashMap<>();

    String catalogImplClassName = restCatalogImpl().getName();
    properties.put(CatalogProperties.CATALOG_IMPL, catalogImplClassName);
    properties.put(CatalogProperties.URI, getRestEndpoint());

    applyConfigPropertiesToFsConf(config);
    properties.put(IcebergUtils.ENABLE_AZURE_ABFSS_SCHEME, "true");

    for (Property p : configPropertyList) {
      properties.put(p.name, p.value);
    }

    // Catalog properties only: the header is a REST client setting, not a Hadoop setting.
    if (isUsingVendedCredentials
        && !containsKeyIgnoreCase(properties, ACCESS_DELEGATION_HEADER_PROPERTY)) {
      properties.put(ACCESS_DELEGATION_HEADER_PROPERTY, VENDED_CREDENTIALS_DELEGATION_MODE);
    }

    return properties;
  }

  private static boolean containsKeyIgnoreCase(Map<String, String> map, String key) {
    for (String k : map.keySet()) {
      if (key.equalsIgnoreCase(k)) {
        return true;
      }
    }
    return false;
  }

  protected Supplier<Catalog> createRestCatalog(Configuration config) {
    return () ->
        CatalogUtil.loadCatalog(
            restCatalogImpl().getName(), catalogName(), buildCatalogProperties(config), config);
  }

  protected String catalogName() {
    return null;
  }

  protected List<String> getAllowedNamespaces() {
    return allowedNamespaces;
  }

  protected boolean isRecursiveAllowedNamespaces() {
    return isRecursiveAllowedNamespaces;
  }

  protected String getRestEndpoint() {
    return restEndpoint;
  }

  // SupportsIcebergMutablePlugin Methods - START
  @Override
  public IcebergModel getIcebergModel(
      IcebergTableProps tableProps,
      String userName,
      OperatorContext context,
      FileIO fileIO,
      @org.jetbrains.annotations.Nullable String userId) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    // TODO: DX-99790 - rename tableProps.getDatabaseName() to use namespace nomenclature
    List<String> dataset = new ArrayList<>();
    dataset.add(getName());
    dataset.addAll(List.of(tableProps.getDatabaseName().split(NAMESPACE_SEPARATOR)));
    dataset.add(tableProps.getTableName());
    return new IcebergCatalogModel(
        null, getFsConfCopy(), fileIO, context, null, this, dataset, userName, userId);
  }

  @VisibleForTesting
  IcebergModel getIcebergModel(String location, List<String> dataset, String userName) {
    final FileIO fileIO;
    try {
      Builder builder =
          SupportsFsCreation.builder().filePath(location).dataset(dataset).userName(userName);
      FileSystem fs = createFS(builder);
      fileIO = createIcebergFileIO(fs, null, dataset, null, null);
    } catch (IOException e) {
      throw UserException.validationError(e)
          .message("Failure creating File System instance for path %s", location)
          .buildSilently();
    }
    String userId =
        Optional.ofNullable(RequestContext.current().get(UserContext.CTX_KEY))
            .map(UserContext::getUserId)
            .orElse(null);

    return new IcebergCatalogModel(
        null, getFsConfCopy(), fileIO, null, null, this, dataset, userName, userId);
  }

  @Override
  public void createEmptyTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      BatchSchema batchSchema,
      WriterOptions writerOptions) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    Schema schema = SchemaConverter.getBuilder().build().toIcebergSchema(batchSchema);
    SortOrder sortOrder = writerOptions.getDeserializedSortOrder();
    PartitionSpec partitionSpec = writerOptions.getPartitionSpec();
    if (partitionSpec == null) {
      partitionSpec =
          IcebergUtils.getIcebergPartitionSpec(
              batchSchema, writerOptions.getPartitionColumns(), schema);
    }
    Map<String, String> tableProperties =
        Optional.ofNullable(
                writerOptions
                    .getTableFormatOptions()
                    .getIcebergSpecificOptions()
                    .getIcebergTableProps())
            .map(IcebergTableProps::getTableProperties)
            .orElse(Collections.emptyMap());

    List<String> tablePathComponents = tableSchemaPath.getPathComponents();
    @Nullable
    String tableLocation =
        getTableLocationUri(getNewTableLocationFromCatalog(writerOptions, tablePathComponents));
    getCatalogAccessor()
        .createTable(
            tablePathComponents, schema, partitionSpec, sortOrder, tableLocation, tableProperties);
    forgetTableStorage(tablePathComponents);

    IcebergMetrics.countFormatVersion(
        IcebergFeatureManager.getIcebergFormatVersion(tableProperties).getValue(),
        IcebergMetrics.OperationType.CREATE);
  }

  @Override
  public CreateTableEntry createNewTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      IcebergTableProps icebergTableProps,
      WriterOptions writerOptions,
      Map<String, Object> storageOptions,
      CreateTableOptions createTableOptions) {
    // TODO: DX-99828 & DX-99788 - related tech debt
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    if (icebergTableProps == null
        || storageOptions != null
            && storageOptions.containsKey("type")
            && !storageOptions.get("type").equals("iceberg")) {
      throw new UnsupportedOperationException(
          "Iceberg Catalog sources can only be used to manage Iceberg tables.");
    }

    final boolean isCTAS = icebergTableProps.getIcebergOpType().equals(IcebergCommandType.CREATE);
    final List<String> dataset = tableSchemaPath.getPathComponents();
    String tableFolderLocation;

    if (isCTAS) {
      // A table dropped earlier under this name may have left its credentials behind.
      forgetTableStorage(dataset);
      tableFolderLocation = getNewTableLocationFromCatalog(writerOptions, dataset);
      if (tableFolderLocation == null) {
        throw UserException.validationError()
            .message(
                "A table must be created within a valid folder. Please create a folder before creating a table.")
            .buildSilently();
      }
    } else {
      tableFolderLocation =
          getExistingTableLocationFromCatalog(tableSchemaPath.getPathComponents());
    }
    if (!tableFolderLocation.endsWith(Path.SEPARATOR)) {
      tableFolderLocation = tableFolderLocation.concat(Path.SEPARATOR);
    }

    String namespaceIdentifier =
        String.join(NAMESPACE_SEPARATOR, tableSchemaPath.getParent().getPathWithoutRoot());

    // TODO: DX-99832 - investigate the pattern here/elsewhere around recreating icebergTableProps
    icebergTableProps = new IcebergTableProps(icebergTableProps);
    icebergTableProps.setTableLocation(tableFolderLocation);
    icebergTableProps.setTableName(tableSchemaPath.getName());
    icebergTableProps.setDatabaseName(namespaceIdentifier);

    String tableDataFolderPath =
        Path.of(tableFolderLocation).resolve("data/" + icebergTableProps.getUuid()).toString();

    String userId =
        Optional.ofNullable(RequestContext.current().get(UserContext.CTX_KEY))
            .map(UserContext::getUserId)
            .orElse(null);

    return new CreateParquetTableEntry(
        schemaConfig.getUserName(),
        userId,
        this,
        tableDataFolderPath,
        icebergTableProps,
        writerOptions,
        tableSchemaPath);
  }

  @Override
  public void dropTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions)
      throws CatalogEntityNotFoundException {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    try {
      getCatalogAccessor().dropTable(tableSchemaPath.getPathComponents());
    } catch (NoSuchTableException e) {
      if (!isDatasetKnownToDremio(tableSchemaPath)) {
        // Nothing to drop: DROP TABLE fails and DROP TABLE IF EXISTS reports "not found", as for
        // the other sources.
        throw new CatalogEntityNotFoundException(
            String.format("Table [%s] not found", tableSchemaPath));
      }
      // The table was dropped outside Dremio, but Dremio still lists it until the next refresh of
      // the source's dataset names. Let the drop succeed so that Dremio removes its entry.
      logger.info(
          "Table {} no longer exists in Iceberg REST catalog source {} (dropped outside Dremio);"
              + " removing Dremio's entry for it.",
          tableSchemaPath,
          name);
    }
    forgetTableStorage(tableSchemaPath.getPathComponents());
  }

  /** Whether Dremio's catalog has an entry (e.g. from a names refresh) for the dataset. */
  private boolean isDatasetKnownToDremio(NamespaceKey key) {
    try {
      NamespaceService namespaceService = sabotContext.getNamespaceService(SYSTEM_USERNAME);
      return namespaceService != null
          && namespaceService.exists(key, NameSpaceContainer.Type.DATASET);
    } catch (RuntimeException e) {
      logger.debug("Could not check whether Dremio has an entry for dataset {}", key, e);
      return false;
    }
  }

  @Override
  public void alterTable(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      AlterTableOption alterTableOption,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = tableSchemaPath.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);

    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());
    icebergCatalogModel.alterTable(
        icebergCatalogModel.getTableIdentifier(tableLocation), alterTableOption);
  }

  @Override
  public void truncateTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = tableSchemaPath.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);

    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());
    icebergCatalogModel.truncateTable(icebergCatalogModel.getTableIdentifier(tableLocation));
  }

  @Override
  public void rollbackTable(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      RollbackOption rollbackOption,
      TableMutationOptions tableMutationOptions) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = tableSchemaPath.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);

    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());
    icebergCatalogModel.rollbackTable(
        icebergCatalogModel.getTableIdentifier(tableLocation), rollbackOption);
  }

  @Override
  public void addColumns(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      List<Field> columnsToAdd,
      TableMutationOptions tableMutationOptions) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = tableSchemaPath.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);
    final SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    final List<Types.NestedField> icebergFields = schemaConverter.toIcebergFields(columnsToAdd);

    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());
    icebergCatalogModel.addColumns(
        icebergCatalogModel.getTableIdentifier(tableLocation), icebergFields);
  }

  @Override
  public void dropColumn(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      String columnToDrop,
      TableMutationOptions tableMutationOptions) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = tableSchemaPath.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);

    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());
    icebergCatalogModel.dropColumn(
        icebergCatalogModel.getTableIdentifier(tableLocation), columnToDrop);
  }

  @Override
  public void changeColumn(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      String columnToChange,
      Field fieldFromSqlColDeclaration,
      TableMutationOptions tableMutationOptions) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = tableSchemaPath.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);

    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());
    icebergCatalogModel.changeColumn(
        icebergCatalogModel.getTableIdentifier(tableLocation),
        columnToChange,
        fieldFromSqlColDeclaration);
  }

  @Override
  public void addPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      List<Field> columns,
      ResolvedVersionContext versionContext) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    // TODO: DX-99658 - This method currently stores primaryKeys as the iceberg spec schema property
    //  identifier-field-ids. This is in contrast to Dremio's table property dremio.primary_key.
    final List<String> dataset = table.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);
    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());

    icebergCatalogModel.updatePrimaryKey(
        icebergCatalogModel.getTableIdentifier(tableLocation), columns);
  }

  @Override
  public void dropPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = table.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);
    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());

    icebergCatalogModel.updatePrimaryKey(
        icebergCatalogModel.getTableIdentifier(tableLocation), Collections.emptyList());
  }

  @Override
  public List<String> getPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext,
      boolean saveInKvStore) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = table.getPathComponents();
    final Schema tableSchema = getCatalogAccessor().getTableMetadata(dataset).schema();

    // TODO: DX-99658 - This method returns lowercase field names in order to match Dremio's
    //  dremio.primary_key implementation.  See IcebergUtils.getPrimaryKeyFromPropertyValue().
    return tableSchema.identifierFieldNames().stream()
        .map(columnName -> columnName.toLowerCase(Locale.ROOT))
        .collect(Collectors.toList());
  }

  @Override
  public StoragePluginId getId() {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    return pluginIdProvider.get();
  }

  @Override
  public void alterSortOrder(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema batchSchema,
      SchemaConfig schemaConfig,
      List<String> sortOrderColumns,
      TableMutationOptions tableMutationOptions) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = table.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);
    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());

    icebergCatalogModel.replaceSortOrder(
        icebergCatalogModel.getTableIdentifier(tableLocation), sortOrderColumns);
  }

  @Override
  public void updateTableProperties(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema schema,
      SchemaConfig schemaConfig,
      Map<String, String> tableProperties,
      TableMutationOptions tableMutationOptions,
      boolean isRemove) {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    final List<String> dataset = table.getPathComponents();
    final String tableLocation = getExistingTableLocationFromCatalog(dataset);
    final IcebergModel icebergCatalogModel =
        getIcebergModel(tableLocation, dataset, schemaConfig.getUserName());
    final IcebergTableIdentifier icebergTableIdentifier =
        icebergCatalogModel.getTableIdentifier(tableLocation);

    if (isRemove) {
      final List<String> propertyNameList = new ArrayList<>(tableProperties.keySet());
      icebergCatalogModel.removeTableProperties(icebergTableIdentifier, propertyNameList);
    } else {
      icebergCatalogModel.updateTableProperties(icebergTableIdentifier, tableProperties);
    }
  }

  @Override
  public String getDefaultCtasFormat() {
    if (!optionManager.getOption(RESTCATALOG_PLUGIN_MUTABLE_ENABLED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }

    return DefaultCtasFormatSelection.ICEBERG.getDefaultCtasFormat();
  }

  // SupportsIcebergMutablePlugin Methods - END

  protected @Nullable String getTableLocationUri(@Nullable String tableLocation) {
    return tableLocation != null && !tableLocation.isBlank() ? tableLocation : null;
  }

  private @Nullable String getNewTableLocationFromCatalog(
      WriterOptions writerOptions, List<String> dataset) {
    // Passed in by user through the LOCATION SQL parameter
    if (StringUtils.isNotBlank(writerOptions.getTableLocation())) {
      return writerOptions.getTableLocation();
    }

    return getCatalogAccessor().getDatasetLocationFromExistingNamespaceLocationUri(dataset);
  }

  private String getExistingTableLocationFromCatalog(List<String> dataset) {
    return getCatalogAccessor().getTableMetadata(dataset).location();
  }

  @Override
  public boolean createOrUpdateView(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      View view,
      ViewOptions viewOptions,
      NamespaceAttribute... attributes)
      throws IOException,
          CatalogUnsupportedOperationException,
          CatalogEntityAlreadyExistsException,
          CatalogEntityNotFoundException {
    // TODO(DX-99994): we might need to change the function return a representation of view instead
    // of boolean.
    if (!viewsEnabled()) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }
    Preconditions.checkNotNull(viewOptions, "view options must present");
    Preconditions.checkNotNull(viewOptions.getBatchSchema(), "BatchSchema must present");
    SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    Schema schema = schemaConverter.toIcebergSchema(viewOptions.getBatchSchema());
    if (viewOptions.isViewCreate()) {
      return createView(tableSchemaPath, view, schema);
    }
    return updateView(tableSchemaPath, view, schema);
  }

  private boolean createView(NamespaceKey tableSchemaPath, View view, Schema schema)
      throws CatalogEntityAlreadyExistsException, CatalogEntityNotFoundException {
    try {
      getCatalogAccessor()
          .createView(
              tableSchemaPath.getPathComponents(),
              null,
              view.getWorkspaceSchemaPath(),
              schema,
              view.getSql());
      return true;
    } catch (NoSuchNamespaceException e) {
      throw new CatalogEntityNotFoundException(
          "Please create the folders in the path before creating the view. "
              + "One or more folders are missing in the path.",
          e);
    } catch (AlreadyExistsException e) {
      throw new CatalogEntityAlreadyExistsException(
          String.format("View already exists: %s", tableSchemaPath.getName()), e);
    }
  }

  private boolean updateView(NamespaceKey tableSchemaPath, View view, Schema schema)
      throws CatalogEntityNotFoundException {
    try {
      getCatalogAccessor()
          .updateView(
              tableSchemaPath.getPathComponents(),
              null,
              view.getWorkspaceSchemaPath(),
              schema,
              view.getSql());
      return true;
    } catch (NoSuchViewException e) {
      throw new CatalogEntityNotFoundException(
          String.format("View %s cannot be found", tableSchemaPath.getName()));
    }
  }

  @Override
  public void dropView(
      NamespaceKey tableSchemaPath, ViewOptions viewOptions, SchemaConfig schemaConfig)
      throws IOException, CatalogEntityNotFoundException {
    if (!viewsEnabled()) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }
    try {
      getCatalogAccessor().dropView(tableSchemaPath.getPathComponents());
    } catch (NoSuchViewException e) {
      throw new CatalogEntityNotFoundException(
          String.format("View %s cannot be found", tableSchemaPath.getName()));
    }
  }

  @Override
  public Optional<ViewTable> getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    // TODO(DX-100232): Deprecate getView from SupportsReadingViews
    return Optional.empty();
  }

  @Override
  public FolderListing getFolderListing() {
    if (!optionManager.getOption(RESTCATALOG_FOLDERS_SUPPORTED)) {
      throw new UnsupportedOperationException(
          "This operation is unsupported for Iceberg Catalog sources.");
    }
    return getFolderListingFromIterator(name, getCatalogAccessor().getFolderStream());
  }

  private static FolderListing getFolderListingFromIterator(
      String name, Stream<IcebergNamespaceWithProperties> folderStream) {
    return folderStream.map(
            folder ->
                new ImmutableCatalogFolder.Builder()
                    .setFullPath(getFullPath(name, folder.getNamespace().levels()))
                    .setStorageUri(folder.getProperties().get("location"))
                    .build())
        ::iterator;
  }

  private static List<String> getFullPath(String name, String[] namespaceLevels) {
    List<String> fullPath = new ArrayList<>();
    fullPath.add(name);
    fullPath.addAll(Arrays.asList(namespaceLevels));
    return fullPath;
  }
}
