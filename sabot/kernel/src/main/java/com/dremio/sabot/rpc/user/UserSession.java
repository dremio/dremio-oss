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
package com.dremio.sabot.rpc.user;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.ValidationException;

import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.proto.UserBitShared.RpcEndpointInfos;
import com.dremio.exec.proto.UserBitShared.UserCredentials;
import com.dremio.exec.proto.UserProtos.Property;
import com.dremio.exec.proto.UserProtos.RecordBatchFormat;
import com.dremio.exec.proto.UserProtos.UserProperties;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.store.ischema.InfoSchemaConstants;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class UserSession {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserSession.class);

  public static final String SCHEMA = "schema";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String IMPERSONATION_TARGET = "impersonation_target";

  // known property names in lower case
  private static final Set<String> KNOWN_PROPERTIES = ImmutableSet.of(SCHEMA, USER, PASSWORD, IMPERSONATION_TARGET);

  private final AtomicInteger queryCount = new AtomicInteger(0);
  private boolean supportComplexTypes = false;
  private UserCredentials credentials;
  private Map<String, String> properties = Maps.newHashMap();
  private List<String> defaultSchemaPath;
  private OptionManager sessionOptions;
  private RpcEndpointInfos clientInfos;
  private boolean useLegacyCatalogName = false;
  private Quoting initialQuoting;
  private boolean supportFullyQualifiedProjections;
  private RecordBatchFormat recordBatchFormat = RecordBatchFormat.DREMIO_1_4;
  private boolean exposeInternalSources = false;
  private SubstitutionSettings substitutionSettings = SubstitutionSettings.of();

  public static class Builder {
    UserSession userSession;

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder withCredentials(UserCredentials credentials) {
      userSession.credentials = credentials;
      return this;
    }

    public Builder withOptionManager(OptionManager systemOptions) {
      userSession.sessionOptions = new SessionOptionManager(systemOptions, userSession);
      return this;
    }

    public Builder withDefaultSchema(List<String> defaultSchemaPath){
      userSession.defaultSchemaPath = defaultSchemaPath;
      return this;
    }

    public Builder withClientInfos(RpcEndpointInfos infos) {
      userSession.clientInfos = infos;
      return this;
    }

    public Builder withRecordBatchFormat(RecordBatchFormat recordBatchFormat) {
      userSession.recordBatchFormat = recordBatchFormat;
      return this;
    }

    public Builder withLegacyCatalog() {
      userSession.useLegacyCatalogName = true;
      return this;
    }

    public Builder withInitialQuoting(Quoting quoting) {
      userSession.initialQuoting = quoting;
      return this;
    }

    public Builder withFullyQualifiedProjectsSupport(boolean value) {
      userSession.supportFullyQualifiedProjections = value;
      return this;
    }

    public Builder withSubstitutionSettings(final SubstitutionSettings substitutionSettings) {
      userSession.substitutionSettings = substitutionSettings;
      return this;
    }

    public Builder withUserProperties(UserProperties properties) {
      userSession.properties = Maps.newHashMap();
      if (properties != null) {
        for (int i = 0; i < properties.getPropertiesCount(); i++) {
          final Property property = properties.getProperties(i);
          final String propertyName = property.getKey().toLowerCase();
          if (KNOWN_PROPERTIES.contains(propertyName)) {
            userSession.properties.put(propertyName, property.getValue());
          } else {
            logger.warn("Ignoring unknown property: {}", propertyName);
          }
        }
      }
      return this;
    }

    public Builder setSupportComplexTypes(boolean supportComplexTypes) {
      userSession.supportComplexTypes = supportComplexTypes;
      return this;
    }

    public Builder exposeInternalSources(boolean exposeInternalSources) {
      userSession.exposeInternalSources = exposeInternalSources;
      return this;
    }

    public UserSession build() {
      UserSession session = userSession;
      userSession = null;
      return session;
    }

    Builder() {
      userSession = new UserSession();
    }
  }

  private UserSession() {
  }

  public boolean isSupportComplexTypes() {
    return supportComplexTypes;
  }

  public OptionManager getOptions() {
    return sessionOptions;
  }

  public UserCredentials getCredentials() {
    return credentials;
  }

  public RpcEndpointInfos getClientInfos() {
    return clientInfos;
  }

  public RecordBatchFormat getRecordBatchFormat() {
    return recordBatchFormat;
  }

  public boolean exposeInternalSources() {
    return exposeInternalSources;
  }

  public SubstitutionSettings getSubstitutionSettings() {
    return substitutionSettings;
  }

  public String getCatalogName() {
    return useLegacyCatalogName ? InfoSchemaConstants.IS_LEGACY_CATALOG_NAME : InfoSchemaConstants.IS_CATALOG_NAME;
  }

  public boolean useLegacyCatalogName() {
    return useLegacyCatalogName;
  }

  /**
   * Does the client requires support for fully qualified column names in projections?
   *
   * Ex:
   *   SELECT
   *       "elastic.yelp".business.city,
   *       "elastic.yelp".business.stars
   *   FROM
   *       "elastic.yelp".business
   *
   * Note: enabling this option disables complex field references in query (ex. mapCol.mapField, listCol[2])
   *
   * @return
   */
  public boolean supportFullyQualifiedProjections() {
    return supportFullyQualifiedProjections;
  }

  public static String getCatalogName(OptionManager options){
    return options.getOption(ExecConstants.USE_LEGACY_CATALOG_NAME) ? InfoSchemaConstants.IS_LEGACY_CATALOG_NAME : InfoSchemaConstants.IS_CATALOG_NAME;
  }

  /**
   * Replace current user credentials with the given user's credentials. Meant to be called only by a
   * {@link InboundImpersonationManager impersonation manager}.
   *
   * @param impersonationManager impersonation manager making this call
   * @param newCredentials user credentials to change to
   */
  public void replaceUserCredentials(final InboundImpersonationManager impersonationManager,
                                     final UserCredentials newCredentials) {
    Preconditions.checkNotNull(impersonationManager, "User credentials can only be replaced by an" +
        " impersonation manager.");
    credentials = newCredentials;
  }

  public String getTargetUserName() {
    return properties.get(IMPERSONATION_TARGET);
  }

  public String getDefaultSchemaName() {
    return getProp(SCHEMA);
  }

  public void incrementQueryCount() {
    queryCount.incrementAndGet();
  }

  public int getQueryCount() {
    return queryCount.get();
  }

  public Quoting getInitialQuoting() {
    return initialQuoting;
  }

  /**
   * Update the schema path for the session.
   * @param newDefaultSchemaPath New default schema path to set. It could be relative to the current default schema or
   *                             absolute schema.
   * @param currentDefaultSchema Current default schema.
   * @throws ValidationException If the given default schema path is invalid in current schema tree.
   */
  public void setDefaultSchemaPath(String newDefaultSchemaPath, SchemaPlus currentDefaultSchema)
      throws ValidationException {
    final List<String> newDefaultPathAsList = SqlUtils.parseSchemaPath(newDefaultSchemaPath);

    SchemaPlus newDefault = null;

    // First try to find the given schema relative to the current default schema.
    // TODO validate if schema exists
//    newDefault = SchemaUtilities.findSchema(currentDefaultSchema, newDefaultPathAsList);

    if (newDefault == null) {
      // If we fail to find the schema relative to current default schema, consider the given new default schema path as
      // absolute schema path.
      newDefault = SchemaUtilities.findSchema(currentDefaultSchema, newDefaultPathAsList);
    }

    if (newDefault == null) {
      SchemaUtilities.throwSchemaNotFoundException(currentDefaultSchema, newDefaultSchemaPath);
    }

    setProp(SCHEMA, SchemaUtilities.getSchemaPath(newDefault));
  }

  /**
   * @return Get current default schema path.
   */
  public String getDefaultSchemaPath() {
    return getProp(SCHEMA);
  }

  /**
   * Get default schema from current default schema path and given schema tree.
   * @param rootSchema
   * @return A {@link org.apache.calcite.schema.SchemaPlus} object.
   */
  public SchemaPlus getDefaultSchema(SchemaPlus rootSchema) {

    final String propertyBasedSchemaPath = getProp(SCHEMA);

    if (Strings.isNullOrEmpty(propertyBasedSchemaPath)) {
      // if we don't have a property, check for a injected session setting.
      if(defaultSchemaPath != null && !defaultSchemaPath.isEmpty()){
        return SchemaUtilities.findSchema(rootSchema, defaultSchemaPath);
      }
      return null;
    }

    return SchemaUtilities.findSchema(rootSchema, propertyBasedSchemaPath);
  }

  private String getProp(String key) {
    return properties.get(key) != null ? properties.get(key) : "";
  }

  private void setProp(String key, String value) {
    properties.put(key, value);
  }
}
