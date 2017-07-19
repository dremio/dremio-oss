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
package com.dremio.dac.model.sources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.dremio.dac.model.common.AddressableResource;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.model.namespace.DatasetContainer;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.proto.model.source.AzureBlobStoreConfig;
import com.dremio.dac.proto.model.source.ClassPathConfig;
import com.dremio.dac.proto.model.source.DB2Config;
import com.dremio.dac.proto.model.source.ElasticConfig;
import com.dremio.dac.proto.model.source.HBaseConfig;
import com.dremio.dac.proto.model.source.HdfsConfig;
import com.dremio.dac.proto.model.source.HiveConfig;
import com.dremio.dac.proto.model.source.KuduConfig;
import com.dremio.dac.proto.model.source.MSSQLConfig;
import com.dremio.dac.proto.model.source.MapRFSConfig;
import com.dremio.dac.proto.model.source.MongoConfig;
import com.dremio.dac.proto.model.source.MySQLConfig;
import com.dremio.dac.proto.model.source.NASConfig;
import com.dremio.dac.proto.model.source.OracleConfig;
import com.dremio.dac.proto.model.source.PDFSConfig;
import com.dremio.dac.proto.model.source.PostgresConfig;
import com.dremio.dac.proto.model.source.RedshiftConfig;
import com.dremio.dac.proto.model.source.S3Config;
import com.dremio.dac.proto.model.source.UnknownConfig;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.TimePeriod;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

/**
 * Source model
 */
// Ignoring the type hierarchy/Overriding DatasetContainer annotation
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = SourceUI.class)
@JsonIgnoreProperties(value={ "links", "fullPathList", "resourcePath" }, allowGetters=true)
public class SourceUI implements AddressableResource, DatasetContainer {
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
  @JsonSubTypes({
      @Type(value = NASConfig.class, name = "NAS"),
      @Type(value = HdfsConfig.class, name = "HDFS"),
      @Type(value = MapRFSConfig.class, name = "MAPRFS"),
      @Type(value = S3Config.class, name = "S3"),
      @Type(value = MongoConfig.class, name = "MONGO"),
      @Type(value = ElasticConfig.class, name = "ELASTIC"),
      @Type(value = OracleConfig.class, name = "ORACLE"),
      @Type(value = MySQLConfig.class, name = "MYSQL"),
      @Type(value = MSSQLConfig.class, name = "MSSQL"),
      @Type(value = PostgresConfig.class, name = "POSTGRES"),
      @Type(value = RedshiftConfig.class, name = "REDSHIFT"),
      @Type(value = HBaseConfig.class, name = "HBASE"),
      @Type(value = KuduConfig.class, name = "KUDU"),
      @Type(value = AzureBlobStoreConfig.class, name = "AZURE"),
      @Type(value = HiveConfig.class, name = "HIVE"),
      @Type(value = PDFSConfig.class, name = "PDFS"),
      @Type(value = DB2Config.class, name = "DB2"),
      @Type(value = UnknownConfig.class, name = "UNKNOWN"),
      @Type(value = ClassPathConfig.class, name = "CLASSPATH")
  })
  @JsonSerialize()
  private Source config;
  @JsonProperty
  private NamespaceTree contents;

  private String name;
  private long ctime;
  private String img;
  private String description;
  private String id;
  private UIMetadataPolicy metadataPolicy = UIMetadataPolicy.DEFAULT_UIMETADATA_POLICY;

  private long numberOfDatasets;
  private SourceState state;

  private Long version;
  // acceleration ttl in hours
  private TimePeriod accelerationTTL;

  public SourceUI setConfig(Source sourceConfig) {
    this.config = sourceConfig;
    return this;
  }

  public Source getConfig() {
    return config;
  }

  /**
   * For debugging purposes, the serialization for the REST API is also
   * being done with Jackson, but that is managed in the setup of the rest server.
   */
  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing object.", e);
    }
  }

  public String getId() {
    return id;
  }

  @Override
  public SourceResourcePath getResourcePath() {
    return new SourceResourcePath(new SourceName(name));
  }

  public SourceUI setNumberOfDatasets(long numberOfDatasets) {
    this.numberOfDatasets = numberOfDatasets;
    return this;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getNumberOfDatasets() {
    return numberOfDatasets;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public Long getCtime() {
    return ctime;
  }

  public TimePeriod getAccelerationTTL() {
    return accelerationTTL;
  }

  public void setAccelerationTTL(final TimePeriod accelerationTTL) {
    this.accelerationTTL = accelerationTTL;
  }

  public void setCtime(long ctime) {
    this.ctime = ctime;
  }

  public SourceState getState() {
    return state;
  }

  public void setState(SourceState state) {
    this.state = state;
  }

  public UIMetadataPolicy getMetadataPolicy(){
    return metadataPolicy;
  }

  public void setMetadataPolicy(UIMetadataPolicy metadataPolicy){
    this.metadataPolicy = MoreObjects.firstNonNull(metadataPolicy, UIMetadataPolicy.DEFAULT_UIMETADATA_POLICY);
  }

  public String getImg() {
    return img;
  }

  public void setImg(String img) {
    this.img = img;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public NamespaceTree getContents() {
    return contents;
  }

  public void setContents(NamespaceTree contents) {
    this.contents = contents;
  }

  public List<String> getFullPathList() {
    return ImmutableList.of(name);
  }

  public Map<String, String> getLinks() {
    Map<String, String> links = new HashMap<>();
    String resourcePath = new SourcePath(new SourceName(name)).toUrlPath();
    links.put("self", resourcePath);
    links.put("rename", resourcePath + "/rename");
    final JobFilters jobFilters = new JobFilters()
      .addContainsFilter(name)
      .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    links.put("jobs", jobFilters.toUrl());
    links.put("format", resourcePath + "/folder_format");
    links.put("file_format", resourcePath + "/file_format");
    links.put("file_preview", resourcePath + "/file_preview");
    return links;
  }

  @JsonIgnore
  public SourceConfig asSourceConfig() {
    SourceConfig c = new SourceConfig();
    c.setType(config.getSourceType());
    c.setName(name);
    c.setCtime(ctime);
    c.setImg(img);
    c.setConfig(config.toByteString());
    c.setDescription(description);
    c.setVersion(version);
    c.setAccelerationTTL(accelerationTTL);
    c.setMetadataPolicy(metadataPolicy.asMetadataPolicy());
    c.setId(new EntityId(getId()));
    return c;
  }

  public static SourceUI get(SourceConfig sourceConfig) {
    SourceUI source = getWithPrivates(sourceConfig);
    // we should not set fields that expose passwords and other private parts of the config
    SourceDefinitions.clearSource(source.getConfig());
    return source;
  }

  public static SourceUI getWithPrivates(SourceConfig sourceConfig) {
    SourceUI source = new SourceUI();
    final Source config = Source.fromByteString(sourceConfig.getType(), sourceConfig.getConfig());
    source.setConfig(config);
    source.setCtime(sourceConfig.getCtime());
    source.setDescription(sourceConfig.getDescription());
    source.setImg(sourceConfig.getImg());
    source.setName(sourceConfig.getName());
    source.setState(SourceState.GOOD);
    source.setMetadataPolicy(UIMetadataPolicy.of(sourceConfig.getMetadataPolicy()));
    source.setVersion(sourceConfig.getVersion());
    source.setAccelerationTTL(sourceConfig.getAccelerationTTL());
    source.setId(sourceConfig.getId().getId());
    return source;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj != null && obj instanceof SourceUI) {
      final SourceUI s = (SourceUI) obj;
      // Name uniquely identifies a source. No need to consider the remaining properties.
      return Objects.equals(name, s.name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    // Name uniquely identifies a source. No need to consider the remaining properties.
    return name.hashCode();
  }

  /** helper method to find whether the source is FileSystem based source */
  public static boolean isFSBasedSource(SourceConfig sourceConfig) {
    final SourceType sourceType = sourceConfig.getType();
    switch (sourceType) {
      case HDFS:
      case NAS:
      case MAPRFS:
      case S3:
      case PDFS:
      case AZURE:
      case CLASSPATH:
        return true;
      case MONGO:
      case ELASTIC:
      case ORACLE:
      case MYSQL:
      case MSSQL:
      case POSTGRES:
      case REDSHIFT:
      case HBASE:
      case KUDU: // TODO: Is KUDU a file system based source?
      case HIVE:
      case DB2:
      case UNKNOWN:
        return false;
      default:
        throw new IllegalArgumentException("Unexpected source type: " + sourceType);
    }
  }

  /** helper method to find whether impersonation is enabled in the given source */
  public static boolean impersonationEnabled(SourceConfig sourceConfig) {
    SourceUI source = get(sourceConfig);
    if (sourceConfig.getType() == SourceType.HDFS) {
      return ((HdfsConfig) source.getConfig()).getEnableImpersonation();
    }

    if (sourceConfig.getType() == SourceType.MAPRFS) {
      return ((MapRFSConfig) source.getConfig()).getEnableImpersonation();
    }

    return false;
  }
}
