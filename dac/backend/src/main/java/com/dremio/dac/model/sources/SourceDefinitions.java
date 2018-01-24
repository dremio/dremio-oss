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

import java.util.Map;

import com.dremio.dac.proto.model.source.ADLConfig;
import com.dremio.dac.proto.model.source.AZBConfig;
import com.dremio.dac.proto.model.source.ClassPathConfig;
import com.dremio.dac.proto.model.source.DB2Config;
import com.dremio.dac.proto.model.source.ElasticConfig;
import com.dremio.dac.proto.model.source.GCSConfig;
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
import com.dremio.service.namespace.source.proto.SourceType;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import io.protostuff.Schema;

/**
 * Kept externally of Sources otherwise we have static initialization ordering problems.
 */
public class SourceDefinitions {
  /**
   * To clear private fields of Source Configurations
   */
  private interface  FieldClearer<T extends Source> {

    void clear(final T source);
  }

  /**
   * SourceDefinition
   */
  public static final class SourceDefinition<S extends Source> {
    private final SourceType type;
    private final Class<S> sourceClass;
    private final Schema<S> schema;
    private final FieldClearer<S> clearer;

    private SourceDefinition(SourceType type, Class<S> sourceClass, Schema<S> schema, FieldClearer<S> clearer) {
      this.type = type;
      this.sourceClass = sourceClass;
      this.schema = schema;
      this.clearer = clearer;
    }

    public static <SOURCE extends Source> Map.Entry<SourceType, SourceDefinition<SOURCE>> of(SourceType type, Class<SOURCE> sourceClass, Schema<SOURCE> schema) {
      return of(type, sourceClass, schema, new FieldClearer<SOURCE>() {
        @Override
        public void clear(SOURCE source) {
        }
      });
    }

    public static <SOURCE extends Source> Map.Entry<SourceType, SourceDefinition<SOURCE>> of(SourceType type, Class<SOURCE> sourceClass, Schema<SOURCE> schema, FieldClearer<SOURCE> clearer) {
      return Maps.immutableEntry(type, new SourceDefinition<>(type, sourceClass, schema, clearer));
    }

    public SourceType getType() {
      return type;
    }

    public Class<S> getSourceClass() {
      return sourceClass;
    }

    public Schema<S> getSchema() {
      return schema;
    }

    public FieldClearer<S> getClearer() {
      return clearer;
    }
  };

  private static final ImmutableMap<SourceType, SourceDefinition<?>> SOURCE_DEFINITIONS =
      ImmutableMap.<SourceType, SourceDefinition<?>> builder()
      .put(SourceDefinition.of(SourceType.UNKNOWN, UnknownConfig.class, UnknownConfig.getSchema()))
      .put(SourceDefinition.of(SourceType.NAS, NASConfig.class, NASConfig.getSchema()))
      .put(SourceDefinition.of(SourceType.HDFS, HdfsConfig.class, HdfsConfig.getSchema()))
      .put(SourceDefinition.of(SourceType.MAPRFS, MapRFSConfig.class, MapRFSConfig.getSchema()))
      .put(SourceDefinition.of(SourceType.S3, S3Config.class, S3Config.getSchema(), new FieldClearer<S3Config>() {
        @Override
        public void clear(S3Config source) {
          source.setAccessKey(null);
          source.setAccessSecret(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.AZB, AZBConfig.class, AZBConfig.getSchema(), new FieldClearer<AZBConfig>() {
        @Override
        public void clear(AZBConfig source) {
          source.setAccessKey(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.ADL, ADLConfig.class, ADLConfig.getSchema(), new FieldClearer<ADLConfig>() {
        @Override
        public void clear(ADLConfig source) {
          source.setRefreshTokenSecret(null);
          source.setClientKeyPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.GCS, GCSConfig.class, GCSConfig.getSchema(), new FieldClearer<GCSConfig>() {
        @Override
        public void clear(GCSConfig source) {
          source.setClientKeySecret(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.MONGO, MongoConfig.class, MongoConfig.getSchema(), new FieldClearer<MongoConfig>() {
        @Override
        public void clear(MongoConfig source) {
          source.setPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.ELASTIC, ElasticConfig.class, ElasticConfig.getSchema(), new FieldClearer<ElasticConfig>() {
        @Override
        public void clear(ElasticConfig source) {
          source.setPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.ORACLE, OracleConfig.class, OracleConfig.getSchema(), new FieldClearer<OracleConfig>() {
        @Override
        public void clear(OracleConfig source) {
          source.setPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.MYSQL, MySQLConfig.class, MySQLConfig.getSchema(), new FieldClearer<MySQLConfig>() {
        @Override
        public void clear(MySQLConfig source) {
          source.setPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.MSSQL, MSSQLConfig.class, MSSQLConfig.getSchema(), new FieldClearer<MSSQLConfig>() {
        @Override
        public void clear(MSSQLConfig source) {
          source.setPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.POSTGRES, PostgresConfig.class, PostgresConfig.getSchema(), new FieldClearer<PostgresConfig>() {
        @Override
        public void clear(PostgresConfig source) {
          source.setPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.REDSHIFT, RedshiftConfig.class, RedshiftConfig.getSchema(), new FieldClearer<RedshiftConfig>() {
        @Override
        public void clear(RedshiftConfig source) {
          source.setPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.KUDU, KuduConfig.class, KuduConfig.getSchema()))
      .put(SourceDefinition.of(SourceType.HBASE, HBaseConfig.class, HBaseConfig.getSchema()))
      .put(SourceDefinition.of(SourceType.HIVE, HiveConfig.class, HiveConfig.getSchema()))
      .put(SourceDefinition.of(SourceType.PDFS, PDFSConfig.class, PDFSConfig.getSchema()))
      .put(SourceDefinition.of(SourceType.DB2, DB2Config.class, DB2Config.getSchema(), new FieldClearer<DB2Config>() {
        @Override
        public void clear(DB2Config source) {
          source.setPassword(null);
        }
      }))
      .put(SourceDefinition.of(SourceType.CLASSPATH, ClassPathConfig.class, ClassPathConfig.getSchema()))
      .build();


  private static final ImmutableMap<Class<? extends Source>, SourceType> SOURCE_TYPES =
      FluentIterable.from(SOURCE_DEFINITIONS.keySet()).uniqueIndex(new Function<SourceType, Class<? extends Source>>() {
        @Override
        public Class<? extends Source> apply(SourceType input) {
          return SOURCE_DEFINITIONS.get(input).getSourceClass();
        }
      });


  public static SourceType getType(Class<? extends Source> sourceClass) {
    return SOURCE_TYPES.get(sourceClass);
  }

  public static Schema<? extends Source> getSourceSchema(SourceType type) {
    SourceDefinition<?> definition = SOURCE_DEFINITIONS.get(type);
    if (definition == null) {
      return null;
    }

    return definition.getSchema();
  }

  /**
   * Clear the source from sensitive information
   *
   * @param source
   */
  public static <S extends Source> void clearSource(S source) {
    if (source == null) {
      return;
    }

    @SuppressWarnings("unchecked")
    SourceDefinition<S> definition = (SourceDefinition<S>) SOURCE_DEFINITIONS.get(source.getSourceType());
    definition.clearer.clear(source);
  }
}
