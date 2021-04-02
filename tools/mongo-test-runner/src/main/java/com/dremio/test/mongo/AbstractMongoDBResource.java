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
package com.dremio.test.mongo;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;

import org.junit.rules.ExternalResource;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import de.flapdoodle.embed.mongo.MongoImportExecutable;
import de.flapdoodle.embed.mongo.config.ImmutableMongoImportConfig;
import de.flapdoodle.embed.mongo.config.MongoImportConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.process.runtime.Network;


class AbstractMongoDBResource extends ExternalResource implements MongoDBResource {


  private final IFeatureAwareVersion version;
  private Optional<Integer> port = Optional.empty();

  protected AbstractMongoDBResource(IFeatureAwareVersion version) {
    this.version = version;
  }

  public IFeatureAwareVersion getVersion() {
    return version;
  }

  protected void setPort(int port) {
    this.port = Optional.of(port);
  }

  @Override
  public int getPort() {
    return port
      .orElseThrow(() -> new IllegalStateException("Trying to access port before server has been started"));
  }

  @Override
  public MongoClient newClient() throws IOException {
    return port
        .map(p -> new com.mongodb.MongoClient("localhost", p))
        .orElseThrow(() -> new IllegalStateException("Trying to access client before server has been started"));
  }

  @Override
  public void importData(String dbname, String collection, String file, ImportFormat format, ImportOption... options) throws IOException {
    int p = port.orElseThrow(() -> new IllegalStateException("Trying to access client before server has been started"));

    ImmutableMongoImportConfig.Builder builder = MongoImportConfig.builder()
        .version(version)
        .net(new Net(p, Network.localhostIsIPv6()))
        .databaseName(dbname)
        .collectionName(collection)
        .importFile(file)
        .type(format.name().toLowerCase(Locale.ROOT));

    for(ImportOption option: options) {
      if (option instanceof ImportOptions) {
        setOption(builder, (ImportOptions) option);
        continue;
      }

      throw new IllegalArgumentException("Unknown option " + option);
    }

    MongoImportExecutable executable = Environment.prepareMongoImport(builder.build());
    executable.start();
  }


  private static void setOption(ImmutableMongoImportConfig.Builder builder, ImportOptions option) {
    switch(option) {
    case JSON_ARRAY:
      builder.isJsonArray(true);
      break;

    case DROP_COLLECTION:
      builder.isDropCollection(true);
      break;

    case UPSERT_DOCUMENTS:
      builder.isUpsertDocuments(true);
      break;

    default:
      throw new AssertionError("Unknown option " + option);
    }
  }

  @Override
  public MongoClientURI getURI() {
    return new MongoClientURI(String.format("mongodb://localhost:%s", getPort()));
  }
}
