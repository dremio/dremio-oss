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

import org.junit.rules.TestRule;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import de.flapdoodle.embed.mongo.distribution.Version;

/**
 * Resource to interact with a mongodb cluster
 */
public interface MongoDBResource extends TestRule {

  public static MongoDBResource newSingleMongoDB() {
    return new SingleMongoDBResource(Version.Main.PRODUCTION);
  }
  /**
   * Gets a new client configured to connect with the cluster
   * @return
   */
  MongoClient newClient() throws IOException ;

  /**
   *
   * @return Mongo Client URI used to connect to mongo db.
   */
  MongoClientURI getURI();

  /**
   * Gets the current port for the resource.
   */
  int getPort();

  /**
   * Imports a data file into mongo under the provided database/collection
   *
   * @param dbname the database name
   * @param collection the collection
   * @param file the file to import
   * @param format the file format
   * @param options the import options
   */
  void importData(String dbname, String collection, String file, ImportFormat format, ImportOption... options) throws IOException;
}
