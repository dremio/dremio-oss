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

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;

/**
 * A single mongod resource
 */
class SingleMongoDBResource extends AbstractMongoDBResource {

  private MongodExecutable executable = null;
  private MongodProcess process = null;

  public SingleMongoDBResource(IFeatureAwareVersion version) {
    super(version);
  }

  @Override
  protected void before() throws Throwable {
    final IMongodConfig config = new MongodConfigBuilder()
        .version(getVersion())
        .build();

    executable = Environment.prepareMongod(config);
    process = executable.start();

    setPort(config.net().getPort());

    super.before();
  }

  @Override
  protected void after() {
    super.after();

    if (process != null) {
      process.stop();
      process = null;
    }

    if (executable != null) {
      executable.stop();
      executable = null;
    }
  }
}
