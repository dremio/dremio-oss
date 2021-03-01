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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;

/**
 * A single mongod resource
 */
class SingleMongoDBResource extends AbstractMongoDBResource {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleMongoDBResource.class);
  private MongodExecutable executable = null;
  private MongodProcess process = null;

  public SingleMongoDBResource(IFeatureAwareVersion version) {
    super(version);
  }

  @Override
  protected void before() throws Throwable {
    final int maxRetries = 10;
    for (int i = 0; i < maxRetries; i++) {
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        final IMongodConfig config = new MongodConfigBuilder()
          .version(getVersion())
          .build();
        executable = Environment.prepareMongod(config);
        Future<MongodProcess> startProcess = executor.submit(() -> {
          return executable.start();
        });
        process = startProcess.get(60, TimeUnit.SECONDS);
        setPort(config.net().getPort());
        executor.shutdown();
        super.before();
        return;
      } catch (TimeoutException e) {
        executor.shutdown();
        if (i == maxRetries -1) {
          throw new RuntimeException(String.format("Failed to start a single mongodb after %d retries.", maxRetries), e);
        }
        logger.info("Failed to start a single mongodb, will retry after 10 seconds.", e);
        Thread.sleep(10_000);
      }
    }
  }

  @Override
  protected void after() {
    super.after();

    try {
      if (process != null) {
        process.stop();
        process = null;
      }

      if (executable != null) {
        executable.stop();
        executable = null;
      }
    } catch (Exception e) {
      logger.warn("Skipping the failure to stop the single mongodb source.", e);
    }
  }
}
