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

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.LoggerFactory;

import com.google.common.io.BaseEncoding;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongoImportExecutable;
import de.flapdoodle.embed.mongo.MongoImportProcess;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongosExecutable;
import de.flapdoodle.embed.mongo.MongosProcess;
import de.flapdoodle.embed.mongo.config.IMongoImportConfig;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.IMongosConfig;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;
import de.flapdoodle.embed.process.runtime.Starter;

/**
 * Environment shared across mongo helpers
 */
final class Environment implements Closeable{
  private static final Environment INSTANCE;
  static {
    try {
      INSTANCE = new Environment();
      // Add shutdown hook to clean up things when JVM is being shutdown
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          INSTANCE.close();
        } catch (IOException e) {
          throw new IOError(e);
        }
      }, "mongo-environment-shutdown"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }



  private final List<Closeable> resources = new ArrayList<>();

  private final Path tempDirectory ;

  // Create custom starters to that pid can be customized and different for each execution (while executable name stays the same)
  private final Starter<IMongodConfig,MongodExecutable,MongodProcess> mongodStarter = new Starter<IMongodConfig,MongodExecutable,MongodProcess>(newRuntimeConfig(Command.MongoD)) {
    @Override
    protected MongodExecutable newExecutable(IMongodConfig config, Distribution distribution, IRuntimeConfig runtime,
        IExtractedFileSet exe) {
      return new MongodExecutable(distribution, config, runtime, exe) {
        @Override
        protected MongodProcess start(Distribution distribution, IMongodConfig config, IRuntimeConfig runtime) throws java.io.IOException {
          return new MongodProcess(distribution, config, runtime, this) {
            @Override
            protected java.io.File pidFile(java.io.File executeableFile) {
              // customize pid so that multiple processes using same executable might have different pid files
             return Environment.this.pidFile(executeableFile);
            };
          };
        };
      };
    }
  };

  private final Starter<IMongosConfig,MongosExecutable,MongosProcess> mongosStarter = new Starter<IMongosConfig,MongosExecutable,MongosProcess>(newRuntimeConfig(Command.MongoS)) {
    @Override
    protected MongosExecutable newExecutable(IMongosConfig config, Distribution distribution, IRuntimeConfig runtime,
        IExtractedFileSet exe) {
      return new MongosExecutable(distribution, config, runtime, exe) {
        @Override
        protected MongosProcess start(Distribution distribution, IMongosConfig config, IRuntimeConfig runtime) throws java.io.IOException {
          return new MongosProcess(distribution, config, runtime, this) {
            @Override
            protected java.io.File pidFile(java.io.File executeableFile) {
              // customize pid so that multiple processes using same executable might have different pid files
             return Environment.this.pidFile(executeableFile);
            };
          };
        };
      };
    }
  };

  private final Starter<IMongoImportConfig,MongoImportExecutable,MongoImportProcess> mongoImportStarter = new Starter<IMongoImportConfig,MongoImportExecutable,MongoImportProcess>(newToolRuntimeConfig(Command.MongoImport)) {
    @Override
    protected MongoImportExecutable newExecutable(IMongoImportConfig config, Distribution distribution, IRuntimeConfig runtime,
        IExtractedFileSet exe) {
      return new MongoImportExecutable(distribution, config, runtime, exe) {
        @Override
        protected MongoImportProcess start(Distribution distribution, IMongoImportConfig config, IRuntimeConfig runtime) throws java.io.IOException {
          return new MongoImportProcess(distribution, config, runtime, this) {
            @Override
            protected java.io.File pidFile(java.io.File executeableFile) {
              // customize pid so that multiple processes using same executable might have different pid files
             return Environment.this.pidFile(executeableFile);
            };
          };
        };
      };
    }
  };

  private Environment() throws IOException {
    tempDirectory = Files.createTempDirectory("mongotest-");
  }

  static MongodExecutable prepareMongod(IMongodConfig config) {
    return INSTANCE.mongodStarter.prepare(config);
  }

  static MongosExecutable prepareMongos(IMongosConfig config) {
    return INSTANCE.mongosStarter.prepare(config);
  }

  static MongoImportExecutable prepareMongoImport(IMongoImportConfig config) {
    return INSTANCE.mongoImportStarter.prepare(config);
  }

  private final IRuntimeConfig newRuntimeConfig(Command command) {
    return newRuntimeConfig(command, true);
  }

  private final IRuntimeConfig newToolRuntimeConfig(Command command) {
    return newRuntimeConfig(command, false);
  }

  private final IRuntimeConfig newRuntimeConfig(Command command, boolean daemonProcess) {
    final StaticArtifactStore artifactStore = StaticArtifactStore.forCommand(command);
    resources.add(artifactStore);
    return new RuntimeConfigBuilder()
        .defaultsWithLogger(command, LoggerFactory.getLogger(MongoDBResource.class))
        .artifactStore(artifactStore)
        .daemonProcess(daemonProcess)
        .build();
  }

  private File pidFile(File executableFile) {
    // Generate random id (same size as UUID)
    byte[] random = new byte[16];
    ThreadLocalRandom.current().nextBytes(random);
    String id = BaseEncoding.base32Hex().omitPadding().encode(random);

    String name = com.google.common.io.Files.getNameWithoutExtension(executableFile.getName());
    return tempDirectory.resolve(String.format("%s-%s.pid", name, id)).toFile();
  }

  @Override
  public void close() throws IOException {
    for (Closeable resource: resources) {
      resource.close();
    }
    // Delete temporary files
    Files.walk(tempDirectory)
      .sorted(Comparator.reverseOrder())
      .forEach(p -> {
      try {
        Files.delete(p);
      } catch (IOException e) {
        throw new IOError(e);
      }
    });
  }
}
