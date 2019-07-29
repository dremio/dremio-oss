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
package com.dremio.provision.yarn;

import static org.apache.twill.api.TwillSpecification.PlacementPolicy.Type.DISTRIBUTED;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import com.dremio.config.DremioConfig;
import com.dremio.provision.yarn.service.YarnDefaultsConfigurator;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * DacDaemon Application specification to run with Twill on YARN
 */
public class DacDaemonYarnApplication implements TwillApplication {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DacDaemonYarnApplication.class);

  public static final String YARN_RUNNABLE_NAME = "DremioTwillRunnable";
  public static final String YARN_APPLICATION_NAME_DEFAULT = "DremioDaemon";
  public static final String YARN_BUNDLED_JAR_NAME = "dremio-daemon-bundle.jar";
  public static final String KEYTAB_FILE_NAME = "dremio-kerberos.keytab";

  public static final String YARN_MEMORY_ON_HEAP = "dremio.yarn.memory.onheap";
  public static final String YARN_MEMORY_OFF_HEAP = "dremio.yarn.memory.offheap";
  public static final String YARN_CPU = "dremio.yarn.cpu";
  public static final String YARN_CONTAINER_COUNT = "dremio.yarn.containerCount";
  public static final String YARN_APP_NAME = "dremio.yarn.clusterName";
  public static final String YARN_CLUSTER_ID = "dremio.yarn.clusterId";
  public static final String YARN_QUEUE_NAME = "dremio.yarn.queue";
  public static final String DEPLOYMENT_POLICY = "dremio.yarn.deploymentpolicy";
  public static final int MAX_APP_RESTART_RETRIES = 5;

  public static final String DREMIO_HOME = "DREMIO_HOME";

  private final YarnConfiguration yarnConfig;

  private final Path yarnBundledJarPath;
  private final String keytabFileLocation;
  private List<File> classpathJarNames = new ArrayList<>();
  private List<String> jarNames = new ArrayList<>();

  public DacDaemonYarnApplication(DremioConfig dremioConfig, YarnConfiguration yarnConfig, @NotNull Environment env) {
    this.yarnConfig = yarnConfig;

    final String dremioHome = Preconditions.checkNotNull(env.getEnv(DREMIO_HOME),
        "Environment variable DREMIO_HOME is not set");

    this.keytabFileLocation = dremioConfig.getString(DremioConfig.KERBEROS_KEYTAB_PATH);

    // Gather the list of jars to be added to the container classpath
    Stream<String> classpathJars = Stream.concat(
        yarnConfig.getTrimmedStringCollection(YarnDefaultsConfigurator.CLASSPATH_JARS).stream().map(d -> dremioHome.concat(d)),
        dremioConfig.getStringList(DremioConfig.YARN_CLASSPATH).stream());


    classpathJars.forEach(classpathJar -> {
      Path jarFullPath = Paths.get(classpathJar);
      Path parentDir = jarFullPath.getParent();
      final String fileName = jarFullPath.getFileName().toString();

      try(Stream<Path> paths = Files.list(parentDir)) {
        paths
        .filter(p -> p.getFileName().toString().matches(fileName))
        .forEach(p -> {
          jarNames.add(p.getFileName().toString());
          this.classpathJarNames.add(p.toFile());
        });
      } catch(IOException e) {
        logger.warn("Cannot list files in directory {}", parentDir, e);
      }
    });

    // Create an application bundle jar based on current application classpath
    AppBundleGenerator appBundleGenerator = AppBundleGenerator.of(dremioConfig);
    try {
      yarnBundledJarPath = appBundleGenerator.generateBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TwillSpecification configure() {
    final String yarnDeploymentPolicyStr = yarnConfig.get(DEPLOYMENT_POLICY, DISTRIBUTED.name());

    TwillSpecification.PlacementPolicy.Type yarnDeploymentPolicy = DISTRIBUTED;
    try {
      yarnDeploymentPolicy =
        TwillSpecification.PlacementPolicy.Type.valueOf(yarnDeploymentPolicyStr.toUpperCase());
    } catch(IllegalArgumentException e) {
      logger.error("Invalid Deployment Policy is provided: {}, reverting to {}", yarnDeploymentPolicyStr, DISTRIBUTED);
    }


    URI kerberosKeytabURI = null;
    if (!Strings.isNullOrEmpty(keytabFileLocation)) {
      kerberosKeytabURI = new File(keytabFileLocation).toURI();
    }

    String cpu = yarnConfig.get(YARN_CPU);
    String memoryOnHeap = yarnConfig.get(YARN_MEMORY_ON_HEAP);
    String memoryOffHeap = yarnConfig.get(YARN_MEMORY_OFF_HEAP);
    String containerCount = yarnConfig.get(YARN_CONTAINER_COUNT);
    if (cpu == null) {
      throw new IllegalArgumentException("No cpu was specified in yarn config");
    }
    if (memoryOnHeap == null) {
      throw new IllegalArgumentException("No memory was specified in yarn config");
    }
    if (memoryOffHeap == null) {
      throw new IllegalArgumentException("No memory was specified in yarn config");
    }
    if (containerCount == null) {
      throw new IllegalArgumentException("No container count was specified in yarn config");
    }
    int cpuInt = Integer.valueOf(cpu);
    int memoryOnHeapInt = Integer.valueOf(memoryOnHeap);
    int memoryOffHeapInt = Integer.valueOf(memoryOffHeap);
    int containerCountInt = Integer.valueOf(containerCount);

    TwillSpecification.Builder.MoreFile files = TwillSpecification.Builder.with()
        .setName(yarnConfig.get(YARN_APP_NAME, YARN_APPLICATION_NAME_DEFAULT))
        .withRunnable()
        .add(YARN_RUNNABLE_NAME, new AppBundleRunnable(),
            ResourceSpecification.Builder.with()
                .setVirtualCores(cpuInt)
                .setMemory(memoryOnHeapInt+memoryOffHeapInt, ResourceSpecification.SizeUnit.MEGA)
                .setInstances(containerCountInt).build())
        .withLocalFiles()
        .add(YARN_BUNDLED_JAR_NAME, yarnBundledJarPath.toUri(), false);


    // Adding Dremio jars as resources
    if (kerberosKeytabURI != null) {
      files = files.add(KEYTAB_FILE_NAME, kerberosKeytabURI, false);
    }

    for (File classpathJarName : classpathJarNames) {
      files = files.add(classpathJarName.getName(), classpathJarName, false);
    }

    return files.apply().withPlacementPolicy()
        .add(yarnDeploymentPolicy, YARN_RUNNABLE_NAME)
        .anyOrder().build();
  }

  public List<String> getJarNames() {
    return jarNames;
  }

  /**
   * Unit Test Helper class
   */
  public static class Environment {

    public String getEnv(String name) {
      return System.getenv(name);
    }
  }
}
