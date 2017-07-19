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
package com.dremio.provision.yarn;

import static org.apache.twill.api.TwillSpecification.PlacementPolicy.Type.DISTRIBUTED;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.ext.BundledJarRunnable;

import com.dremio.config.DremioConfig;
import com.dremio.provision.yarn.service.YarnDefaultsConfigurator;
import com.google.common.base.Strings;

/**
 * DacDaemon Application specification to run with Twill on YARN
 */
public class DacDaemonYarnApplication implements TwillApplication {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DacDaemonYarnApplication.class);

  public static final String YARN_RUNNABLE_NAME = "BundledJarRunnable";
  public static final String YARN_APPLICATION_NAME_DEFAULT = "DremioDaemon";
  public static final String YARN_BUNDLED_JAR_NAME = "dremio-daemon-bundle.jar";
  public static final String LOGBACK_FILE_NAME = "logback.xml";
  public static final String KEYTAB_FILE_NAME = "dremio-kerberos.keytab";

  public static final String YARN_MEMORY_ON_HEAP = "dremio.yarn.memory.onheap";
  public static final String YARN_MEMORY_OFF_HEAP = "dremio.yarn.memory.offheap";
  public static final String YARN_CPU = "dremio.yarn.cpu";
  public static final String YARN_CONTAINER_COUNT = "dremio.yarn.containerCount";
  public static final String YARN_APP_NAME = "dremio.yarn.clusterName";
  public static final String YARN_CLUSTER_ID = "dremio.yarn.clusterId";
  public static final String YARN_QUEUE_NAME = "dremio.yarn.queue";
  public static final String DREMIO_HOME = "DREMIO_HOME";
  public static final String DREMIO_CONFIG_HOME = "DREMIO_CONF_DIR";
  public static final String DEPLOYMENT_POLICY = "dremio.yarn.deploymentpolicy";
  public static final int MAX_APP_RESTART_RETRIES = 5;

  private final YarnConfiguration yarnConfig;
  private final String yarnBundledJarName;
  private final String logbackFileName;
  private final String keytabFileLocation;
  private List<File> classpathJarNames = new ArrayList<>();
  private List<String> jarNames = new ArrayList<>();

  public DacDaemonYarnApplication(DremioConfig dremioConfig, YarnConfiguration yarnConfig, @NotNull Environment env) {
    this.yarnConfig = yarnConfig;
    final String dremioHome = env.getEnv(DREMIO_HOME);
    final String dremioConfigHome = env.getEnv(DREMIO_CONFIG_HOME);
    this.keytabFileLocation = dremioConfig.getString(DremioConfig.KERBEROS_KEYTAB_PATH);

    List<String> classpathJarNamesTemp = new ArrayList<>();

    Collection<String> defaultClasspaths = yarnConfig.getTrimmedStringCollection(YarnDefaultsConfigurator
      .APP_CLASSPATH_JARS);
    for (String defaultClasspath : defaultClasspaths) {
      classpathJarNamesTemp.add(dremioHome + defaultClasspath);
    }
    classpathJarNamesTemp.addAll(dremioConfig.getStringList(DremioConfig.YARN_APP_CLASSPATH));

    for (String classpathJar : classpathJarNamesTemp) {
      File jarFullPath = new File(classpathJar);
      File parentDir = jarFullPath.getParentFile();
      final String fileName = jarFullPath.getName();
      File[] files = parentDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.matches(fileName);
        }
      });
      if (files != null && files.length > 0) {
        for (File file : files) {
          jarNames.add(file.getName());
          classpathJarNames.add(file);
        }
      } else {
        logger.warn("None of the files in the directory {} match {}", parentDir, fileName);
      }
    }

    if (dremioHome != null) {
      String jarPath = dremioHome + File.separatorChar + "jars" + File.separatorChar + "bundled";
      File bundledDir = new File(jarPath);
      boolean isDirectory = bundledDir.isDirectory();
      if (isDirectory) {
        String[] fileNames = bundledDir.list(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return (name.startsWith("dremio-daemon-bundle") && name.endsWith(".jar"));
          }
        });
        if (fileNames != null && fileNames.length == 1) {
          yarnBundledJarName = jarPath + File.separatorChar + fileNames[0];
          if (dremioConfigHome != null) {
            logbackFileName = dremioConfigHome + File.separatorChar  + "logback.xml";
          } else {
            logbackFileName = dremioHome + File.separatorChar + "conf" + File.separatorChar + "logback.xml";
          }
          return;
        }
      }
    }

    throw new IllegalArgumentException("Dremio Daemon Bundle jar was not found. It should be located at: " +
        "DREMIO_HOME/jars/bundled directory");
  }

  public String getYarnBundledJarName() {
    return yarnBundledJarName;
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


    File jarFile = new File(yarnBundledJarName);
    File logbackFile = new File(logbackFileName);
    URL logbackFileURL = DacDaemonYarnApplication.class.getResource("/"+LOGBACK_FILE_NAME);
    URI logbackURI = logbackFile.toURI();
    if (logbackFileURL != null) {
      try {
        logbackURI = logbackFileURL.toURI();
      } catch (URISyntaxException e) {
        logger.error("Unable to convert {} to URI", logbackFileURL, e);
      }
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
        .add(YARN_RUNNABLE_NAME, new BundledJarRunnable(),
            ResourceSpecification.Builder.with()
                .setVirtualCores(cpuInt)
                .setMemory(memoryOnHeapInt+memoryOffHeapInt, ResourceSpecification.SizeUnit.MEGA)
                .setInstances(containerCountInt).build())
        .withLocalFiles()
        .add(LOGBACK_FILE_NAME, logbackURI, false)
        .add(YARN_BUNDLED_JAR_NAME, jarFile.toURI(), false);

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
