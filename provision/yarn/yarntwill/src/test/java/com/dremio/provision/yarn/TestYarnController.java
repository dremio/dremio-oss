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


import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_HOSTNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogThrowable;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.VM;
import com.dremio.config.DremioConfig;
import com.dremio.provision.Property;
import com.dremio.provision.PropertyType;
import com.dremio.provision.yarn.service.YarnDefaultsConfigurator;
import com.dremio.test.TemporarySystemProperties;

/**
 * Test YarnController
 */
public class TestYarnController {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestYarnController.class);

  private static String LOG_STRING = "o.a.t.i.a.ApplicationMasterService - [my.cluster.com] " +
    "ApplicationMasterService:addContainerRequests(ApplicationMasterService.java:125) - " +
  "Confirmed 1 containers running for BundledJarRunnable\n" +
  "java.lang.Throwable: logThrowable Exception\n" +
  "Caused by: java.lang.Throwable: logThrowableChild Exception\n" +
  "Caused by: java.lang.Throwable: logThrowableChild1 Exception\n" +
  "Caused by: java.lang.Throwable: Initial Exception\n";

  private static final String SHIM_LOADER_NAME = "dremio-shimloader-0.9.2-201703030219530674-3d10a7e.jar";
  private static final String SOME_JAR_TO_LOAD = "some-jar-to-load.jar";

  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  public static final DremioConfig dremioConfig = DremioConfig.create();

  private static File finalPath;
  private static File thirdrdPartyDir;

  @BeforeClass
  public static void beforeClass() throws Exception {
    DacDaemonYarnApplication.isTestingModeOn = true;
    finalPath = tempDir.newFolder("jars", "bundled");
    thirdrdPartyDir = tempDir.newFolder("jars", "3rdparty");
    File filePath = new File(finalPath, "dremio-daemon-bundle.jar");
    filePath.createNewFile();
    File shimFilePath = new File(finalPath, SHIM_LOADER_NAME);
    shimFilePath.createNewFile();
    File randomFilePath = new File(finalPath, "some-jar-to-load.jar");
    randomFilePath.createNewFile();
    File maprfsJar = new File(thirdrdPartyDir, "dremio-maprfs-shaded-5.1.0-mapr.jar");
    maprfsJar.createNewFile();
  }

  @AfterClass
  public static void afterClass() {
    DacDaemonYarnApplication.isTestingModeOn = false;
  }

  @Before
  public void setup() {
    properties.set(DremioConfig.PLUGINS_ROOT_PATH_PROPERTY, "./plugins");
  }

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test
  public void testYarnController() throws Exception {
    assumeNonMaprProfile();
    YarnController yarnController = new YarnController();
    YarnConfiguration yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");
    String jvmOptions = yarnController.prepareCommandOptions(yarnConfiguration, getProperties());
    logger.info("JVMOptions: {}", jvmOptions);

    assertTrue(jvmOptions.contains(" -Dpaths.dist=pdfs:///data/mydata/pdfs"));
    assertTrue(jvmOptions.contains(" -Xmx4096"));
    assertTrue(jvmOptions.contains(" -XX:MaxDirectMemorySize=5120m"));
    assertTrue(jvmOptions.contains(" -XX:+PrintClassHistogramAfterFullGC"));
    assertTrue(jvmOptions.contains(" -Xms4096m"));
    assertTrue(jvmOptions.contains(" -XX:ThreadStackSize=512"));
    assertTrue(jvmOptions.contains(" -Dzookeeper.saslprovider=com.mapr.security.maprsasl.MaprSaslProvider"));
    assertTrue(jvmOptions.contains(" -Dzookeeper.client.sasl=false"));
    assertTrue(jvmOptions.contains(" -Dzookeeper.sasl.clientconfig=Client"));
    assertTrue(jvmOptions.contains(" -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf"));
    assertTrue(jvmOptions.contains(" -Dpaths.spilling=[maprfs:///var/mapr/local/${NM_HOST}/mapred/spill]"));
    assertFalse(jvmOptions.contains("JAVA_HOME"));
    assertTrue(jvmOptions.contains(" -DMAPR_IMPALA_RA_THROTTLE"));
    assertTrue(jvmOptions.contains(" -DMAPR_MAX_RA_STREAMS"));
    assertTrue(jvmOptions.contains(" -D" + DremioConfig.NETTY_REFLECTIONS_ACCESSIBLE + "=true"));
    assertTrue(jvmOptions.contains(" -D"+VM.DREMIO_CPU_AVAILABLE_PROPERTY + "=2"));

    DacDaemonYarnApplication.Environment myEnv = new DacDaemonYarnApplication.Environment() {
      @Override
      public String getEnv(String name) {
        return tempDir.getRoot().toString();
      }
    };

    DacDaemonYarnApplication dacDaemonApp = new DacDaemonYarnApplication(yarnController.dremioConfig,
      yarnConfiguration, myEnv);
    TwillSpecification twillSpec = dacDaemonApp.configure();
    assertEquals(DacDaemonYarnApplication.YARN_APPLICATION_NAME_DEFAULT, twillSpec.getName());
    Map<String, RuntimeSpecification> runnables = twillSpec.getRunnables();
    assertNotNull(runnables);
    assertEquals(1, runnables.size());

    RuntimeSpecification runnable = runnables.get(DacDaemonYarnApplication.YARN_RUNNABLE_NAME);
    assertNotNull(runnable);

    assertEquals(DacDaemonYarnApplication.YARN_RUNNABLE_NAME, runnable.getName());

    assertEquals(9216, runnable.getResourceSpecification().getMemorySize());
    assertEquals(2, runnable.getResourceSpecification().getVirtualCores());
    assertEquals(3, runnable.getResourceSpecification().getInstances());
  }

  private List<Property> getProperties() {
    List<Property> propertyList = new ArrayList<>();

    propertyList.add(new Property("java.security.auth.login.config", "/opt/mapr/conf/mapr.login.conf")
      .setType(PropertyType.JAVA_PROP));
    propertyList.add(new Property("zookeeper.client.sasl", "false"));
    propertyList.add(new Property("zookeeper.sasl.clientconfig", "Client"));
    propertyList.add(new Property("zookeeper.saslprovider", "com.mapr.security.maprsasl.MaprSaslProvider"));
    propertyList.add(new Property("-Xms4096m", "").setType(PropertyType.SYSTEM_PROP));
    propertyList.add(new Property("-XX:ThreadStackSize", "512").setType(PropertyType.SYSTEM_PROP));
    propertyList.add(new Property("paths.spilling", "[maprfs:///var/mapr/local/${NM_HOST}/mapred/spill]"));
    propertyList.add(new Property("JAVA_HOME", "/abc/bcd").setType(PropertyType.ENV_VAR));
    return propertyList;
  }

  /**
   * DREMIO_GC_OPTS,   Property added from UI,    Expected in JVM opts,  Should not be in JVM opts
   * ----------------, ------------------------,  ---------------------, -------------------------
   * empty               empty (i.e no GC option) empty                  -XX:+UseParallelGC or -XX:+UseG1GC
   * empty               -XX:+UseG1GC             -XX:+UseG1GC           -XX:+UseParallelGC
   * empty               -XX:+UseParallelGC       -XX:+UseParallelGC     -XX:+UseG1GC
   * -XX:+UseG1GC        empty                    -XX:+UseG1GC           -XX:+UseParallelGC
   * -XX:+UseG1GC        -XX:+UseG1GC             -XX:+UseG1GC           -XX:+UseParallelGC
   * -XX:+UseG1GC        -XX:+UseParallelGC       -XX:+UseParallelGC     -XX:+UseG1GC
   * -XX:+UseParallelGC  empty                    -XX:+UseParallelGC     -XX:+UseG1GC
   * -XX:+UseParallelGC  -XX:+UseG1GC             -XX:+UseG1GC           -XX:+UseParallelGC
   * -XX:+UseParallelGC  -XX:+UseParallelGC       -XX:+UseParallelGC     -XX:+UseG1GC
   * @throws Exception
   */
  @Test
  public void testDremioGCOptionsScenarios() throws Exception {
    testDremioGCOptionsScenario("", "", "", "-XX:+UseParallelGC");
    testDremioGCOptionsScenario("", "", "", "-XX:+UseG1GC");
    testDremioGCOptionsScenario("", "-XX:+UseG1GC", "-XX:+UseG1GC", "-XX:+UseParallelGC");
    testDremioGCOptionsScenario("", "-XX:+UseParallelGC", "-XX:+UseParallelGC", "-XX:+UseG1GC");
    testDremioGCOptionsScenario("-XX:+UseG1GC", "", "-XX:+UseG1GC", "-XX:+UseParallelGC");
    testDremioGCOptionsScenario("-XX:+UseG1GC", "-XX:+UseG1GC", "-XX:+UseG1GC", "-XX:+UseParallelGC");
    testDremioGCOptionsScenario("-XX:+UseG1GC", "-XX:+UseParallelGC", "-XX:+UseParallelGC", "-XX:+UseG1GC");
    testDremioGCOptionsScenario("-XX:+UseParallelGC", "", "-XX:+UseParallelGC", "-XX:+UseG1GC");
    testDremioGCOptionsScenario("-XX:+UseParallelGC", "-XX:+UseG1GC", "-XX:+UseG1GC", "-XX:+UseParallelGC");
    testDremioGCOptionsScenario("-XX:+UseParallelGC", "-XX:+UseParallelGC", "-XX:+UseParallelGC", "-XX:+UseG1GC");
  }

  private void testDremioGCOptionsScenario(String dremioGCOpts,
                                           String systemProperty,
                                           String expectedInJvmOptions,
                                           String shouldNotBeInJvmOptions) throws Exception {
    assumeNonMaprProfile();
    YarnController yarnController = new YarnController() {
      @Override
      protected String getDremioGCOpts() {
        return dremioGCOpts;
      }
    };

    YarnConfiguration yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");
    List<Property> propertyList = getProperties();
    if (systemProperty != null && !systemProperty.isEmpty()) {
      propertyList.add(new Property(systemProperty, "").setType(PropertyType.SYSTEM_PROP));
    }
    String jvmOptions = yarnController.prepareCommandOptions(yarnConfiguration, propertyList);
    logger.info("JVMOptions: {}", jvmOptions);
    if (expectedInJvmOptions != null && !expectedInJvmOptions.isEmpty()) {
      assertTrue(jvmOptions.contains(expectedInJvmOptions));
    }
    if (shouldNotBeInJvmOptions != null && !shouldNotBeInJvmOptions.isEmpty()) {
      assertTrue(!jvmOptions.contains(shouldNotBeInJvmOptions));
    }
  }

  @Test
  public void testDeploymentPolicy() throws Exception {
    assumeNonMaprProfile();
    YarnConfiguration yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");

    yarnConfiguration.set(DacDaemonYarnApplication.DEPLOYMENT_POLICY, "default");

    DacDaemonYarnApplication.Environment myEnv = new DacDaemonYarnApplication.Environment() {
      @Override
      public String getEnv(String name) {
        return tempDir.getRoot().toString();
      }
    };

    DacDaemonYarnApplication dacDaemonApp = new DacDaemonYarnApplication(dremioConfig,
      yarnConfiguration, myEnv);
    TwillSpecification twillSpec = dacDaemonApp.configure();

    assertEquals(1, twillSpec.getPlacementPolicies().size());
    assertEquals(TwillSpecification.PlacementPolicy.Type.DEFAULT, twillSpec.getPlacementPolicies().get(0).getType());

    yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");
    yarnConfiguration.set(DacDaemonYarnApplication.DEPLOYMENT_POLICY, "distributed");

    DacDaemonYarnApplication dacDaemonApp1 = new DacDaemonYarnApplication(dremioConfig,
      yarnConfiguration, myEnv);
    TwillSpecification twillSpec1 = dacDaemonApp1.configure();

    assertEquals(1, twillSpec1.getPlacementPolicies().size());
    assertEquals(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, twillSpec1.getPlacementPolicies().get(0).getType());

    yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");
    yarnConfiguration.set(DacDaemonYarnApplication.DEPLOYMENT_POLICY, "abcd");

    DacDaemonYarnApplication dacDaemonApp2 = new DacDaemonYarnApplication(dremioConfig,
      yarnConfiguration, myEnv);
    TwillSpecification twillSpec2 = dacDaemonApp2.configure();

    assertEquals(1, twillSpec2.getPlacementPolicies().size());
    assertEquals(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, twillSpec2.getPlacementPolicies().get(0).getType());

    yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");

    DacDaemonYarnApplication dacDaemonApp3 = new DacDaemonYarnApplication(dremioConfig,
      yarnConfiguration, myEnv);
    TwillSpecification twillSpec3 = dacDaemonApp3.configure();

    assertEquals(1, twillSpec3.getPlacementPolicies().size());
    assertEquals(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, twillSpec3.getPlacementPolicies().get(0).getType());

  }

  @Test
  public void testRegexInPath() throws Exception {
    assumeNonMaprProfile();
    DacDaemonYarnApplication.Environment myEnv = new DacDaemonYarnApplication.Environment() {
      @Override
      public String getEnv(String name) {
        return tempDir.getRoot().toString();
      }
    };

    System.setProperty("provisioning.yarn.classpath", "["+ tempDir.getRoot().toString() +
      "/jars/bundled/" + SOME_JAR_TO_LOAD + "]");
    YarnConfiguration yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");
    yarnConfiguration.set(YarnDefaultsConfigurator.CLASSPATH_JARS, YarnDefaultsConfigurator.MapRYarnDefaults.getAppClassPath());
    DacDaemonYarnApplication dacDaemonApp = new DacDaemonYarnApplication(DremioConfig.create(),
      yarnConfiguration, myEnv);

    List<String> names = dacDaemonApp.getJarNames();
    assertFalse(names.isEmpty());
    assertEquals(3, names.size());

    String flatNames = names.toString();
    assertTrue(flatNames.contains(SHIM_LOADER_NAME));
    assertTrue(flatNames.contains("dremio-maprfs-shaded-5.1.0-mapr.jar"));
    assertTrue(flatNames.contains(SOME_JAR_TO_LOAD));
    System.clearProperty("provisioning.yarn.classpath");
  }

  @Test
  public void testRegexInPathNonMapR() throws Exception {
    assumeNonMaprProfile();
    DacDaemonYarnApplication.Environment myEnv = new DacDaemonYarnApplication.Environment() {
      @Override
      public String getEnv(String name) {
        return tempDir.getRoot().toString();
      }
    };

    System.setProperty("provisioning.yarn.classpath", "["+ tempDir.getRoot().toString() +
      "/jars/bundled/" + SOME_JAR_TO_LOAD + "]");
    YarnConfiguration yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");
    DacDaemonYarnApplication dacDaemonApp = new DacDaemonYarnApplication(DremioConfig.create(),
      yarnConfiguration, myEnv);

    List<String> names = dacDaemonApp.getJarNames();
    assertFalse(names.isEmpty());
    assertEquals(1, names.size());

    String flatNames = names.toString();
    assertTrue(flatNames.contains(SOME_JAR_TO_LOAD));
    System.clearProperty("provisioning.yarn.classpath");
  }

  @Test
  public void testEmptyYarnClasspath() throws Exception {
    assumeNonMaprProfile();
    DacDaemonYarnApplication.Environment myEnv = new DacDaemonYarnApplication.Environment() {
      @Override
      public String getEnv(String name) {
        return tempDir.getRoot().toString();
      }
    };

    YarnConfiguration yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");
    yarnConfiguration.set(YarnDefaultsConfigurator.CLASSPATH_JARS, YarnDefaultsConfigurator.MapRYarnDefaults.getAppClassPath());
    DacDaemonYarnApplication dacDaemonApp = new DacDaemonYarnApplication(DremioConfig.create(),
      yarnConfiguration, myEnv);

    List<String> names = dacDaemonApp.getJarNames();
    assertFalse(names.isEmpty());
    assertEquals(2, names.size());

    String flatNames = names.toString();
    assertTrue(flatNames.contains(SHIM_LOADER_NAME));
    assertTrue(flatNames.contains("dremio-maprfs-shaded-5.1.0-mapr.jar"));
  }

  @Test
  public void testNonMapRYarnClasspath() throws Exception {
    assumeNonMaprProfile();
    DacDaemonYarnApplication.Environment myEnv = new DacDaemonYarnApplication.Environment() {
      @Override
      public String getEnv(String name) {
        return tempDir.getRoot().toString();
      }
    };
    File shimFilePath = new File(finalPath, SHIM_LOADER_NAME);
    File maprfsJar = new File(thirdrdPartyDir, "dremio-maprfs-shaded-5.1.0-mapr.jar");

    try {
      shimFilePath.delete();
      maprfsJar.delete();

      YarnConfiguration yarnConfiguration = createYarnConfig("resource-manager", "hdfs://name-node:8020");
      DacDaemonYarnApplication dacDaemonApp = new DacDaemonYarnApplication(DremioConfig.create(),
        yarnConfiguration, myEnv);

      List<String> names = dacDaemonApp.getJarNames();
      assertTrue(names.isEmpty());
    } finally {
      shimFilePath.createNewFile();
      maprfsJar.createNewFile();
    }
  }

  @Test
  public void testYarnTwillController() throws Exception {
    YarnTwillLogHandler logHandler = new YarnTwillLogHandler();

    final Throwable throwable = new Throwable("Initial Exception");
    final LogThrowable logThrowableChild2 = new LogThrowable() {
      @Override
      public String getClassName() {
        return throwable.getClass().getName();
      }

      @Override
      public String getMessage() {
        return throwable.getMessage();
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        return throwable.getStackTrace();
      }

      @Override
      public LogThrowable getCause() {
        return null;
      }
    };

    final LogThrowable logThrowableChild1 = new LogThrowable() {
      @Override
      public String getClassName() {
        return "ABCD";
      }

      @Override
      public String getMessage() {
        return "logThrowableChild1 Exception";
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        return new StackTraceElement[0];
      }

      @Override
      public LogThrowable getCause() {
        return logThrowableChild2;
      }
    };

    final LogThrowable logThrowableChild = new LogThrowable() {
      @Override
      public String getClassName() {
        return "BCDEF";
      }

      @Override
      public String getMessage() {
        return "logThrowableChild Exception";
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        return new StackTraceElement[0];
      }

      @Override
      public LogThrowable getCause() {
        return logThrowableChild1;
      }
    };

    final LogThrowable logThrowable = new LogThrowable() {
      @Override
      public String getClassName() {
        return "CDEFG";
      }

      @Override
      public String getMessage() {
        return "logThrowable Exception";
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        return new StackTraceElement[0];
      }

      @Override
      public LogThrowable getCause() {
        return logThrowableChild;
      }
    };

    LogEntry logEntry = new LogEntry() {
      @Override
      public String getLoggerName() {
        return "org.apache.twill.internal.appmaster.ApplicationMasterService";
      }

      @Override
      public String getHost() {
        return "my.cluster.com";
      }

      @Override
      public long getTimestamp() {
        return System.currentTimeMillis() - 1000*3600*24;
      }

      @Override
      public Level getLogLevel() {
        return Level.INFO;
      }

      @Override
      public String getSourceClassName() {
        return "ApplicationMasterService";
      }

      @Override
      public String getSourceMethodName() {
        return "addContainerRequests";
      }

      @Override
      public String getFileName() {
        return "ApplicationMasterService.java";
      }

      @Override
      public int getLineNumber() {
        return 125;
      }

      @Override
      public String getThreadName() {
        return "run";
      }

      @Override
      public String getMessage() {
        return "Confirmed 1 containers running for BundledJarRunnable";
      }

      @Override
      public String getRunnableName() {
        return null;
      }

      @Override
      public LogThrowable getThrowable() {
        return logThrowable;
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        return new StackTraceElement[0];
      }
    };

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    PrintStream old = System.out;
    System.setOut(ps);
    logHandler.onLog(logEntry);
    // Put things back
    System.out.flush();
    System.setOut(old);
    String logEvent = baos.toString();
    assertTrue(logEvent.contains(LOG_STRING));
  }

  private YarnConfiguration createYarnConfig(String rmHost, String fsURL) {
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set(FS_DEFAULT_NAME_KEY, fsURL);
    yarnConfiguration.set(RM_HOSTNAME, rmHost);
    yarnConfiguration.set(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs");
    yarnConfiguration.set(DremioConfig.YARN_JVM_OPTIONS, " -Xmx4096");
    yarnConfiguration.set(DacDaemonYarnApplication.YARN_CONTAINER_COUNT, "3");
    yarnConfiguration.set(DacDaemonYarnApplication.YARN_MEMORY_ON_HEAP, "4096");
    yarnConfiguration.set(DacDaemonYarnApplication.YARN_MEMORY_OFF_HEAP, "5120");
    yarnConfiguration.set(DacDaemonYarnApplication.YARN_CPU, "2");

    return yarnConfiguration;
  }
}
