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
package com.dremio.jdbc;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.utils.SqlUtils;

public class ITTestShadedJar {

  private static NodeClassLoader nodeLoader;
  private static URLClassLoader rootClassLoader;
  private static volatile String jdbcURL = null;

  private static URL getJdbcUrl() throws MalformedURLException {
    return new URL(
        String.format("%s../../target/dremio-jdbc-driver-%s.jar",
            ClassLoader.getSystemClassLoader().getResource("").toString(),
            System.getProperty("project.version")
            ));

  }

  /**
   * Helper method which creates a {@link Connection} using the JDBC driver. It is caller's responsibility to close
   * the connection
   * @return
   */
  private Connection createConnection() throws Exception {
    // print class path for debugging
    System.out.println("java.class.path:");
    System.out.println(System.getProperty("java.class.path"));

    final URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    method.setAccessible(true);
    method.invoke(loader, getJdbcUrl());

    Class<? extends Driver> clazz = loader.loadClass("com.dremio.jdbc.Driver").asSubclass(Driver.class);
    Driver driver = clazz.newInstance();
    return driver.connect(jdbcURL, null);
  }

  @Test
  public void testDatabaseVersion() throws Exception {
    try (Connection c = createConnection()) {
      DatabaseMetaData metadata = c.getMetaData();
      assertEquals("Dremio JDBC Driver", metadata.getDriverName());
      assertEquals("Dremio Server", metadata.getDatabaseProductName());
    }
  }

  @Test
  public void testIdentifierQuoting() throws Exception {
    String expQuote = "\"";
    if ("true".equalsIgnoreCase(System.getProperty(SqlUtils.LEGACY_USE_BACKTICKS))) {
      expQuote = "`";
    }
    try (Connection c = createConnection()) {
      assertEquals(expQuote, c.getMetaData().getIdentifierQuoteString());
    }
  }

  @Test
  public void executeJdbcAllQuery() throws Exception {

    // print class path for debugging
    System.out.println("java.class.path:");
    System.out.println(System.getProperty("java.class.path"));

    final URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    method.setAccessible(true);
    method.invoke(loader, getJdbcUrl());

    Class<?> clazz = loader.loadClass("com.dremio.jdbc.Driver");
    try {
      Driver driver = (Driver) clazz.newInstance();
      try (Connection c = driver.connect(jdbcURL, null)) {
        String path = Paths.get("").toAbsolutePath().toString() + "/src/test/resources/types.json";
        printQuery(c, "select * from dfs.`" + path + "`");
      }
    } catch (Exception ex) {
      throw ex;
    }

  }

  private static void printQuery(Connection c, String query) throws SQLException {
    try (Statement s = c.createStatement(); ResultSet result = s.executeQuery(query)) {
      while (result.next()) {
        final int columnCount = result.getMetaData().getColumnCount();
        for(int i = 1; i < columnCount+1; i++){
          System.out.print(result.getObject(i));
          System.out.print('\t');
        }
        System.out.println(result.getObject(1));
      }
    }
  }



  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    nodeLoader = new NodeClassLoader();
    rootClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    try {
    runWithLoader("NodeStartThread", nodeLoader);
    } catch (Exception e) {
      printClassesLoaded("root", rootClassLoader);
      throw e;
    }
  }

  @AfterClass
  public static void closeClient() throws Exception {
    runWithLoader("NodeStopThread", nodeLoader);
  }

  private static int getClassesLoadedCount(ClassLoader classLoader) {
    try {
      Field f = ClassLoader.class.getDeclaredField("classes");
      f.setAccessible(true);
      Vector<Class<?>> classes = (Vector<Class<?>>) f.get(classLoader);
      return classes.size();
    } catch (Exception e) {
      System.out.println("Failure while loading class count.");
      return -1;
    }
  }

  private static void printClassesLoaded(String prefix, ClassLoader classLoader) {
    try {
      Field f = ClassLoader.class.getDeclaredField("classes");
      f.setAccessible(true);
      Vector<Class<?>> classes = (Vector<Class<?>>) f.get(classLoader);
      for (Class<?> c : classes) {
        System.out.println(prefix + ": " + c.getName());
      }
    } catch (Exception e) {
      System.out.println("Failure while printing loaded classes.");
    }
  }

  private static void runWithLoader(String name, ClassLoader loader) throws Exception {
    Class<?> clazz = loader.loadClass(ITTestShadedJar.class.getName() + "$" + name);
    Object o = clazz.getDeclaredConstructors()[0].newInstance(loader);
    clazz.getMethod("go").invoke(o);
  }

  public abstract static class AbstractLoaderThread extends Thread {
    private Exception ex;
    protected final ClassLoader loader;

    public AbstractLoaderThread(ClassLoader loader) {
      this.setContextClassLoader(loader);
      this.loader = loader;
    }

    @Override
    public final void run() {
      try {
        internalRun();
      } catch (Exception e) {
        this.ex = e;
      }
    }

    protected abstract void internalRun() throws Exception;

    public void go() throws Exception {
      start();
      join();
      if (ex != null) {
        throw ex;
      }
    }
  }

  public static class NodeStartThread extends AbstractLoaderThread {

    public NodeStartThread(ClassLoader loader) {
      super(loader);
    }

    @Override
    protected void internalRun() throws Exception {
      Class<?> clazz = loader.loadClass("com.dremio.BaseTestQuery");
      clazz.getMethod("setupDefaultTestCluster").invoke(null);

      // loader.loadClass("com.dremio.exec.exception.SchemaChangeException");

      // execute a single query to make sure the node is fully up
      clazz.getMethod("testNoResult", String.class, new Object[] {}.getClass())
          .invoke(null, "select * from (VALUES 1)", new Object[] {});
      jdbcURL = (String) clazz.getMethod("getJDBCURL").invoke(null);
    }

  }

  public static class NodeStopThread extends AbstractLoaderThread {

    public NodeStopThread(ClassLoader loader) {
      super(loader);
    }

    @Override
    protected void internalRun() throws Exception {
      Class<?> clazz = loader.loadClass("com.dremio.BaseTestQuery");
      clazz.getMethod("closeClient").invoke(null);
    }
  }

}
