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
package com.dremio.exec.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos.UserProperties;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;
import java.io.IOException;
import org.junit.Test;

public class TestExceptionInjection extends BaseTestQuery {
  private static final String NO_THROW_FAIL = "Didn't throw expected exception";

  private static final UserSession session =
      UserSession.Builder.newBuilder()
          .withSessionOptionManager(
              new SessionOptionManagerImpl(nodes[0].getContext().getOptionValidatorListing()),
              nodes[0].getContext().getOptionManager())
          .withCredentials(
              UserBitShared.UserCredentials.newBuilder()
                  .setUserName(UserServiceTestImpl.TEST_USER_1)
                  .build())
          .withUserProperties(UserProperties.getDefaultInstance())
          .build();

  /**
   * Class whose methods we want to simulate runtime at run-time for testing purposes. The class
   * must have access to QueryId, UserSession and NodeEndpoint. For instance, these are accessible
   * from {@link com.dremio.exec.ops.QueryContext}.
   */
  private static class DummyClass {
    private static final ControlsInjector injector =
        ControlsInjectorFactory.getInjector(DummyClass.class);
    private final QueryContext context;

    public DummyClass(final QueryContext context) {
      this.context = context;
    }

    /**
     * Method that injects an unchecked exception with the given site description.
     *
     * @param desc the injection site description
     */
    public void descPassthroughMethod(final String desc) {
      // ... code ...

      // simulated unchecked exception
      injector.injectUnchecked(context.getExecutionControls(), desc);

      // ... code ...
    }

    public static final String THROWS_IOEXCEPTION = "<<throwsIOException>>";

    /**
     * Method that injects an IOException with a site description of THROWS_IOEXCEPTION.
     *
     * @throws IOException
     */
    public void throwsIOException() throws IOException {
      // ... code ...

      // simulated IOException
      injector.injectChecked(context.getExecutionControls(), THROWS_IOEXCEPTION, IOException.class);

      // ... code ...
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void noInjection() throws Exception {
    test("select * from information_schema.columns");
  }

  @SuppressWarnings("static-method")
  @Test
  public void emptyInjection() throws Exception {
    ControlsInjectionUtil.setControls(session, "{\"injections\":[]}");
    test("select * from information_schema.columns");
  }

  /**
   * Assert that DummyClass.descPassThroughMethod does indeed throw the expected exception.
   *
   * @param dummyClass the instance of DummyClass
   * @param exceptionClassName the expected exception
   * @param exceptionDesc the expected exception site description
   */
  private static void assertPassthroughThrows(
      final DummyClass dummyClass, final String exceptionClassName, final String exceptionDesc) {
    try {
      dummyClass.descPassthroughMethod(exceptionDesc);
      fail(NO_THROW_FAIL);
    } catch (Exception e) {
      assertEquals(exceptionClassName, e.getClass().getName());
      assertEquals(exceptionDesc, e.getMessage());
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void uncheckedInjection() {
    // set exceptions via a string
    final String exceptionDesc = "<<injected from descPassthroughMethod()>>";
    final String exceptionClassName = "java.lang.RuntimeException";
    final String jsonString =
        "{\"injections\":[{"
            + "\"type\":\"exception\","
            + "\"siteClass\":\"com.dremio.exec.testing.TestExceptionInjection$DummyClass\","
            + "\"desc\":\""
            + exceptionDesc
            + "\","
            + "\"nSkip\":0,"
            + "\"nFire\":1,"
            + "\"exceptionClass\":\""
            + exceptionClassName
            + "\""
            + "}]}";
    ControlsInjectionUtil.setControls(session, jsonString);

    final QueryContext context =
        new QueryContext(session, nodes[0].getContext(), QueryId.getDefaultInstance());

    // test that the exception gets thrown
    final DummyClass dummyClass = new DummyClass(context);
    assertPassthroughThrows(dummyClass, exceptionClassName, exceptionDesc);
    try {
      context.close();
    } catch (Exception e) {
      fail();
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void checkedInjection() {
    // set the injection via the parsing POJOs
    final String controls =
        Controls.newBuilder()
            .addException(DummyClass.class, DummyClass.THROWS_IOEXCEPTION, IOException.class, 0, 1)
            .build();
    ControlsInjectionUtil.setControls(session, controls);

    final QueryContext context =
        new QueryContext(session, nodes[0].getContext(), QueryId.getDefaultInstance());

    // test that the expected exception (checked) gets thrown
    final DummyClass dummyClass = new DummyClass(context);
    try {
      dummyClass.throwsIOException();
      fail(NO_THROW_FAIL);
    } catch (IOException e) {
      assertEquals(DummyClass.THROWS_IOEXCEPTION, e.getMessage());
    }
    try {
      context.close();
    } catch (Exception e) {
      fail();
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void skipAndLimit() {
    final String passthroughDesc = "<<injected from descPassthrough>>";
    final int nSkip = 7;
    final int nFire = 3;
    final Class<? extends Throwable> exceptionClass = RuntimeException.class;
    final String controls =
        Controls.newBuilder()
            .addException(DummyClass.class, passthroughDesc, exceptionClass, nSkip, nFire)
            .build();
    ControlsInjectionUtil.setControls(session, controls);

    final QueryContext context =
        new QueryContext(session, nodes[0].getContext(), QueryId.getDefaultInstance());

    final DummyClass dummyClass = new DummyClass(context);

    // these shouldn't throw
    for (int i = 0; i < nSkip; ++i) {
      dummyClass.descPassthroughMethod(passthroughDesc);
    }

    // these should throw
    for (int i = 0; i < nFire; ++i) {
      assertPassthroughThrows(dummyClass, exceptionClass.getName(), passthroughDesc);
    }

    // this shouldn't throw
    dummyClass.descPassthroughMethod(passthroughDesc);
    try {
      context.close();
    } catch (Exception e) {
      fail();
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void injectionOnSpecificBit() throws Exception {
    try (AutoCloseable ignore = () -> {}) {
      updateTestCluster(2, DEFAULT_SABOT_CONFIG);
      // Creating two nodes
      final SabotContext nodeContext1 = getSabotContext(0);
      final SabotContext nodeContext2 = getSabotContext(1);

      final UserSession session =
          UserSession.Builder.newBuilder()
              .withSessionOptionManager(
                  new SessionOptionManagerImpl(nodeContext1.getOptionValidatorListing()),
                  nodeContext1.getOptionManager())
              .withCredentials(
                  UserBitShared.UserCredentials.newBuilder()
                      .setUserName(UserServiceTestImpl.TEST_USER_1)
                      .build())
              .withUserProperties(UserProperties.getDefaultInstance())
              .build();

      final String passthroughDesc = "<<injected from descPassthrough>>";
      final int nSkip = 7;
      final int nFire = 3;
      final Class<? extends Throwable> exceptionClass = RuntimeException.class;
      // only node1's (address, port)
      final String controls =
          Controls.newBuilder()
              .addExceptionOnNode(
                  DummyClass.class,
                  passthroughDesc,
                  exceptionClass,
                  nodeContext1.getEndpoint(),
                  nSkip,
                  nFire)
              .build();

      ControlsInjectionUtil.setControls(session, controls);

      {
        final QueryContext queryContext1 =
            new QueryContext(session, nodeContext1, QueryId.getDefaultInstance());
        final DummyClass class1 = new DummyClass(queryContext1);

        // these shouldn't throw
        for (int i = 0; i < nSkip; ++i) {
          class1.descPassthroughMethod(passthroughDesc);
        }

        // these should throw
        for (int i = 0; i < nFire; ++i) {
          assertPassthroughThrows(class1, exceptionClass.getName(), passthroughDesc);
        }

        // this shouldn't throw
        class1.descPassthroughMethod(passthroughDesc);
        try {
          queryContext1.close();
        } catch (Exception e) {
          fail();
        }
      }
      {
        final QueryContext queryContext2 =
            new QueryContext(session, nodeContext2, QueryId.getDefaultInstance());
        final DummyClass class2 = new DummyClass(queryContext2);

        // these shouldn't throw
        for (int i = 0; i < nSkip; ++i) {
          class2.descPassthroughMethod(passthroughDesc);
        }

        // these shouldn't throw
        for (int i = 0; i < nFire; ++i) {
          class2.descPassthroughMethod(passthroughDesc);
        }

        // this shouldn't throw
        class2.descPassthroughMethod(passthroughDesc);
        try {
          queryContext2.close();
        } catch (Exception e) {
          fail();
        }
      }
    }
  }
}
