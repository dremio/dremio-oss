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

import static com.dremio.exec.ExecConstants.NODE_CONTROLS_VALIDATOR;
import static com.dremio.exec.ExecConstants.NODE_CONTROL_INJECTIONS;
import static org.junit.Assert.fail;

import java.util.List;

import com.dremio.exec.client.DremioClient;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcException;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Static methods for constructing exception and pause injections for testing purposes.
 */
public class ControlsInjectionUtil {
  /**
   * Constructor. Prevent instantiation of static utility class.
   */
  private ControlsInjectionUtil() {
  }

  public static void setSessionOption(final DremioClient dremioClient, final String option, final String value) {
    try {
      final List<QueryDataBatch> results = dremioClient.runQuery(
        UserBitShared.QueryType.SQL, String.format("ALTER session SET \"%s\" = %s",
          option, value));
      for (final QueryDataBatch data : results) {
        data.release();
      }
    } catch (final RpcException e) {
      fail("Could not set option: " + e.toString());
    }
  }

  public static void setControls(final DremioClient dremioClient, final String controls) {
    validateControlsString(controls);
    setSessionOption(dremioClient, NODE_CONTROL_INJECTIONS, "'" + controls + "'");
  }

  public static void setControls(final UserSession session, final String controls) {
    validateControlsString(controls);
    final OptionValue opValue = OptionValue.createString(OptionValue.OptionType.SESSION,
      NODE_CONTROL_INJECTIONS, controls);

    final OptionManager options = session.getOptions();
    try {
      NODE_CONTROLS_VALIDATOR.validate(opValue);
      options.setOption(opValue);
    } catch (final Exception e) {
      fail("Could not set controls options: " + e.getMessage());
    }
    session.incrementQueryCount();
  }

  public static void validateControlsString(final String controls) {
    try {
      ExecutionControls.validateControlsString(controls);
    } catch (final Exception e) {
      fail("Could not validate controls JSON: " + e.getMessage());
    }
  }

  /**
   * Create a single exception injection. Note this format is not directly accepted by the injection mechanism. Use
   * the {@link Controls} to build exceptions.
   */
  public static String createException(final Class<?> siteClass, final String desc, final int nSkip,
                                       final int nFire, final Class<? extends Throwable> exceptionClass) {
    final String siteClassName = siteClass.getName();
    final String exceptionClassName = exceptionClass.getName();
    return "{ \"type\":\"exception\","
      + "\"siteClass\":\"" + siteClassName + "\","
      + "\"desc\":\"" + desc + "\","
      + "\"nSkip\":" + nSkip + ","
      + "\"nFire\":" + nFire + ","
      + "\"exceptionClass\":\"" + exceptionClassName + "\"}";
  }

  /**
   * Create a single exception injection on a specific bit. Note this format is not directly accepted by the injection
   * mechanism. Use the {@link Controls} to build exceptions.
   */
  public static String createExceptionOnNode(final Class<?> siteClass, final String desc, final int nSkip,
                                            final int nFire, final Class<? extends Throwable> exceptionClass,
                                            final NodeEndpoint endpoint) {
    final String siteClassName = siteClass.getName();
    final String exceptionClassName = exceptionClass.getName();
    return "{ \"type\":\"exception\","
      + "\"siteClass\":\"" + siteClassName + "\","
      + "\"desc\":\"" + desc + "\","
      + "\"nSkip\":" + nSkip + ","
      + "\"nFire\":" + nFire + ","
      + "\"exceptionClass\":\"" + exceptionClassName + "\","
      + "\"address\":\"" + endpoint.getAddress() + "\","
      + "\"port\":\"" + endpoint.getUserPort() + "\"}";
  }

  /**
   * Create a pause injection. Note this format is not directly accepted by the injection mechanism. Use the
   * {@link Controls} to build exceptions.
   */
  public static String createPause(final Class<?> siteClass, final String desc, final int nSkip) {
    return "{ \"type\" : \"pause\"," +
      "\"siteClass\" : \"" + siteClass.getName() + "\","
      + "\"desc\" : \"" + desc + "\","
      + "\"nSkip\" : " + nSkip + "}";
  }

  /**
   * Create a pause injection on a specific bit. Note this format is not directly accepted by the injection
   * mechanism. Use the {@link Controls} to build exceptions.
   */
  public static String createPauseOnNode(final Class<?> siteClass, final String desc, final int nSkip,
                                        final NodeEndpoint endpoint) {
    return "{ \"type\" : \"pause\"," +
      "\"siteClass\" : \"" + siteClass.getName() + "\","
      + "\"desc\" : \"" + desc + "\","
      + "\"nSkip\" : " + nSkip + ","
      + "\"address\":\"" + endpoint.getAddress() + "\","
      + "\"port\":\"" + endpoint.getUserPort() + "\"}";
  }

  /**
   * Create a latch injection. Note this format is not directly accepted by the injection mechanism. Use the
   * {@link Controls} to build exceptions.
   */
  public static String createLatch(final Class<?> siteClass, final String desc) {
    return "{ \"type\":\"latch\"," +
      "\"siteClass\":\"" + siteClass.getName() + "\","
      + "\"desc\":\"" + desc + "\"}";
  }

  /**
   * Clears all the controls.
   */
  public static void clearControls(final DremioClient client) {
    setControls(client, ExecutionControls.DEFAULT_CONTROLS);
  }
}
