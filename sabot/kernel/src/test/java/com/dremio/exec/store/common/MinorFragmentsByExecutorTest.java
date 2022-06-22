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
package com.dremio.exec.store.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.net.HostAndPort;

/**
 * Test for {@link MinorFragmentsByExecutor}
 */
public class MinorFragmentsByExecutorTest {
  @Test
  public void testEqualsAndComparable_hostOnly() {
    MinorFragmentsByExecutor endpointsWithHost = new MinorFragmentsByExecutor(HostAndPort.fromHost("host"));
    MinorFragmentsByExecutor endpointsWithHost2 = new MinorFragmentsByExecutor(HostAndPort.fromHost("host"));
    assertEquals(endpointsWithHost2, endpointsWithHost);
    assertEquals(endpointsWithHost2.hashCode(), endpointsWithHost.hashCode());
    assertEquals(0, endpointsWithHost2.compareTo(endpointsWithHost));
  }

  @Test
  public void testEqualsAndComparable_hostAndPort() {
    MinorFragmentsByExecutor endpointsWithHostPort = new MinorFragmentsByExecutor(HostAndPort.fromParts("host", 12345));
    MinorFragmentsByExecutor endpointsWithHostPort2 = new MinorFragmentsByExecutor(HostAndPort.fromParts("host", 12345));
    assertEquals(endpointsWithHostPort2, endpointsWithHostPort);
    assertEquals(endpointsWithHostPort2.hashCode(), endpointsWithHostPort.hashCode());
    assertEquals(0, endpointsWithHostPort2.compareTo(endpointsWithHostPort));
  }

  @Test
  public void testNotEqualsAndComparable_hostAndPort_hostIsBigger() {
    MinorFragmentsByExecutor endpointsWithHostPort = new MinorFragmentsByExecutor(HostAndPort.fromParts("host", 12345));
    MinorFragmentsByExecutor endpointsWithHostPort2 = new MinorFragmentsByExecutor(HostAndPort.fromParts("host2", 12345));
    assertNotEquals(endpointsWithHostPort2, endpointsWithHostPort);
    assertNotEquals(endpointsWithHostPort2.hashCode(), endpointsWithHostPort.hashCode());
    assertTrue(endpointsWithHostPort2.compareTo(endpointsWithHostPort) > 0);
  }

  @Test
  public void testNotEqualsAndComparable_hostAndPort_portIsBigger() {
    MinorFragmentsByExecutor endpointsWithHostPort = new MinorFragmentsByExecutor(HostAndPort.fromParts("host", 12345));
    MinorFragmentsByExecutor endpointsWithHostPort2 = new MinorFragmentsByExecutor(HostAndPort.fromParts("host", 23456));
    assertNotEquals(endpointsWithHostPort2, endpointsWithHostPort);
    assertNotEquals(endpointsWithHostPort2.hashCode(), endpointsWithHostPort.hashCode());
    assertTrue(endpointsWithHostPort2.compareTo(endpointsWithHostPort) > 0);
  }

  @Test
  public void testNotEqualsButComparable_hostAndPort_noPortInFirst() {
    MinorFragmentsByExecutor endpointsWithHost = new MinorFragmentsByExecutor(HostAndPort.fromHost("host"));
    MinorFragmentsByExecutor endpointsWithHostPort = new MinorFragmentsByExecutor(HostAndPort.fromParts("host", 12345));
    assertNotEquals(endpointsWithHostPort, endpointsWithHost);
    assertNotEquals(endpointsWithHostPort.hashCode(), endpointsWithHost.hashCode());
    assertEquals(0, endpointsWithHostPort.compareTo(endpointsWithHost));
  }

  @Test
  public void testNotEqualsButComparable_internalStateChange() {
    MinorFragmentsByExecutor endpointsWithHostPort = new MinorFragmentsByExecutor(HostAndPort.fromParts("host", 12345));
    MinorFragmentsByExecutor endpointsWithHostPort2 = new MinorFragmentsByExecutor(HostAndPort.fromParts("host", 12345));

    endpointsWithHostPort.addMinorFragmentEndPoint(10);
    assertNotEquals(endpointsWithHostPort2, endpointsWithHostPort);
    assertNotEquals(endpointsWithHostPort2.hashCode(), endpointsWithHostPort.hashCode());
    assertEquals(0, endpointsWithHostPort2.compareTo(endpointsWithHostPort));
  }
}
