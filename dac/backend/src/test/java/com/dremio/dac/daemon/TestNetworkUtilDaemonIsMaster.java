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
package com.dremio.dac.daemon;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test NetworkUtil's addressResolvesToThisNode() method
 */
public class TestNetworkUtilDaemonIsMaster {

  @Test
  public void test127() throws UnknownHostException {
    Assert.assertTrue(NetworkUtil.addressResolvesToThisNode(InetAddress.getByName("127.0.0.1")));
  }

  @Test
  public void testLocalHost() throws UnknownHostException {
    Assert.assertTrue(NetworkUtil.addressResolvesToThisNode(InetAddress.getByName("localhost")));
  }

  @Test
  public void testLocalHostCanonical() throws UnknownHostException {
    Assert.assertTrue(NetworkUtil.addressResolvesToThisNode(InetAddress.getByName(InetAddress.getLocalHost().getCanonicalHostName())));
  }


  @Test
  public void testAnotherAddress() throws UnknownHostException {
    Assert.assertFalse(NetworkUtil.addressResolvesToThisNode(InetAddress.getByName("google.com")));
  }

  @Test
  public void testAnotherAddress2() throws UnknownHostException {
    Assert.assertFalse(NetworkUtil.addressResolvesToThisNode(InetAddress.getByName("172.50.1.1")));
  }


}
