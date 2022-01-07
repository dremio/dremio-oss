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
package com.dremio.ssl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.function.Function;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.junit.Test;

import com.dremio.common.SuppressForbidden;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link CompositeTrustManager}
 */
public class TestCompositeTrustManager {

  /**
   * Helper implementation of X509ExtendedTrustManager that has the same behavior in every method
   * and also can be inspected to see if each method has been called once.
   */
  static abstract class SingleStateX509ExtendedTrustManager extends X509ExtendedTrustManager {
    boolean checkClientSocketCalled = false;
    boolean checkClientEngineCalled = false;
    boolean checkClientCalled = false;
    boolean checkServerSocketCalled = false;
    boolean checkServerEngineCalled = false;
    boolean checkServerCalled = false;

    abstract void doCheck() throws CertificateException;

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
      checkClientSocketCalled = true;
      doCheck();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
      checkServerSocketCalled = true;
      doCheck();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
      checkClientEngineCalled = true;
      doCheck();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
      checkServerEngineCalled = true;
      doCheck();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      checkClientCalled = true;
      doCheck();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      checkServerCalled = true;
      doCheck();
    }
  }

  @SuppressForbidden
  static class AlwaysSuccessTrustManager extends SingleStateX509ExtendedTrustManager {
    static final X509Certificate CERT1 = mock(X509Certificate.class);
    static final X509Certificate CERT2 = mock(X509Certificate.class);

    @Override
    void doCheck() {
      // Don't throw to indicate success.
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[] {CERT1, CERT2};
    }
  }

  @SuppressForbidden
  static class FailingTrustManager extends SingleStateX509ExtendedTrustManager {
    static final X509Certificate CERT1 = mock(X509Certificate.class);
    static final X509Certificate CERT2 = mock(X509Certificate.class);

    @Override
    void doCheck() throws CertificateException {
      // Always throw to indicate failure.
      throw new CertificateException();
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[] {CERT1, CERT2};
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullTrustManagers() {
    new CompositeTrustManager(null);
  }

  @Test
  public void testFactoryEmptyTrustManager() {
    final CompositeTrustManagerFactory factory = CompositeTrustManagerFactory.newBuilder()
      .addTrustStore(Collections.emptyList())
      .build();
    final TrustManager[] tms = factory.getTrustManagers();
    assertEquals(1, tms.length);
  }

  @Test
  public void testCreateByFactory() {
    final CompositeTrustManagerFactory factory = CompositeTrustManagerFactory.newBuilder()
      .addDefaultTrustStore()
      .build();
    final TrustManager[] tms = factory.getTrustManagers();
    assertEquals(1, tms.length);
    assertTrue(tms[0] instanceof X509ExtendedTrustManager);
  }

  @Test
  public void testAggregatesIssuers() {
    final CompositeTrustManager mgr =
      new CompositeTrustManager(ImmutableList.of(new FailingTrustManager(), new AlwaysSuccessTrustManager()));

    assertArrayEquals(new X509Certificate[]
      {FailingTrustManager.CERT1, FailingTrustManager.CERT2, AlwaysSuccessTrustManager.CERT1, AlwaysSuccessTrustManager.CERT2},
      mgr.getAcceptedIssuers());
  }

  @Test
  public void testFailManagerCheckClientCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null)),
      (tm -> tm.checkClientCalled),
      new FailingTrustManager());
  }

  @Test
  public void testFailManagerCheckClientSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (Socket) null)),
      (tm -> tm.checkClientSocketCalled),
      new FailingTrustManager());
  }

  @Test
  public void testFailManagerCheckClientEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkClientEngineCalled),
      new FailingTrustManager());
  }

  @Test
  public void testFailManagerCheckServerCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null)),
      (tm -> tm.checkServerCalled),
      new FailingTrustManager());
  }

  @Test
  public void testFailManagerCheckServerSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (Socket) null)),
      (tm -> tm.checkServerSocketCalled),
      new FailingTrustManager());
  }

  @Test
  public void testFailManagerCheckServerEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkServerEngineCalled),
      new FailingTrustManager());
  }

  @Test
  public void testSuccessManagerCheckClientCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null)),
      (tm -> tm.checkClientCalled),
      new AlwaysSuccessTrustManager());
  }

  @Test
  public void testSuccessManagerCheckClientSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (Socket) null)),
      (tm -> tm.checkClientSocketCalled),
      new AlwaysSuccessTrustManager());
  }

  @Test
  public void testSuccessManagerCheckClientEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkClientEngineCalled),
      new AlwaysSuccessTrustManager());
  }

  @Test
  public void testSuccessManagerCheckServerCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null)),
      (tm -> tm.checkServerCalled),
      new AlwaysSuccessTrustManager());
  }

  @Test
  public void testSuccessManagerCheckServerSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (Socket) null)),
      (tm -> tm.checkServerSocketCalled),
      new AlwaysSuccessTrustManager());
  }

  @Test
  public void testSuccessManagerCheckServerEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkServerEngineCalled),
      new AlwaysSuccessTrustManager());
  }

  @Test
  public void testFailFailSuccessManagerCheckClientCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null)),
      (tm -> tm.checkClientCalled),
      new FailingTrustManager(), new FailingTrustManager(), new AlwaysSuccessTrustManager());
  }

  @Test
  public void testFailFailSuccessManagerCheckClientSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (Socket) null)),
      (tm -> tm.checkClientSocketCalled),
      new FailingTrustManager(), new FailingTrustManager(), new AlwaysSuccessTrustManager());
  }

  @Test
  public void testFailFailSuccessManagerCheckClientEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkClientEngineCalled),
      new FailingTrustManager(), new FailingTrustManager(), new AlwaysSuccessTrustManager());
  }

  @Test
  public void testFailFailSuccessManagerCheckServerCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null)),
      (tm -> tm.checkServerCalled),
      new FailingTrustManager(), new FailingTrustManager(), new AlwaysSuccessTrustManager());
  }

  @Test
  public void testFailFailSuccessManagerCheckServerSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (Socket) null)),
      (tm -> tm.checkServerSocketCalled),
      new FailingTrustManager(), new FailingTrustManager(), new AlwaysSuccessTrustManager());
  }

  @Test
  public void testFailFailSuccessManagerCheckServerEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkServerEngineCalled),
      new FailingTrustManager(), new FailingTrustManager(), new AlwaysSuccessTrustManager());
  }

  @Test
  public void testSuccessFailManagerCheckClientCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null)),
      (tm -> tm.checkClientCalled),
      new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testSuccessFailManagerCheckClientSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (Socket) null)),
      (tm -> tm.checkClientSocketCalled),
      new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testSuccessFailManagerCheckClientEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkClientEngineCalled),
      new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testSuccessFailManagerCheckServerCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null)),
      (tm -> tm.checkServerCalled),
      new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testSuccessFailManagerCheckServerSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (Socket) null)),
      (tm -> tm.checkServerSocketCalled),
      new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testSuccessFailManagerCheckServerEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkServerEngineCalled),
      new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailSuccessFailManagerCheckClientCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null)),
      (tm -> tm.checkClientCalled),
      new FailingTrustManager(), new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailSuccessFailManagerCheckClientSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (Socket) null)),
      (tm -> tm.checkClientSocketCalled),
      new FailingTrustManager(), new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailSuccessFailManagerCheckClientEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkClientEngineCalled),
      new FailingTrustManager(), new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailSuccessFailManagerCheckServerCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null)),
      (tm -> tm.checkServerCalled),
      new FailingTrustManager(), new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailSuccessFailManagerCheckServerSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (Socket) null)),
      (tm -> tm.checkServerSocketCalled),
      new FailingTrustManager(), new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailSuccessFailManagerCheckServerEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkServerEngineCalled),
      new FailingTrustManager(), new AlwaysSuccessTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailFailManagerCheckClientCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null)),
      (tm -> tm.checkClientCalled),
      new FailingTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailFailManagerCheckClientSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (Socket) null)),
      (tm -> tm.checkClientSocketCalled),
      new FailingTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailFailManagerCheckClientEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkClientEngineCalled),
      new FailingTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailFailManagerCheckServerCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null)),
      (tm -> tm.checkServerCalled),
      new FailingTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailFailManagerCheckServerSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (Socket) null)),
      (tm -> tm.checkServerSocketCalled),
      new FailingTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testFailFailManagerCheckServerEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkServerEngineCalled),
      new FailingTrustManager(), new FailingTrustManager());
  }

  @Test
  public void testEmtpyManagerCheckClientCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null)),
      (tm -> tm.checkClientCalled));
  }

  @Test
  public void testEmptyManagerCheckClientSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (Socket) null)),
      (tm -> tm.checkClientSocketCalled));
  }

  @Test
  public void testEmptyManagerCheckClientEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkClientTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkClientEngineCalled));
  }

  @Test
  public void testEmptyManagerCheckServerCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null)),
      (tm -> tm.checkServerCalled));
  }

  @Test
  public void testEmptyManagerCheckServerSocketCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (Socket) null)),
      (tm -> tm.checkServerSocketCalled));
  }

  @Test
  public void testEmptyManagerCheckServerEngineCalled() {
    checkCompositeDelegation(
      (tm -> tm.checkServerTrusted(null, null, (SSLEngine) null)),
      (tm -> tm.checkServerEngineCalled));
  }

  /**
   * Creates a CompositeTrustManager composed of the given trustManagers in the same order
   * and verifies that delegation across each is done correctly (or short-circuited).
   * Also verifies if the overall call succeeds or fails depending on if there was at least one passing trust manager.
   */
  private void checkCompositeDelegation(TrustFunction trustFunction, Function<SingleStateX509ExtendedTrustManager, Boolean> checkDelegated,
                                        SingleStateX509ExtendedTrustManager... trustManagers) {
    boolean checksWereSuccessful;
    final CompositeTrustManager tm = new CompositeTrustManager(ImmutableList.copyOf(trustManagers));
    try {
      trustFunction.apply(tm);
      checksWereSuccessful = true;
    } catch (CertificateException ex) {
      checksWereSuccessful = false;
    }

    SingleStateX509ExtendedTrustManager prev = null;
    boolean hadAtLeastOneSuccessfulManager = false;
    for (SingleStateX509ExtendedTrustManager curr : trustManagers) {
      final boolean shouldHaveDelegatedToCurrent;
      shouldHaveDelegatedToCurrent =
        prev == null || prev instanceof FailingTrustManager;

      // Validate that the current trust manager was delegated to.
      // This should happen if it is either the first trust manager or the previous trust manager
      // was one that should fail its check.
      assertEquals(shouldHaveDelegatedToCurrent, checkDelegated.apply(curr));

      if (curr instanceof AlwaysSuccessTrustManager) {
        hadAtLeastOneSuccessfulManager = true;
      }
      prev = curr;
    }

    assertEquals(checksWereSuccessful, hadAtLeastOneSuccessfulManager);
  }

  @FunctionalInterface
  private interface TrustFunction {
    void apply(CompositeTrustManager mgr) throws CertificateException;
  }
}
