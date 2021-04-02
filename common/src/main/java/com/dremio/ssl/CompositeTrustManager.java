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

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.handler.ssl.util.SimpleTrustManagerFactory;

/**
 * A TrustManager that checks if certificates validate against multiple sources.
 *
 * Note: This class subclasses and aggregates X509ExtendedTrustManager because Netty
 * uses different verification callbacks when it detects that an X509TrustsManager
 * is an X509ExtendedTrustManager.
 */
class CompositeTrustManager extends X509ExtendedTrustManager implements X509TrustManager {
  private final ImmutableList<X509ExtendedTrustManager> trustManagers;

  CompositeTrustManager(ImmutableList<X509ExtendedTrustManager> trustManagers) {
    this.trustManagers = Preconditions.checkNotNull(trustManagers);
  }

  @FunctionalInterface
  private interface CheckTrustedFunction {
    void apply(X509ExtendedTrustManager trustManager) throws CertificateException;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String authType) throws CertificateException {
    doTrustCheck(tm -> tm.checkClientTrusted(x509Certificates, authType));
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String authType) throws CertificateException {
    doTrustCheck(tm -> tm.checkServerTrusted(x509Certificates, authType));
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return trustManagers.stream()
      .flatMap(tm -> Arrays.stream(tm.getAcceptedIssuers()))
      .toArray(X509Certificate[]::new);
  }


  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
    doTrustCheck(tm -> tm.checkClientTrusted(x509Certificates, s, socket));
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
    doTrustCheck(tm -> tm.checkServerTrusted(x509Certificates, s, socket));
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
    doTrustCheck(tm -> tm.checkClientTrusted(x509Certificates, s, sslEngine));
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
    doTrustCheck(tm -> tm.checkServerTrusted(x509Certificates, s, sslEngine));
  }

  private void doTrustCheck(CheckTrustedFunction callback) throws CertificateException {
    final CertificateException wrapperException = new CertificateException("Failed to verify the certificate.");
    for (X509ExtendedTrustManager tm : trustManagers) {
      try {
        callback.apply(tm);
        return;
      } catch (CertificateException ex) {
        wrapperException.addSuppressed(ex);
      }
    }
    throw wrapperException;
  }

}
