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
package com.dremio.services.nessie.proxy;

import static org.jboss.weld.environment.se.Weld.SHUTDOWN_HOOK_SYSTEM_PROPERTY;

import com.dremio.services.nessie.restjavax.converters.ContentKeyParamConverterProvider;
import com.dremio.services.nessie.restjavax.converters.NamespaceParamConverterProvider;
import com.dremio.services.nessie.restjavax.converters.ReferenceTypeParamConverterProvider;
import com.dremio.services.nessie.restjavax.exceptions.ConstraintViolationExceptionMapper;
import com.dremio.services.nessie.restjavax.exceptions.NessieExceptionMapper;
import com.dremio.services.nessie.restjavax.exceptions.NessieJaxRsJsonMappingExceptionMapper;
import com.dremio.services.nessie.restjavax.exceptions.NessieJaxRsJsonParseExceptionMapper;
import com.dremio.services.nessie.restjavax.exceptions.ValidationExceptionMapper;
import java.net.URI;
import javax.ws.rs.core.Application;
import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.projectnessie.client.ext.NessieClientResolver;

/**
 * A JUnit 5 extension that starts up Weld/JerseyTest. This is a copy of NessieJaxRsExtension from
 * OSS adjusted for the proxy rest resources.
 */
public class NessieProxyJaxRsExtension extends NessieClientResolver
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(NessieProxyJaxRsExtension.class);
  private final Class<?> clientProducer;

  public NessieProxyJaxRsExtension(final Class<?> clientProducer) {
    this.clientProducer = clientProducer;
  }

  @Override
  protected URI getBaseUri(ExtensionContext extensionContext) {
    EnvHolder env = extensionContext.getStore(NAMESPACE).get(EnvHolder.class, EnvHolder.class);
    if (env == null) {
      throw new ParameterResolutionException(
          "Nessie JaxRs env. is not initialized in " + extensionContext.getUniqueId());
    }
    return env.jerseyTest.target().getUri();
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    // Put EnvHolder into the top-most context handled by this exception. Nested contexts will reuse
    // the same value to minimize Jersey restarts. EnvHolder will initialize on first use and close
    // when its owner context is destroyed.
    // Note: we also use EnvHolder.class as a key to the map of stored values.
    extensionContext
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            EnvHolder.class,
            key -> {
              try {
                return new EnvHolder(clientProducer);
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    EnvHolder env = extensionContext.getStore(NAMESPACE).get(EnvHolder.class, EnvHolder.class);
    env.reset();
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    EnvHolder env = extensionContext.getStore(NAMESPACE).get(EnvHolder.class, EnvHolder.class);
    env.reset();
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return super.supportsParameter(parameterContext, extensionContext)
        || parameterContext.isAnnotated(ProxyUri.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (super.supportsParameter(parameterContext, extensionContext)) {
      return super.resolveParameter(parameterContext, extensionContext);
    }

    EnvHolder env = extensionContext.getStore(NAMESPACE).get(EnvHolder.class, EnvHolder.class);
    if (env == null) {
      throw new ParameterResolutionException(
          "Nessie JaxRs env. is not initialized in " + extensionContext.getUniqueId());
    }

    if (parameterContext.isAnnotated(ProxyUri.class)) {
      return env.jerseyTest.target().getUri();
    }

    throw new ParameterResolutionException(
        "Unsupported annotation on parameter "
            + parameterContext.getParameter()
            + " on "
            + parameterContext.getTarget());
  }

  private static class EnvHolder implements CloseableResource {
    private final Weld weld;
    private final JerseyTest jerseyTest;

    void reset() {}

    public EnvHolder(final Class<?> clientProducer) throws Exception {
      weld = new Weld();
      weld.addBeanClass(clientProducer);
      // Let Weld scan all the resources to discover injection points and dependencies
      weld.addPackages(true, ProxyConfigResource.class);
      // Inject external beans
      weld.addExtension(new NessieProxyExtension());
      weld.property(SHUTDOWN_HOOK_SYSTEM_PROPERTY, "false");
      weld.initialize();

      jerseyTest =
          new JerseyTest() {
            @Override
            protected Application configure() {
              ResourceConfig config = new ResourceConfig();
              config.register(ProxyV2ConfigResource.class);
              config.register(ProxyV2TreeResource.class);
              config.register(ProxyTreeResource.class);
              config.register(ProxyDiffResource.class);
              config.register(ProxyContentResource.class);
              config.register(ProxyConfigResource.class);
              config.register(ProxyNamespaceResource.class);
              config.register(ContentKeyParamConverterProvider.class);
              config.register(NamespaceParamConverterProvider.class);
              config.register(ReferenceTypeParamConverterProvider.class);
              config.register(ProxyExceptionMapper.class, 10);
              config.register(ProxyRuntimeExceptionMapper.class, 10);
              config.register(ConstraintViolationExceptionMapper.class, 10);
              config.register(ValidationExceptionMapper.class, 10);
              config.register(NessieExceptionMapper.class);
              config.register(NessieJaxRsJsonParseExceptionMapper.class, 10);
              config.register(NessieJaxRsJsonMappingExceptionMapper.class, 10);
              config.register(EncodingFilter.class);
              config.register(GZipEncoder.class);
              config.register(DeflateEncoder.class);

              // Use a dynamically allocated port, not a static default (80/443) or statically
              // configured port.
              set(TestProperties.CONTAINER_PORT, "0");

              return config;
            }
          };

      jerseyTest.setUp();
    }

    @Override
    public void close() throws Throwable {
      jerseyTest.tearDown();
      weld.shutdown();
    }
  }
}
