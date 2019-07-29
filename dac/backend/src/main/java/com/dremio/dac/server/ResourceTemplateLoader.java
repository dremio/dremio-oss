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
package com.dremio.dac.server;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.eclipse.jetty.util.resource.Resource;

import freemarker.cache.TemplateLoader;

/**
 * A FreeMarker template loader using the same logic as
 * Jetty {@code org.eclipse.jetty.servlet.DefaultServlet} to find
 * templates
 */
final class ResourceTemplateLoader implements TemplateLoader {
  private final Resource baseResource;

  public ResourceTemplateLoader(Resource baseResource) {
    this.baseResource = baseResource;
  }

  @Override
  public Object findTemplateSource(String name) throws IOException {
    Resource resource = baseResource.addPath(name);
    if (!resource.exists()) {
      return null;
    }
    return resource;
  }

  @Override
  public Reader getReader(Object templateSource, String encoding) throws IOException {
    return new InputStreamReader(((Resource) templateSource).getInputStream(), encoding);
  }

  @Override
  public long getLastModified(Object templateSource) {
    return ((Resource) templateSource).lastModified();
  }

  @Override
  public void closeTemplateSource(Object templateSource) throws IOException {
    ((Resource) templateSource).close();
  }
}
