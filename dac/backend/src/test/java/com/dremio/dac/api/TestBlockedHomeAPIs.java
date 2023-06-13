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
package com.dremio.dac.api;

import static com.dremio.options.OptionValue.OptionType.SYSTEM;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.options.OptionValue;
import com.dremio.service.namespace.file.proto.TextFileConfig;

/**
 * Tests for {@link com.dremio.dac.resource.HomeResource} when Home space is disabled
 */
public class TestBlockedHomeAPIs extends BaseTestServer {
  private static String HOME_API_PATH = "/home";

  private static boolean saveArsEnabled;

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
    saveArsEnabled = getSabotContext().getOptionManager().getOption(CatalogOptions.CATALOG_ARS_ENABLED);

    // Enable CATALOG_ARS_ENABLED
    getSabotContext().getOptionManager().setOption(
        OptionValue.createBoolean(SYSTEM, CatalogOptions.CATALOG_ARS_ENABLED.getOptionName(), true));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // Restore CATALOG_ARS_ENABLED
    getSabotContext().getOptionManager().setOption(
      OptionValue.createBoolean(SYSTEM, CatalogOptions.CATALOG_ARS_ENABLED.getOptionName(), saveArsEnabled));
  }

  @Test
  public void testBlockedHomeApis() {
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("file")).buildDelete());
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("folder")).buildDelete());
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio")).buildGet());
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("file_format")).buildGet());
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("file")).buildGet());
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("folder")).buildGet());
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("file_preview_unsaved")).buildPost(null));
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("file_preview")).buildPost(null));
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("folder")).buildPost(null));
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("upload_cancel")).buildPost(null));
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("upload_finish")).buildPost(null));
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("upload_start")).buildPost(null));
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getAPIv2().path(HOME_API_PATH).path("@dremio").path("file_format")).buildPut(Entity.entity(new TextFileConfig(), JSON)));
  }
}
