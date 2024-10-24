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
package com.dremio.dac.cmd.upgrade;

import static java.util.Collections.emptyList;

import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;

/**
 * The pre-23.1 Dremio users had to use Snowflake Community connector. Dremio 23.1 started to
 * include 1st-party Snowflake connector provided by Dremio. This connector has different ARP
 * definitions, so when a Snowflake source is created with a community connector — it can't be
 * deserialized with protobuf and fails with ProtobufException, and Dremio fails to start. For more
 * details see DX-59245. DX-60529 is to improve the UX when the <a
 * href="https://docs.dremio.com/software/deployment/standalone/standalone-install-upgrade/#2310-upgrade-notes">
 * upgrade instruction</a> was not followed, and the removal of the source is done programmatically.
 */
public class DeleteSnowflakeCommunitySource extends UpgradeTask {
  public DeleteSnowflakeCommunitySource() {
    super("Clean up after Snowflake community edition plugin", emptyList());
  }

  static final String taskUUID = "dcb07715-ebd9-432d-b948-601679b9577b";
  private static final String ANSI_RED_BACKGROUND = "\u001B[41m";
  private static final String ANSI_WHITE_TEXT = "\u001B[37m";
  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ACTION_REQUIRED_MESSAGE =
      ANSI_RED_BACKGROUND
          + ANSI_WHITE_TEXT
          + "[ACTION REQUIRED]\n\n"
          + "It appears that you had a Snowflake source created by the Snowflake Community connector. "
          + "The Community connector is not compatible with the Dremio Snowflake connector.\n"
          + "The source created by the Community connector was removed.\n\n"
          + "Please re-add your Snowflake source manually."
          + ANSI_RESET
          + "\n\n";

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    NamespaceServiceImpl namespaceService =
        new NamespaceServiceImpl(
            context.getKvStoreProvider(),
            new CatalogStatusEventsImpl(),
            CatalogEventMessagePublisherProvider.NO_OP);

    // Will detect incompatible Snowflake sources created with the community connector by catching
    // ProtobufException when attempting to read source configuration. Then we delete such
    // sources in the catch block.
    namespaceService.getSources().stream()
        .filter(s -> "SNOWFLAKE".equals(s.getType()))
        .forEach(
            snowflakeConfig -> {
              //noinspection CatchMayIgnoreException
              try {
                context.getConnectionReader().getConnectionConf(snowflakeConfig);
              } catch (Exception e) {
                if (e.getCause() instanceof io.protostuff.ProtobufException) {
                  System.out.print(ACTION_REQUIRED_MESSAGE);
                  try {
                    namespaceService.deleteSource(
                        snowflakeConfig.getKey(), snowflakeConfig.getTag());
                  } catch (NamespaceException ex) {
                    throw new RuntimeException(ex);
                  }
                }
              }
            });
  }
}
