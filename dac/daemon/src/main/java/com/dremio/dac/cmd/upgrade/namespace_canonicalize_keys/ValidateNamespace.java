/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.cmd.upgrade.namespace_canonicalize_keys;

import java.util.Collection;

import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.service.namespace.NamespaceService;

/**
 * Fails the upgrade if any duplicate entries are found in the namespace
 */
public class ValidateNamespace extends UpgradeTask {

  public ValidateNamespace() {
    super("Detecting namespace duplicates", VERSION_106, VERSION_108);
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final NamespaceService namespaceService = new NamespaceServiceWithoutNormalization(context.getKVStoreProvider().get());
    RootNode root = Utils.buildNamespaceHierarchy(namespaceService);
    Collection<Node> duplicates = root.findDuplicates();

    if (!duplicates.isEmpty()) {
      System.out.printf("Found %d duplicate entries in the namespace%n", duplicates.size());
      for (Node node : duplicates) {
        System.out.println(node.toString(false));
      }

      throw new RuntimeException(String.format("Namespace contains %d duplicate entries, " +
        "make sure to remove those duplicates before upgrading", duplicates.size()));
    }
  }
}
