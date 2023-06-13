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
import { ChangeEvent } from "react";
import { withRouter, InjectedRouter, WithRouterProps } from "react-router";
import { EntryV1 as Entry } from "@app/services/nessie/client";
import { Type } from "@app/types/nessie";

import * as classes from "./NamespaceSettings.module.less";

const handleSettingsClick = (
  e: ChangeEvent<HTMLInputElement>,
  router: InjectedRouter,
  location: any,
  entry: Entry
) => {
  const { push } = router;
  e.preventDefault();
  e.stopPropagation();
  const name = (entry && entry.name && entry.name.elements) || [""];
  const type = (entry && entry.type) || "";

  push({
    ...location,
    state: {
      modalTitle: name[name.length - 1],
      modal: "NessieDatasetSettingsModal",
      entityType: type,
      entityName: name,
    },
  });
};

const NamespaceSettings = ({
  entry,
  router,
  location,
}: {
  entry: Entry;
} & WithRouterProps) => {
  const validTypes = [Type.IcebergTable];
  const entryType = entry.type;
  if (entryType && !validTypes.includes(entryType)) return null;
  return (
    <div className={`action-wrap ${classes["settings"]}`}>
      <dremio-icon
        name="interface/settings"
        class="main-settings-btn"
        onClick={(e: ChangeEvent<HTMLInputElement>) =>
          handleSettingsClick(e, router, location, entry)
        }
      />
    </div>
  );
};

export default withRouter(NamespaceSettings);
