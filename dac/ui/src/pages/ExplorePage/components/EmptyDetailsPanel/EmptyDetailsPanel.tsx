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

import { CollapsiblePanelWrapper } from "../CollapsiblePanelWrapper/CollapsiblePanelWrapper";

import * as classes from "../CollapsiblePanelWrapper/CollapsiblePanelWrapper.module.less";

type EmptityDetailsPanelProps = {
  datasetDetailsCollapsed: boolean;
  handleDatasetDetailsCollapse: () => void;
};

export const EmptyDetailsPanel = (props: EmptityDetailsPanelProps) => {
  return (
    <CollapsiblePanelWrapper
      panelContent={
        <div className={classes["empty-details-panel__empty"]}>
          <dremio-icon
            name="interface/meta"
            class={classes["empty-details-panel__details-icon__large"]}
          />
          <p className="text-semibold pt-3 pb-1">No dataset selected</p>
          <p>
            Hover on a dataset in the Data panel and click{" "}
            <dremio-icon
              name="interface/meta"
              class={classes["empty-details-panel__icon"]}
            />
          </p>
        </div>
      }
      headerIcon={
        <dremio-icon
          key="interface/meta"
          name="interface/meta"
          class={classes["empty-details-panel__icon"]}
        />
      }
      headerTitle="Dataset Details"
      handleDatasetDetailsCollapse={props.handleDatasetDetailsCollapse}
      datasetDetailsCollapsed={props.datasetDetailsCollapsed}
    />
  );
};
