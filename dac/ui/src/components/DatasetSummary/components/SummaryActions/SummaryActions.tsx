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

import Immutable from "immutable";

import QueryDataset from "#oss/components/QueryDataset/QueryDataset";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { getSummaryActions } from "dyn-load/utils/summary-utils";

import * as classes from "./SummaryActions.module.less";

type SummaryActionProps = {
  dataset: Immutable.Map<any, any>;
  shouldRenderLineageButton: boolean;
  hideSqlEditorIcon?: boolean;
  hideGoToButton?: boolean;
};

const SummaryActions = ({
  dataset,
  shouldRenderLineageButton,
  hideSqlEditorIcon,
  hideGoToButton,
}: SummaryActionProps) => {
  const resourceId = dataset.getIn(["fullPath", 0]);
  const versionContext = getVersionContextFromId(dataset.get("entityId"));

  return (
    <>
      {!hideSqlEditorIcon && (
        <QueryDataset
          fullPath={dataset.get("fullPath")}
          resourceId={resourceId}
          isButton
          buttonClassName={classes["summary-actions__button"]}
          versionContext={versionContext}
        />
      )}
      {getSummaryActions({
        dataset: dataset,
        classes: classes,
        shouldRenderLineageButton: shouldRenderLineageButton,
        hideGoToButton: hideGoToButton,
      })}
    </>
  );
};

export default SummaryActions;
