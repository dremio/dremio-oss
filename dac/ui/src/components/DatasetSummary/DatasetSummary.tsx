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
import SummaryItemLabel from "./components/SummaryItemLabel/SummaryItemLabel";
import SummarySubHeader from "./components/SummarySubHeader/SummarySubHeader";
import SummaryStats from "./components/SummaryStats/SummaryStats";
import SummaryColumns from "./components/SummaryColumns/SummaryColumns";
import exploreUtils from "@app/utils/explore/exploreUtils";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import * as classes from "./DatasetSummary.module.less";

type DatasetSummaryProps = {
  title: string;
  dataset: Immutable.Map<string, any>;
  fullPath: string;
  disableActionButtons: boolean;
  location: Record<string, any>;
};

const DatasetSummary = ({
  dataset,
  fullPath,
  title,
  disableActionButtons,
  location,
}: DatasetSummaryProps) => {
  const datasetType = dataset.get("datasetType");
  const selfLink = dataset.getIn(["links", "query"]);
  const editLink = dataset.getIn(["links", "edit"]);
  const jobsLink = wrapBackendLink(dataset.getIn(["links", "jobs"]));
  const jobCount = dataset.get("jobCount");
  const descendantsCount = dataset.get("descendants");
  const fields = dataset.get("fields");
  const canAlter = dataset.getIn(["permissions", "canAlter"]);
  const fieldsCount = fields && fields.size;
  const resourceId = dataset.getIn(["fullPath", 0]);
  const isSqlEditorTab = exploreUtils.isSqlEditorTab(location);
  return (
    <div
      onClick={(e) => e.stopPropagation()}
      className={classes["dataset-summary-container"]}
    >
      <div className={classes["dataset-summary-top-section"]}>
        <SummaryItemLabel
          fullPath={fullPath}
          resourceId={resourceId}
          datasetType={datasetType}
          canAlter={canAlter}
          title={title}
          selfLink={selfLink}
          editLink={editLink}
          isSqlEditorTab={isSqlEditorTab}
          disableActionButtons={disableActionButtons}
        />
        <SummarySubHeader subTitle={fullPath} />
        <SummaryStats
          location={location}
          jobsLink={jobsLink}
          jobCount={jobCount}
          descendantsCount={descendantsCount}
        />
      </div>
      <SummaryColumns fields={fields} fieldsCount={fieldsCount} />
    </div>
  );
};

export default DatasetSummary;
