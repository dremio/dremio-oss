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

import ArcticDataset, { DatasetType } from "./ArcticDataset";

type DatasetSummaryOverlayProps = {
  datasetType: DatasetType;
  name: string;
  path: string;
  reference: string;
};

const DatasetSummaryOverlay = (props: DatasetSummaryOverlayProps) => {
  return (
    <div className="dataset-summary-overlay">
      <div className="dataset-summary-overlay__title">
        <ArcticDataset type={props.datasetType} />
        {props.name}
      </div>
      <div className="dataset-summary-overlay__secondary-content">
        {props.path}
      </div>
      <div className="dataset-summary-overlay__secondary-content">
        Ref: {/* @ts-ignore */}
        <dremio-icon
          name="vcs/branch"
          style={{ height: 16, width: 16, marginRight: 2 }}
        />
        {props.reference}
      </div>
    </div>
  );
};

export default DatasetSummaryOverlay;
