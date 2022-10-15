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
import { useState, useRef, useEffect } from "react";
import PropTypes from "prop-types";

import { Tooltip } from "@app/components/Tooltip";
import FontIcon from "components/Icon/FontIcon";
import { getIconByEntityType } from "utils/iconUtils";
import DatasetSummaryOverlay from "components/Dataset/DatasetSummaryOverlay";
import { checkTypeToShowOverlay } from "utils/datasetUtils";
import { Tooltip as DremioTooltip } from "dremio-ui-lib";

import "./JobsContent.less";

const DatasetCell = ({ job }) => {
  const [tooltipOpen, setTooltipOpen] = useState(false);

  const TooltipInnerStyle = {
    width: "auto",
    maxWidth: "450px",
    background: "#F4FAFC", //DX-34369
    border: "1.5px solid #43B8C9",
    padding: "0px 8px 12px 8px",
  };

  useEffect(() => {
    const timer = setTimeout(() => setTooltipOpen(false), 3000);
    return () => clearTimeout(timer);
  }, [tooltipOpen]);

  const handleMouseEnter = () => {
    setTooltipOpen(true);
  };

  const handleMouseLeave = () => {
    setTooltipOpen(false);
  };

  const datasetRef = useRef(null);
  const datasetArray = job.get("queriedDatasets");
  const isInternalQuery =
    job.get("queryType") && job.get("queryType") === "UI_INITIAL_PREVIEW";
  const datasetType = datasetArray.getIn([0, "datasetType"]);

  if (!datasetArray) {
    return null;
  }
  const iconType = getIconByEntityType(isInternalQuery ? "OTHER" : datasetType);
  const datasetTitle = datasetArray.getIn([0, "datasetName"]);

  const showOverlay = checkTypeToShowOverlay(datasetType);

  const renderNameAndIcon = () => {
    return (
      <div className="jobsContent-dataset">
        <span className="jobsContent-dataset__dataset">
          <FontIcon
            type={iconType}
            iconStyle={{
              verticalAlign: "middle",
              flexShrink: 0,
            }}
          />
        </span>
        <div className="jobsContent-dataset__name">
          {datasetArray.getIn([0, "datasetName"])}
        </div>
      </div>
    );
  };

  return (
    <div
      className="jobsContent-dataset"
      ref={datasetRef}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      data-qa="dataSetCell"
    >
      {showOverlay ? (
        <DremioTooltip
          interactive
          type="richTooltip"
          enterDelay={1000}
          title={
            <DatasetSummaryOverlay
              inheritedTitle={datasetTitle}
              datasetType={datasetType}
              fullPath={datasetArray.getIn([0, "datasetPathsList"])}
            />
          }
        >
          {renderNameAndIcon()}
        </DremioTooltip>
      ) : (
        renderNameAndIcon()
      )}
      {!showOverlay && (
        <Tooltip
          key="tooltip"
          target={() => (tooltipOpen ? datasetRef.current : null)}
          placement="bottom-start"
          type="custom"
          className="jobsContent-dataset__tooltip"
          tooltipArrowClass="textWithHelp__tooltipArrow --light"
          tooltipInnerStyle={TooltipInnerStyle}
        >
          {datasetArray.map((dataset, index) => {
            const datasetName = dataset.get("datasetName");
            const datasetPath = dataset.get("datasetPath");
            const queryText = job.get("queryText");
            const description = job.get("description");
            const datasetDescription =
              !queryText || queryText === "NA" ? description : datasetPath;
            const datasetType = dataset.get("datasetType");
            return (
              <div
                key={`datasetCell-${index}`}
                className="jobsContent-dataset__tooltipWrapper"
              >
                <FontIcon
                  type={getIconByEntityType(datasetType)}
                  iconStyle={{
                    verticalAlign: "middle",
                    flexShrink: 0,
                  }}
                />
                <div className="jobsContent-dataset__nameWrapper">
                  <div className="jobsContent-dataset__label">
                    {datasetName}
                  </div>
                  <div className="jobsContent-dataset__path">
                    {datasetDescription}
                  </div>
                </div>
              </div>
            );
          })}
        </Tooltip>
      )}
    </div>
  );
};

DatasetCell.propTypes = {
  job: PropTypes.object,
};

DatasetCell.defaultProps = {
  job: null,
};

export default DatasetCell;
