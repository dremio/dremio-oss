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
import { Tooltip } from "#oss/components/Tooltip";
import { getIconByEntityType } from "utils/iconUtils";
import DatasetSummaryOverlay from "components/Dataset/DatasetSummaryOverlay";
import { checkTypeToShowOverlay } from "utils/datasetUtils";
import { Popover } from "dremio-ui-lib/components";
import VersionContext from "dremio-ui-common/components/VersionContext.js";

import "./JobsContent.less";

const DatasetCell = ({ job, isContextMenuOpen }) => {
  const [tooltipOpen, setTooltipOpen] = useState(false);

  const TooltipInnerStyle = {
    width: "auto",
    maxWidth: "450px",
    background: "#F4FAFC", //DX-34369
    border: "1.5px solid var(--border--focused)",
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

  if (!datasetArray) {
    return null;
  }

  const datasetType = datasetArray.getIn([0, "datasetType"]);
  const versionContextObj = datasetArray.getIn([0, "versionContext"]);
  let versionContext;
  try {
    versionContext = JSON.parse(versionContextObj);
  } catch (e) {
    // nothing
  }

  const datasetTitle = datasetArray.getIn([0, "datasetName"]);

  const showOverlay =
    checkTypeToShowOverlay(datasetType) || !!versionContextObj;

  const renderNameAndIcon = () => {
    return (
      <div className="jobsContent-dataset">
        <span className="jobsContent-dataset__dataset">
          <div className="shrink-0">
            <dremio-icon
              name={getIconByEntityType(
                isInternalQuery ? datasetType : "OTHER",
                !!versionContext,
              )}
            ></dremio-icon>
          </div>
        </span>
        <div className="jobsContent-dataset__name">
          {datasetArray.getIn([0, "datasetName"])}
          {versionContext && (
            <span className="jobsContent-dataset__versionContext">
              {"("}
              <VersionContext versionContext={versionContext} />
              {")"}
            </span>
          )}
        </div>
      </div>
    );
  };

  return (
    <>
      <div
        className="jobsContent-dataset"
        ref={datasetRef}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        data-qa="dataSetCell"
      >
        {showOverlay && !isContextMenuOpen ? (
          <>
            <Popover
              role="tooltip"
              placement="right"
              delay={750}
              mode="hover"
              portal
              showArrow
              content={
                <DatasetSummaryOverlay
                  inheritedTitle={datasetTitle}
                  datasetType={datasetType}
                  fullPath={datasetArray.getIn([0, "datasetPathsList"])}
                  openWikiDrawer={() => {}}
                  showColumns
                  versionContext={versionContext}
                  hideMainActionButtons
                />
              }
            >
              {renderNameAndIcon()}
            </Popover>
          </>
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
                  <div className="shrink-0">
                    <dremio-icon
                      name={getIconByEntityType(datasetType)}
                    ></dremio-icon>
                  </div>
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
    </>
  );
};

DatasetCell.propTypes = {
  job: PropTypes.object,
  isContextMenuOpen: PropTypes.bool,
};

DatasetCell.defaultProps = {
  job: null,
};

export default DatasetCell;
