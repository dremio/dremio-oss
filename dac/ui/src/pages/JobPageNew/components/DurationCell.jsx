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
import Immutable from "immutable";
import { injectIntl } from "react-intl";

import { Tooltip } from "#oss/components/Tooltip";
import { Tooltip as DremioTooltip } from "dremio-ui-lib";
import DurationBreakdown from "#oss/pages/JobPageNew/components/DurationBreakdown";
import { getDuration } from "utils/jobListUtils";

import "./JobsContent.less";

const DurationCell = ({
  duration,
  durationDetails,
  isSpilled,
  isFromExplorePage,
  intl,
}) => {
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const durationRef = useRef(null);

  useEffect(() => {
    if (!isFromExplorePage) {
      const timer = setTimeout(() => setTooltipOpen(false), 3000);
      return () => clearTimeout(timer);
    }
  }, [tooltipOpen]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleMouseEnter = () => {
    if (!isFromExplorePage) {
      setTooltipOpen(true);
    }
  };

  const handleMouseLeave = () => {
    setTooltipOpen(false);
  };

  return (
    <div
      data-qa="durationCell"
      ref={durationRef}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      <div className="jobsContent__twoColumnWrapper jobsContent__numericContent dremio-typography-tabular-numeric">
        <div className="jobsContent__durationSpill">
          <span>{duration}</span>
          <span>
            {isSpilled && (
              <DremioTooltip
                title={intl.formatMessage({ id: "Job.SpilledHover" })}
              >
                <dremio-icon
                  name="interface/disk-spill"
                  class="jobsContent__spillIcon"
                />
              </DremioTooltip>
            )}
          </span>
        </div>
      </div>
      <Tooltip
        key="tooltip"
        target={() => (tooltipOpen ? durationRef.current : null)}
        placement="bottom-start"
        type="custom"
        className="jobsContent__tooltip"
        tooltipInnerStyle={styles.tooltipInnerStyle}
        tooltipArrowClass="textWithHelp__tooltipArrow --light"
      >
        {/* TODO: props values need to be populated from API response */}
        <DurationBreakdown
          pending={getDuration(durationDetails, "PENDING")}
          metadataRetrival={getDuration(durationDetails, "METADATA_RETRIEVAL")}
          planning={getDuration(durationDetails, "PLANNING")}
          engineStart={getDuration(durationDetails, "ENGINE_START")}
          queued={getDuration(durationDetails, "QUEUED")}
          executionPlanning={getDuration(durationDetails, "EXECUTION_PLANNING")}
          starting={getDuration(durationDetails, "STARTING")}
          running={getDuration(durationDetails, "RUNNING")}
        />
      </Tooltip>
    </div>
  );
};

DurationCell.propTypes = {
  duration: PropTypes.string,
  durationDetails: PropTypes.instanceOf(Immutable.List),
  isSpilled: PropTypes.bool,
  intl: PropTypes.object.isRequired,
  isFromExplorePage: PropTypes.bool,
};

DurationCell.defaultProps = {
  duration: "",
  durationDetails: Immutable.List(),
  isSpilled: false,
};

const styles = {
  tooltipInnerStyle: {
    width: "425px",
    maxWidth: "533px",
    whiteSpace: "nowrap",
    background: "#F4FAFC", //DX-34369
    border: "1.5px solid var(--color--brand--300)",
    padding: "2px 18px 18px 18px",
  },
  iconStyle: {
    height: "25px",
  },
};

export default injectIntl(DurationCell);
