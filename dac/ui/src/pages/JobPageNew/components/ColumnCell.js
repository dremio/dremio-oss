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
import classNames from "classnames";
import { Tooltip } from "@app/components/Tooltip";
import "./JobsContent.less";

const ColumnCell = ({ data, isNumeric }) => {
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const [isTruncated, setIsTruncated] = useState(false);
  const cellRef = useRef(null);
  const TooltipInnerStyle = {
    width: "auto",
    maxWidth: "33.31rem",
    background: "#F4FAFC", //DX-34369
    border: "1.5px solid #43B8C9",
  };

  const handleMouseEnter = () => {
    if (isTruncated) {
      setTooltipOpen(true);
    }
  };

  useEffect(() => {
    const hasOverflowingDiv =
      cellRef.current.offsetWidth < cellRef.current.scrollWidth;
    setIsTruncated(hasOverflowingDiv);
  }, []);

  const handleMouseLeave = () => {
    setTooltipOpen(false);
  };
  return (
    <div
      ref={cellRef}
      data-testid="testColumnCell"
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      data-qa="columnCell"
      className={classNames({
        jobsContent__noDataCell: !data,
      })}
    >
      <div
        className={classNames(
          { jobsContent__columnCell: isTruncated },
          {
            jobsContent__numericContent: isNumeric,
            "dremio-typography-tabular-numeric": isNumeric,
          }
        )}
      >
        {data}
      </div>
      {tooltipOpen && (
        <Tooltip
          key="tooltip"
          target={() => (tooltipOpen ? cellRef.current : null)}
          placement="bottom-start"
          type="custom"
          className="jobsContent__tooltip"
          tooltipArrowClass="textWithHelp__tooltipArrow --light"
          tooltipInnerStyle={TooltipInnerStyle}
        >
          <div className="jobsContent__columnTooltipText">{data}</div>
        </Tooltip>
      )}
    </div>
  );
};

ColumnCell.propTypes = {
  data: PropTypes.string,
  isNumeric: PropTypes.bool,
};

ColumnCell.defaultProps = {
  data: "",
};

export default ColumnCell;
