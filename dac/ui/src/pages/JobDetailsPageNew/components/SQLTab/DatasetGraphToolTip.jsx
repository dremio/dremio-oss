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
import { Tooltip } from "#oss/components/Tooltip";
import "./DatasetGraph.less";

const DatasetGraphToolTip = (props) => {
  const { item: { sql = "", dataSet: { datasetName = "" } = {} } = {} } =
    props || {};
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const TooltipInnerStyle = {
    width: "auto",
    maxWidth: "500px",
    background: "var(--fill--brand)",
    border: "1.5px solid orange",
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
  return (
    <div
      ref={datasetRef}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      data-qa="dataSetGraphToolTip"
    >
      {datasetName}
      {sql && (
        <Tooltip
          key="tooltip"
          target={() => (tooltipOpen ? datasetRef.current : null)}
          placement="bottom-start"
          type="custom"
          className="dataSetGraph__tooltip"
          tooltipArrowClass="textWithHelp__tooltipArrow --light"
          tooltipInnerStyle={TooltipInnerStyle}
        >
          <div className="dataSetGraph__sqlTooltipText">{sql}</div>
        </Tooltip>
      )}
    </div>
  );
};

export default DatasetGraphToolTip;
