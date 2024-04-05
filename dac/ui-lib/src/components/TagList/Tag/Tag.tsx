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

import React, { useState } from "react";
import { default as Tooltip } from "../../Tooltip";
import { cloneDeep } from "lodash";
import "./Tag.scss";

type TagTypes = {
  text?: string;
  daqa?: string;
  onRef?: any;
};

const Tag = ({ text, daqa, onRef }: TagTypes) => {
  const [showSingleTooltip, setShowSingleTooltip] = useState({});

  const onHoverSingleTag = (
    e: { currentTarget: { clientWidth: number; scrollWidth: number } },
    text: string | number,
  ) => {
    if (
      e.currentTarget &&
      e.currentTarget.clientWidth < e.currentTarget.scrollWidth
    ) {
      const tooltipArr = cloneDeep(showSingleTooltip);
      tooltipArr[text] = true;
      setShowSingleTooltip(cloneDeep(tooltipArr));
    }
  };

  const handleSingleRequestClose = (text: string | number) => {
    const tooltipArr = cloneDeep(showSingleTooltip);
    tooltipArr[text] = false;
    setShowSingleTooltip(cloneDeep(tooltipArr));
  };

  return (
    <Tooltip
      key={text + "tooltip"}
      open={
        Object.keys(showSingleTooltip).length > 0
          ? showSingleTooltip[text]
          : false
      }
      title={text}
    >
      <div className="tag" data-qa={daqa} ref={onRef}>
        <span className="text-wrapper">
          <span
            onMouseEnter={(e) => {
              onHoverSingleTag(e, text);
            }}
            onMouseLeave={() => {
              handleSingleRequestClose(text);
            }}
            className="text"
          >
            {text}
          </span>
        </span>
      </div>
    </Tooltip>
  );
};

export default Tag;
