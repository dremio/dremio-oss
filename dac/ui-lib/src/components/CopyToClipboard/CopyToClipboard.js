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
import PropTypes from "prop-types";
import clsx from "clsx";

import { CopyToClipboard } from "react-copy-to-clipboard";
import Tooltip from "@mui/material/Tooltip";
import SvgIcon from "@mui/material/SvgIcon";

import { ReactComponent as CopyIcon } from "../../art/copy.svg";

import "./CopyToClipboard.scss";

const CopyText = (props) => {
  const { className, placement, value, onCopy, tooltipClasses, tooltipText } =
    props;

  const [tooltipOpen, setTooltipOpen] = useState(false);
  const [tooltipTitle, setTooltipTitle] = useState("");

  const handleCopySuccess = () => {
    if (onCopy && typeof onCopy === "function") {
      onCopy();
    }
    setTooltipOpen(true);
    setTooltipTitle(tooltipText);
    setTimeout(() => {
      setTooltipOpen(false);
    }, 1000);
  };

  const iconClass = clsx("copy__icon", className);

  return (
    <Tooltip
      arrow
      open={tooltipOpen}
      title={tooltipTitle}
      placement={placement}
      classes={{
        ...tooltipClasses,
        tooltip: "copy__content",
      }}
    >
      <span className={iconClass}>
        <CopyToClipboard text={value} onCopy={handleCopySuccess}>
          <SvgIcon component={CopyIcon} fontSize="large" />
        </CopyToClipboard>
      </span>
    </Tooltip>
  );
};

CopyText.defaultProps = {
  placement: "bottom",
  tooltipText: "Copied",
};

CopyText.propTypes = {
  className: PropTypes.string,
  placement: PropTypes.string,
  value: PropTypes.string.isRequired,
  onCopy: PropTypes.func,
  tooltipText: PropTypes.string,
  tooltipClasses: PropTypes.object,
};

export default CopyText;
