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

import React from "react";
import PropTypes from "prop-types";
import clsx from "clsx";

import MuiDialogTitle from "@material-ui/core/DialogTitle";

import { ReactComponent as WarningIcon } from "../../../art/WarningCircle.svg";
import { ReactComponent as ErrorIcon } from "../../../art/ErrorCircle.svg";
import { ReactComponent as InfoIcon } from "../../../art/InfoCircle.svg";
import { ReactComponent as CloseIcon } from "../../../art/CloseBig.svg";

import "./dialogTitle.scss";

const ICONS = {
  default: null,
  warning: WarningIcon,
  info: InfoIcon,
  error: ErrorIcon,
};

const DialogTitle = (props) => {
  const { children, onClose, classes, endChildren, type } = props;

  const titleClasses = {
    root: clsx(["dialogTitle", { [classes.root]: classes.root }]),
    ...classes,
  };

  const titleIconClasss = clsx([
    "dialogTitle__content__icon",
    { "--error": type === "error" },
  ]);

  const TitleIcon = ICONS[type];

  return (
    <MuiDialogTitle classes={titleClasses} disableTypography>
      <span className="dialogTitle__content flex --alignCenter">
        {TitleIcon && (
          <span className={titleIconClasss}>
            <TitleIcon />
          </span>
        )}
        <h2>{children}</h2>
      </span>
      <div className="dialogTitle__endChildren">
        {endChildren && endChildren}
        {onClose && (
          <span className="dialogTitle__icon margin-left" onClick={onClose}>
            <CloseIcon />
          </span>
        )}
      </div>
    </MuiDialogTitle>
  );
};

DialogTitle.propTypes = {
  children: PropTypes.node,
  classes: PropTypes.object,
  onClose: PropTypes.func,
  disableSpacing: PropTypes.bool,
  endChildren: PropTypes.node,
  type: PropTypes.oneOf(["default", "info", "warning", "error"]),
};

DialogTitle.defaultProps = {
  classes: {},
  type: "default",
};

export default DialogTitle;
