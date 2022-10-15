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
import { get } from "lodash";
import TextareaAutosize from "@mui/material/TextareaAutosize";

import Label from "../Label";
import CopyToClipboard from "../CopyToClipboard";

import "./textArea.scss";

const TextArea = (props) => {
  const {
    classes,
    dataQa,
    defaultValue,
    disabled,
    disableTextCopy,
    enableCopy,
    helpText,
    label,
    labelStyle,
    minLines,
    maxLines,
    name,
    noResize,
    onChange,
    onCopy,
    value,
    tooltipText,
    tooltipClasses,
    tooltipPlacement,
    form: { errors, touched },
    ...otherProps
  } = props;

  const hasError = get(touched, name) && get(errors, name);

  const rootClass = clsx("textAreaRoot", { [classes.root]: classes.root });
  const labelClass = clsx("textAreaRoot__label", {
    [classes.label]: classes.label,
  });
  const labelInnerClass = clsx({ [classes.labelInner]: classes.labelInner });
  const textAreaContainerClass = clsx("textAreaRoot__container", {
    [classes.container]: classes.container,
  });
  const textClass = clsx(
    { "--noResize": noResize },
    { "--singleLine": maxLines === 1 },
    { "--error": hasError }
  );

  const handleMouseEnter = () => {
    document.body.classList.add("disable-back-scroll");
  };

  const handleMouseLeave = () => {
    document.body.classList.remove("disable-back-scroll");
  };

  const handleTextAreaCopy = (e) => {
    if (disableTextCopy) {
      e.preventDefault();
    }
  };

  return (
    <div className={rootClass}>
      {label && (
        <div className="textAreaRoot__labelContainer">
          <Label
            value={label}
            className={labelClass}
            labelInnerClass={labelInnerClass}
            style={labelStyle}
            helpText={helpText}
            id={`textbox-label-${name}`}
          />
          {enableCopy && (
            <CopyToClipboard
              value={value}
              onCopy={onCopy}
              placement={tooltipPlacement}
              tooltipClasses={tooltipClasses}
              tooltipText={tooltipText}
            />
          )}
        </div>
      )}

      <div className={textAreaContainerClass}>
        <TextareaAutosize
          defaultValue={defaultValue}
          value={value}
          disabled={disabled}
          className={textClass}
          minRows={minLines}
          maxRows={minLines > maxLines ? minLines : maxLines}
          aria-labelledby={`textbox-label-${name}`}
          onChange={onChange}
          name={name}
          onPointerEnter={handleMouseEnter}
          onMouseEnter={handleMouseEnter}
          onMouseLeave={handleMouseLeave}
          onCopy={handleTextAreaCopy}
          data-qa={dataQa}
          {...otherProps}
        />
        {!label && enableCopy && (
          <CopyToClipboard
            value={value}
            onCopy={onCopy}
            className="textAreaRoot__icon"
            placement={tooltipPlacement}
            tooltipClasses={tooltipClasses}
            tooltipText={tooltipText}
          />
        )}
      </div>
    </div>
  );
};

TextArea.propTypes = {
  classes: PropTypes.shape({
    root: PropTypes.string,
    label: PropTypes.string,
    labelInner: PropTypes.string,
    container: PropTypes.string,
  }),
  copyMessage: PropTypes.string,
  dataQa: PropTypes.string,
  value: PropTypes.string,
  disabled: PropTypes.bool,
  form: PropTypes.object,
  disableTextCopy: PropTypes.bool,
  enableCopy: PropTypes.bool,
  label: PropTypes.string,
  labelStyle: PropTypes.object,
  minLines: PropTypes.number,
  maxLines: PropTypes.number,
  name: PropTypes.string,
  noResize: PropTypes.bool,
  onChange: PropTypes.func,
  onCopy: PropTypes.func,
  defaultValue: PropTypes.string,
  helpText: PropTypes.string,
  tooltipText: PropTypes.string,
  tooltipClasses: PropTypes.object,
  tooltipPlacement: PropTypes.string,
};

TextArea.defaultProps = {
  classes: {},
  copyMessage: "Copied",
  form: {},
  disabled: false,
  disableTextCopy: false,
  enableCopy: false,
  label: null,
  noResize: false,
  tooltipPlacement: "bottom",
};

export default TextArea;
