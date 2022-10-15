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

import Label from "../Label";
import CopyToClipboard from "../CopyToClipboard";

import SyntaxHighlighter from "react-syntax-highlighter";

import jsonStyle from "./jsonStyle";

import "./JsonSyntaxHighlighter.scss";

type JsonSyntaxHighlighterProps = {
  json: any;
  label: string;
  labelClass?: any;
  labelStyle?: any;
  labelInnerClass?: any;
  helpText?: string;
  tooltipPlacement?: any;
  tooltipClasses?: any;
  tooltipText?: any;
};

const JsonSyntaxHighlighter = (props: JsonSyntaxHighlighterProps) => {
  const {
    json,
    label,
    labelClass,
    labelStyle,
    labelInnerClass,
    helpText,
    tooltipPlacement,
    tooltipClasses,
    tooltipText,
  } = props;

  return (
    <React.Fragment>
      <div className="label-container">
        <Label
          value={label}
          className={labelClass}
          labelInnerClass={labelInnerClass}
          style={labelStyle}
          helpText={helpText}
          id={`textbox-label-${name}`}
        />
        <CopyToClipboard
          value={json}
          placement={tooltipPlacement}
          tooltipClasses={tooltipClasses}
          tooltipText={tooltipText}
        />
      </div>
      <div className="json-container" tabIndex={1}>
        <SyntaxHighlighter language="json" style={jsonStyle}>
          {json}
        </SyntaxHighlighter>
      </div>
    </React.Fragment>
  );
};

export default JsonSyntaxHighlighter;
