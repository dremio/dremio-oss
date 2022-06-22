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

import { ReactComponent as XSmall } from "../../art/XSmall.svg";
import { ReactComponent as SuccessIcon } from "../../art/OK.svg";
import { ReactComponent as WarningIcon } from "../../art/Warning.svg";
import { ReactComponent as ErrorIcon } from "../../art/ErrorSolid.svg";
import { ReactComponent as InfoIcon } from "../../art/InfoCircleNoninteractive.svg";

import "./simpleMessage.scss";

const SimpleMessage = ({ type, message, handleDismiss }) => {
  const renderMessageIcon = (messageType) => {
    switch (messageType) {
      case "error":
        return <ErrorIcon className="simpleMessage__icon margin-right" />;
      case "warning":
        return <WarningIcon className="simpleMessage__icon margin-right" />;
      case "info":
        return <InfoIcon className="simpleMessage__icon margin-right" />;
      case "success":
        return <SuccessIcon className="simpleMessage__icon margin-right" />;
      default:
        return null;
    }
  };

  return (
    <div className={`simpleMessage --${type}`}>
      {renderMessageIcon(type)}
      <span className="simpleMessage__text">{message}</span>
      <XSmall
        className="simpleMessage__dismissIcon"
        onClick={handleDismiss}
        data-qa="dismiss-message"
      />
    </div>
  );
};

SimpleMessage.propTypes = {
  type: PropTypes.oneOf(["error", "info", "success", "warning"]).isRequired,
  message: PropTypes.string.isRequired,
  handleDismiss: PropTypes.func.isRequired,
};

SimpleMessage.defaultProps = {
  type: "info",
};

export default SimpleMessage;
