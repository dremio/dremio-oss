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
import { Component } from "react";
import PropTypes from "prop-types";
import classNames from "classnames";
import SourceIcon from "components/Icon/SourceIcon";
import sendEventToIntercom from "@inject/sagas/utils/sendEventToIntercom";
import INTERCOM_EVENTS from "@inject/constants/intercomEvents";
import * as VersionUtils from "@app/utils/versionUtils";
import {
  buttonBase,
  buttonDisabled,
  buttonClickable,
  connectionLabel,
  pill,
  pillBeta,
  pillCommunity,
  iconContainer,
} from "./SelectConnectionButton.less";

export default class SelectConnectionButton extends Component {
  static propTypes = {
    sampleSource: PropTypes.bool,
    label: PropTypes.string.isRequired,
    iconType: PropTypes.string.isRequired,
    icon: PropTypes.string,
    pillText: PropTypes.string,
    isCommunity: PropTypes.bool,
    disabled: PropTypes.bool,
    onClick: PropTypes.func,
  };

  static defaultProps = {
    disabled: false,
  };

  render() {
    const {
      label,
      iconType,
      icon,
      pillText,
      disabled,
      isCommunity,
      onClick,
      sampleSource = false,
    } = this.props;
    const edition = VersionUtils.getEditionFromConfig();
    // if icon is provided, use it, otherwise use iconType as an icon file name
    let src = icon;
    if (!src) {
      src =
        iconType === "sources/NETEZZA" ? `${iconType}.png` : `${iconType}.svg`;
    }
    const buttonClass = classNames({
      [buttonBase]: true,
      [buttonDisabled]: disabled,
      [buttonClickable]: !disabled,
    });
    return (
      <button
        disabled={disabled}
        className={buttonClass}
        onClick={
          !disabled
            ? () => {
                onClick();
                // sends intercom event for sample source since it doesnt have a form like the rest.
                if (sampleSource && edition === "DCS")
                  sendEventToIntercom(INTERCOM_EVENTS.SOURCE_ADD_COMPLETE);
              }
            : undefined
        }
        data-qa={iconType}
        key={iconType}
      >
        <div className={iconContainer}>
          <SourceIcon src={src} alt="" style={styles.iconStyle} />
        </div>
        <h3 className={connectionLabel}>{label}</h3>
        {pillText && (
          <div
            className={classNames({
              [pill]: true,
              [pillBeta]: !isCommunity,
              [pillCommunity]: isCommunity,
            })}
          >
            {pillText}
          </div>
        )}
      </button>
    );
  }
}

const styles = {
  iconStyle: {
    margin: 0,
    width: 33,
    height: 33,
    display: "flex",
  },
};
