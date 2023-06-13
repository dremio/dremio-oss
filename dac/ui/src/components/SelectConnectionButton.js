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
import classNames from "clsx";
import { Button } from "dremio-ui-lib/components";
import SourceIcon from "components/Icon/SourceIcon";
import {
  buttonBase,
  buttonDisabled,
  connectionLabel,
  pill,
  pillBeta,
  pillCommunity,
  iconContainer,
} from "./SelectConnectionButton.less";
import { sonarEvents } from "dremio-ui-common/sonar/sonarEvents.js";
export default class SelectConnectionButton extends Component {
  static propTypes = {
    sampleSource: PropTypes.bool,
    label: PropTypes.string.isRequired,
    dremioIcon: PropTypes.string,
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
      dremioIcon,
      pillText,
      disabled,
      isCommunity,
      onClick,
      sampleSource = false,
    } = this.props;

    const buttonClass = classNames({
      [buttonBase]: true,
      [buttonDisabled]: disabled,
    });
    return (
      <Button
        variant="tertiary"
        disabled={disabled}
        className={buttonClass}
        onClick={() => {
          onClick();
          if (sampleSource) {
            sonarEvents.sourceAddComplete();
          }
        }}
        data-qa={dremioIcon}
      >
        <div className={iconContainer}>
          <SourceIcon dremioIcon={dremioIcon} style={styles.iconStyle} />
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
      </Button>
    );
  }
}

const styles = {
  iconStyle: {
    margin: 0,
    width: 40,
    height: 40,
    display: "flex",
  },
};
