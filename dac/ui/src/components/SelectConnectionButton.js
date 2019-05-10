/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import SourceIcon from 'components/Icon/SourceIcon';
import {
  buttonBase,
  buttonDisabled,
  buttonClickable,
  connectionLabel,
  pill,
  pillBeta,
  pillCommunity
} from './SelectConnectionButton.less';

export default class SelectConnectionButton extends Component {
  static propTypes = {
    label: PropTypes.string.isRequired,
    iconType: PropTypes.string.isRequired,
    icon: PropTypes.string,
    pillText: PropTypes.string,
    isCommunity: PropTypes.bool,
    disabled: PropTypes.bool,
    onClick: PropTypes.func
  };

  static defaultProps = {
    disabled: false
  };

  render() {
    const { label, iconType, icon, pillText, disabled, isCommunity, onClick } = this.props;
    // if icon is provided, use it, otherwise use iconType as an icon file name
    let src = icon;
    if (!src) {
      src = (iconType === 'sources/NETEZZA') ? `${iconType}.png` : `${iconType}.svg`;
    }
    const buttonClass = classNames({
      [buttonBase]: true,
      [buttonDisabled]: disabled,
      [buttonClickable]: !disabled
    });
    return <button
      disabled={disabled}
      className={buttonClass}
      onClick={!disabled ? onClick : undefined}
      data-qa={iconType}
      key={iconType}
    >
      <SourceIcon src={src} alt='' style={styles.iconStyle} />
      <h3 className={connectionLabel}>
        {label}
      </h3>
      {pillText && <div
        className={classNames({
          [pill]: true,
          [pillBeta]: !isCommunity,
          [pillCommunity]: isCommunity
        })}
      >
        {pillText}
      </div>}
    </button>;
  }
}

const styles = {
  iconStyle: {
    width: 60,
    height: 60
  }
};
