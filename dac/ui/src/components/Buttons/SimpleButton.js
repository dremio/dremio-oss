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
import Radium from 'radium';
import PropTypes from 'prop-types';
import invariant from 'invariant';

import FontIcon from 'components/Icon/FontIcon';

import * as buttonStyles from 'uiTheme/radium/buttons';


@Radium
export default class SimpleButton extends Component {

  static propTypes = {
    onClick: PropTypes.func,
    disabled: PropTypes.bool,
    submitting: PropTypes.bool,
    buttonStyle: PropTypes.string,
    style: PropTypes.object,
    children: PropTypes.node
  };

  static defaultProps = {
    disabled: false,
    submitting: false
  };

  renderSpinner() {
    const {buttonStyle, submitting} = this.props;
    if (!submitting) {
      return null;
    }
    const loader = buttonStyle === 'primary' ? 'LoaderWhite' : 'Loader';
    return <FontIcon type={loader + ' spinner'} theme={styles.spinner} />;
  }

  render() {
    const {buttonStyle, disabled, submitting, style, children, ...props} = this.props;
    invariant(buttonStyles[buttonStyle] !== undefined, `Unknown button style: "${buttonStyle}"`);

    const combinedStyle = [
      styles.base,
      buttonStyles[buttonStyle],
      disabled ? buttonStyles.disabled : {},
      submitting ? buttonStyles.submitting[buttonStyle] : {},
      style
    ];

    return (
      <button
        disabled={submitting || disabled}
        {...props}
        style={combinedStyle}>
        {this.renderSpinner() || children}
      </button>
    );
  }
}

const styles = {
  base: {
    lineHeight: '27px',
    textDecoration: 'none'
  },
  spinner: {
    Container: {
      display: 'block',
      height: 24
    }
  }
};
