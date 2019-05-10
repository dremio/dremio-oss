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
import classnames from 'classnames';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { PALE_BLUE } from 'uiTheme/radium/colors';
import { formDefault } from 'uiTheme/radium/typography';

@Radium
@pureRender
export default class MenuItem extends Component {
  static propTypes = {
    className: PropTypes.string,
    children: PropTypes.node,
    onClick: PropTypes.func,
    disabled: PropTypes.bool
  };

  render() {
    const disabledStyle = this.props.disabled
      ? styles.disabled
      : {};
    return (
      <div
        className={classnames('dropdown-menu-item', this.props.className)}
        style={[styles.base, disabledStyle]}
        onClick={this.props.onClick}>
        <span>{this.props.children}</span>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 8,
    paddingRight: 8,
    cursor: 'pointer',
    height: 24,
    ...formDefault,
    ':hover': {
      backgroundColor: PALE_BLUE
    }
  },
  disabled: {
    ':hover': {
      backgroundColor: '#fff'
    },
    cursor: 'default',
    'color': '#DDDDDD'
  }
};
