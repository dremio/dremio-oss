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
import { Component } from 'react';
import { Link } from 'react-router';
import Radium from 'radium';
import PropTypes from 'prop-types';

import MenuItem from './MenuItem';
import './MenuItemLink.less';
@Radium
export default class MenuItemLink extends Component {
  static propTypes = {
    href: PropTypes.oneOfType([ PropTypes.string, PropTypes.object ]).isRequired,
    text: PropTypes.string.isRequired,
    closeMenu: PropTypes.func,
    disabled: PropTypes.bool,
    external: PropTypes.bool,
    newWindow: PropTypes.bool,
    rightIcon: PropTypes.object,
    leftIcon: PropTypes.object
  };

  constructor(props) {
    super(props);

    this.onClick = this.onClick.bind(this);
  }

  onClick() {
    const { closeMenu } = this.props;
    if (closeMenu) {
      closeMenu();
    }
  }

  render() {
    const { href, text, disabled, external, newWindow, leftIcon, rightIcon } = this.props;

    const target = newWindow ? '_blank' : null;

    const menuItem = (
      <MenuItem disabled={disabled} leftIcon={leftIcon} rightIcon={rightIcon}>
        <div className='menuItemLink__item'>{text}</div>
      </MenuItem>
    );

    const link = external
      ? <a href={href} target={target} className='menuItemLink__link' onClick={this.onClick}>{menuItem}</a>
      : <Link className='menuItemLink__link' to={href || ''} target={target} onClick={this.onClick}>{menuItem}</Link>;

    return link;
  }
}
