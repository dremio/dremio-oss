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
import { Link } from 'react-router';
import Radium from 'radium';
import PropTypes from 'prop-types';
import invariant from 'invariant';

import MenuItem from './MenuItem';

@Radium
export default class MenuItemLink extends Component {
  static propTypes = {
    href: PropTypes.oneOfType([ PropTypes.string, PropTypes.object ]).isRequired,
    text: PropTypes.string.isRequired,
    closeMenu: PropTypes.func,
    disabled: PropTypes.bool,
    external: PropTypes.bool,
    newWindow: PropTypes.bool
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
    const { href, text, disabled, external, newWindow } = this.props;

    invariant(!newWindow || external && newWindow, 'newWindow cannot be enabled without external also being enabled');

    const target = newWindow ? '_blank' : null;

    const link = external
      ? <a href={href} target={target} style={styles.linkStyle} onClick={this.onClick}>{text}</a>
      : <Link style={styles.linkStyle} to={href || ''} onClick={this.onClick}>{text}</Link>;
    return (
      <MenuItem disabled={disabled}>
        {link}
      </MenuItem>
    );
  }
}

const styles = {
  linkStyle: {
    textDecoration: 'none',
    color: '#333',
    display: 'flex',
    alignItems: 'center',
    width: '100%',
    height: 25
  }
};
