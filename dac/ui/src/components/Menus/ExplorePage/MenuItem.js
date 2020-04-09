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
import { PureComponent } from 'react';
import classnames from 'classnames';
import PropTypes from 'prop-types';

import { PALE_BLUE } from 'uiTheme/radium/colors';
import { formDefault } from 'uiTheme/radium/typography';
import DefaultMenuItem from '@app/components/Menus/MenuItem';

export default class MenuItem extends PureComponent {
  static propTypes = {
    className: PropTypes.string,
    children: PropTypes.node,
    onClick: PropTypes.func,
    href: PropTypes.string,
    title: PropTypes.string,
    disabled: PropTypes.bool
  };

  renderDiv = (className, style) => {
    const { onClick, children, title, disabled } = this.props;
    return (
      <div
        className={classnames('dropdown-menu-item', className)}
        title={title}
        onClick={disabled ? undefined : onClick} >
        <DefaultMenuItem disabled={disabled} style={style}>{children}</DefaultMenuItem>
      </div>
    );
  };

  renderLink = (className, style) => {
    const { href, children, title, onClick, disabled } = this.props;
    return (
      <a
        href={href}
        title={title}
        onClick={onClick}
        className={classnames('menu-item-link', className)}
      >
        <DefaultMenuItem disabled={disabled} style={style}>{children}</DefaultMenuItem>
      </a>
    );
  };

  render() {
    const { className, href, disabled } = this.props;
    const style = {...styles.base, ...(disabled && styles.disabled)};
    return (href && !disabled) ? this.renderLink(className, style) : this.renderDiv(className, style);
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
