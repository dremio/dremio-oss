/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Component, PropTypes } from 'react';
import classNames from 'classnames';
import { Link } from 'react-router';
import Immutable from 'immutable';
import Radium from 'radium';
import DocumentTitle from 'react-document-title';

import { body, h4 } from 'uiTheme/radium/typography';

@Radium
class UserNavigation extends Component {

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static propTypes = {
    menuItems: PropTypes.instanceOf(Immutable.List),
    title: PropTypes.string
  }

  getSelectedItem() {
    const { location } = this.context;
    return this.props.menuItems.find(item => location.pathname === item.get('url'));
  }

  renderMenuItems() {
    const { location } = this.context;
    const linkStyles = {...styles.linkItem, ...body};
    return this.props.menuItems.map((item, i) => {
      const className = classNames('left-item', {'selected': location.pathname === item.get('url')});
      return (
        <li key={i}>
          <Link
            className={className}
            style={linkStyles}
            to={item.get('url')}>{item.get('name')}</Link>
        </li>
      );
    });
  }

  render() {
    const { title } = this.props;
    return (
      <div className='left-menu' style={styles.leftMenu}>
        <DocumentTitle title={this.getSelectedItem().get('name')} />
        {title ? <h4 style={[styles.menuTitle, h4]}>{title}</h4> : ''}
        <ul>{this.renderMenuItems()}</ul>
      </div>
    );
  }
}

export default UserNavigation;

const styles = {
  leftMenu: {
    width: 250,
    padding: '15px 10px 20px 10px',
    backgroundColor: '#f3f3f3'
  },
  linkItem: {
    display: 'block',
    padding: '5px 10px',
    textDecoration: 'none',
    borderRadius: 2
  },
  menuTitle: {
    margin: '0 0 10px'
  }
};
