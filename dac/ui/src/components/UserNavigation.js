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
import classNames from 'classnames';
import { Link } from 'react-router';
import Radium from 'radium';
import PropTypes from 'prop-types';

@Radium
export default class UserNavigation extends Component {

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static propTypes = {
    sections: PropTypes.arrayOf(PropTypes.object)
  };

  static getFlatMenuItemsList(sections) {
    return sections.reduce((items, section) => {
      return items.concat(section.items);
    }, []);
  }

  getSelectedItem() {
    const { location } = this.context;
    const items = UserNavigation.getFlatMenuItemsList(this.props.sections);
    return items.find(item => location.pathname === item.url);
  }

  renderMenuItems(menuItems) {
    const { location } = this.context;
    const linkStyles = styles.linkItem;
    return menuItems.map((item, i) => {
      const className = classNames('left-item', {'selected': location.pathname === item.url});
      return (
        <li key={i}>
          <Link
            className={className}
            style={linkStyles}
            to={item.url}>{item.name}</Link>
        </li>
      );
    });
  }

  render() {
    return (
      <div className='left-menu' style={styles.leftMenu} data-qa='left-menu'>
        <h3 style={styles.title}>{la('Admin')}</h3>
        {/*<DocumentTitle title={this.getSelectedItem().name} />*/}
        <ul>
          {this.props.sections.map((section, sectionIndex) => (
            <li key={`left-nav-section-${sectionIndex}`} data-qa={`left-nav-section-${sectionIndex}`} style={styles.section}>
              <h4 style={styles.menuTitle}>{section.title}</h4>
              <ul>
                {this.renderMenuItems(section.items)}
              </ul>
            </li>
          ))}
        </ul>
      </div>
    );
  }
}


const styles = {
  leftMenu: {
    width: 250,
    padding: '15px 10px 20px',
    backgroundColor: '#f3f3f3'
  },
  title: {
    marginBottom: 15
  },
  linkItem: {
    display: 'block',
    color: '#333333',
    padding: '5px 10px',
    textDecoration: 'none',
    borderRadius: 2
  },
  section: {
    marginBottom: 10
  },
  menuTitle: {
    margin: '0 0 10px'
  }
};
