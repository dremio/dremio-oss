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
import classNames from 'classnames';
import { Link, withRouter } from 'react-router';
import Radium from 'radium';
import PropTypes from 'prop-types';

@Radium
export class UserNavigation extends Component {
  static propTypes = {
    sections: PropTypes.arrayOf(PropTypes.object),
    title: PropTypes.string,
    location: PropTypes.object.isRequired
  };

  static getFlatMenuItemsList(sections) {
    return sections.reduce((items, section) => {
      return items.concat(section.items);
    }, []);
  }

  getSelectedItem() {
    const { location } = this.props;
    const items = UserNavigation.getFlatMenuItemsList(this.props.sections);
    return items.find(item => location.pathname === item.url);
  }

  renderMenuItems(menuItems) {
    const { location } = this.props;
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

  renderMenuHeader = (section) => {
    if (!section.url) {
      return <h4 style={styles.menuTitle}>{section.title}</h4>;
    }
    const { location } = this.props;
    const className = classNames('left-item', {'selected': location.pathname === section.url});

    return (
      <Link
        className={className}
        style={styles.linkHeader}
        to={section.url}>
        <h4>{section.title}</h4>
      </Link>
    );
  }

  render() {
    return (
      <div className='left-menu' style={styles.leftMenu} data-qa='left-menu'>
        <h3 style={styles.title}>{this.props.title || la('Admin')}</h3>
        {/*<DocumentTitle title={this.getSelectedItem().name} />*/}
        <ul>
          {this.props.sections.map((section, sectionIndex) => (
            <li key={`left-nav-section-${sectionIndex}`} data-qa={`left-nav-section-${sectionIndex}`} style={styles.section}>
              {this.renderMenuHeader(section)}
              { section.items && <ul>
                {this.renderMenuItems(section.items)}
              </ul> }
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
  linkHeader: {
    display: 'block',
    color: '#333333',
    padding: '5px 0px',
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

export default withRouter(UserNavigation);
