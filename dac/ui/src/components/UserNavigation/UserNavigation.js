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

import classNames from 'classnames';
import PropTypes from 'prop-types';
import { Link, withRouter } from 'react-router';
import { FormattedMessage } from 'react-intl';
import Art from '@app/components/Art';

import './userNavigation.less';

const UserNavigation = (props) => {
  const {
    location,
    sections,
    title,
    titleObject,
    navigationSection
  } = props;
  const renderMenuItems = (menuItems) => {
    return menuItems.map((item, i) => {
      const selected = location.pathname === item.url || location.pathname.startsWith(`${item.url}/`);
      const className = classNames(
        'userNavigation__link',
        'userNavigation__item',
        { '--selected': selected }
      );
      return (
        <li key={i}>
          <Link
            className={className}
            to={item.url}
          >
            <FormattedMessage id={item.name} defaultMessage={item.name} />
          </Link>
        </li>
      );
    });
  };

  const renderMenuHeader = (section, index, isNavigation) => {
    if (!section.url) {
      return (
        <div className='userNavigation__header'>
          {section.icon && <Art src={section.icon} alt={section.title} className='userNavigation__icon' />}
          <span><FormattedMessage id={section.title}  defaultMessage={section.title}/></span>
        </div>
      );
    }

    const selected = location.pathname === section.url || location.pathname.startsWith(`${section.url}/`);
    const itemClassName = classNames(
      { 'userNavigation__headerLink': !isNavigation },
      { 'userNavigation__navigation': isNavigation },
      { 'border-top': isNavigation },
      { '--selected': selected }
    );
    return (
      <Link
        className='userNavigation__link'
        to={section.url}
        key={`nav-header-${index}`}
      >
        <div className={itemClassName}>
          {section.icon && <Art src={section.icon} alt={section.title} id={selected ? 'selected' : null} className={selected || section.icon === 'Back.svg' ? 'userNavigation__icon' : 'userNavigation__icon --unselected'} />}
          <span><FormattedMessage id={section.title}  defaultMessage={section.title}/></span>
          {section.rightIcon && <Art src={section.rightIcon} alt={section.title} className='userNavigation__icon' />}
        </div>
      </Link>
    );
  };

  return (
    <div className='left-menu userNavigation__container' data-qa='left-menu'>
      {
        titleObject ? (
          <div className='userNavigation__title'>
            <div className='userNavigation__iconContainer'>
              <Link to={titleObject.url} >
                <Art src={titleObject.icon} alt={titleObject.title} className='userNavigation__title-icon' />
              </Link>
            </div>
            <div className='userNavigation__titleContainer'>
              <span className='userNavigation__location'>
                <FormattedMessage id={titleObject.topTitle} />
              </span>
              <p className='text-ellipsis' title={titleObject.title}>
                {titleObject.title}
              </p>
            </div>
          </div>
        ) : (
          <div className='userNavigation__title userNavigation__loneTitle'>
            <span className='text-ellipsis'>
              {title}
            </span>
          </div>
        )
      }
      <ul className='userNavigation__itemList'>
        {sections.map((section, sectionIndex) => (
          <li key={`left-nav-section-${sectionIndex}`} data-qa={`left-nav-section-${sectionIndex}`}>
            {renderMenuHeader(section, sectionIndex)}
            { section.items && <ul>
              {renderMenuItems(section.items)}
            </ul> }
          </li>
        ))}
        {
          navigationSection && <div className='margin-top--triple'>
            {navigationSection.map((section, index) => renderMenuHeader(section, index, true))}
          </div>
        }
      </ul>
    </div>
  );
};

UserNavigation.propTypes = {
  location: PropTypes.object.isRequired,
  sections: PropTypes.array,
  title: PropTypes.string,
  titleObject: PropTypes.object,
  navigationSection: PropTypes.array
};

export default withRouter(UserNavigation);
