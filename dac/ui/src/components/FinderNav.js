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
import { PropTypes, Component } from 'react';
import { Link } from 'react-router';
import Immutable  from 'immutable';
import classNames from 'classnames';

import FontIcon from 'components/Icon/FontIcon';
import { h4 } from 'uiTheme/radium/typography';

import './FinderNav.less';

import FinderNavSection from './FinderNavSection';

const MAX_TO_SHOW = Infinity;

//not pure because of active link in FinderNavItem
export default class FinderNav extends Component {
  static propTypes = {
    title: PropTypes.string.isRequired,
    navItems: PropTypes.instanceOf(Immutable.List).isRequired,
    isInProgress: PropTypes.bool,
    addHref: PropTypes.oneOfType([PropTypes.string, PropTypes.object]),
    listHref: PropTypes.oneOfType([PropTypes.string, PropTypes.object]),
    toggleActivePin: PropTypes.func,
    children: PropTypes.node
  };

  render() {
    const { title, navItems, isInProgress, addHref, listHref, children } = this.props;
    const wrapClass = classNames('finder-nav', `${title.toLowerCase()}-wrap`); // todo: don't use ui-string for code keys

    return (
      <div className={wrapClass}>
        <div className='finder-nav-title' data-qa={title} style={{...h4, fontWeight: 500}}>
          <Link className='pointer' to={listHref}>{title} »</Link>
          {addHref && (
            <Link
              className='pull-right'
              data-qa={`add-${title.toLowerCase()}`}
              to={addHref}>
              <FontIcon type='Add' hoverType='AddHover' theme={styles.fontIcon}/>
            </Link>
          )}
        </div>
        <div className='nav-list'>
          {!isInProgress && <FinderNavSection
            items={navItems}
            isInProgress={isInProgress}
            maxItemsCount={MAX_TO_SHOW}
            title={title}
            listHref={listHref}
            toggleActivePin={this.props.toggleActivePin} />
          }
          {!isInProgress && children}
        </div>
      </div>
    );
  }
}

const styles = {
  fontIcon: {
    Icon: {
      width: 20,
      height: 20
    },
    Container: {
      height: 20,
      verticalAlign: 'middle'
    }
  }
};
