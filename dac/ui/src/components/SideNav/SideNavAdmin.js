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

import {useIntl} from 'react-intl';
import PropTypes from 'prop-types';
import { compose } from 'redux';
import { withRouter, Link } from 'react-router';
import classNames from 'classnames';

import '@app/components/IconFont/css/DremioIcons.css';
import '@app/components/SideNav/SideNav.less';
import {DEFAULT_ICON_COLOR, ACTIVE_ICON_COLOR} from '@app/components/SideNav/SideNavConstants';

const SideNavAdmin = (props) => {
  const {wideNarrowWidth, displayTooltip, displayLabel, location} = props;
  const intl = useIntl();

  const isAdminActive = location.pathname.startsWith('/admin') ? ' --active' : '';
  let activeStyle = {color: DEFAULT_ICON_COLOR};
  if (location.pathname === '/admin/nodeActivity') {
    activeStyle = {color: ACTIVE_ICON_COLOR};
  }

  return (
    <div className={'sideNav-item' + wideNarrowWidth + isAdminActive}>
      <div className={'sideNav-item__link' + wideNarrowWidth + isAdminActive}>
        <Link to='/admin'  onClick={(e) => e.stopPropagation()} style={{...activeStyle}}>
          <div className={classNames('sideNav-items', wideNarrowWidth)}>
            <div className='sideNav-item__icon'>
              <div className={classNames('sideNav-item__dropdownIcon', wideNarrowWidth)} title={displayTooltip ? intl.formatMessage({id: 'SideNav.Admin'}) : ''}>
                <span className={'sideNav-item__iconType --resourceIcon dremioIcon-SideNavResources'}></span>
              </div>
            </div>
            <div className={'sideNav-item__labelNext' + displayLabel}>
              {intl.formatMessage({id: 'SideNav.Admin'})}
            </div>
          </div>
        </Link>
      </div>
    </div>
  );
};

SideNavAdmin.propTypes = {
  wideNarrowWidth: PropTypes.string,
  displayLabel: PropTypes.string,
  displayTooltip: PropTypes.bool,
  location: PropTypes.object,
  router: PropTypes.shape({
    isActive: PropTypes.func,
    push: PropTypes.func
  })
};

export default compose(withRouter)(SideNavAdmin);
