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

import { connect } from 'react-redux';
import {useIntl} from 'react-intl';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { compose } from 'redux';
import { withRouter } from 'react-router';

/****************************************************/
/*                                                  */
/* THE COMMENTED OUT CODE IS FOR WIDE MODE, WHICH   */
/* FOR NOW IS HIDDEN                                */
/*                                                  */
/****************************************************/

import { getExploreState } from '@app/selectors/explore';
import { getLocation } from 'selectors/routing';

import { showConfirmationDialog } from 'actions/confirmation';
import { resetNewQuery } from 'actions/explore/view';
import { EXPLORE_VIEW_ID } from 'reducers/explore/view';
import { parseResourceId } from 'utils/pathUtils';

import SideNavAdmin from 'dyn-load/components/SideNav/SideNavAdmin';
import SideNavExtra from 'dyn-load/components/SideNav/SideNavExtra';

import localStorageUtils, { useProjectContext } from '@inject/utils/storageUtils/localStorageUtils';
import getIconColor from '@app/utils/getIconColor';
import getUserIconInitials from '@app/utils/userIcon';


import SideNavHoverMenu from './SideNavHoverMenu';
import AccountMenu from './AccountMenu';
import '@app/components/IconFont/css/DremioIcons-old.css';
import '@app/components/IconFont/css/DremioIcons.css';

import { isActive } from './SideNavUtils';
import HelpMenu from './HelpMenu';
import { TopAction } from './components/TopAction';
import './SideNav.less';

const SideNav = (props) => {
  const {
    socketIsOpen,
    user,
    router,
    location,
    currentSql,
    narwhalOnly
  } = props;

  const loc = location.pathname;
  const intl = useIntl();
  const ctx = useProjectContext();
  const isDDPOnly = localStorageUtils ? localStorageUtils.isDataPlaneOnly(ctx) : false;
  const logoSVG = isDDPOnly ? 'DremioLogoDDP.svg' : 'DremioLogo32x32.svg';

  const { backgroundColor: userBgColor, color: userColor } = getIconColor(user.get('userId'));
  const  userName = user.get('userName');
  const userNameFirst2 = getUserIconInitials(user);

  const userTooltip = intl.formatMessage({id: 'SideNav.User'}) + userName;

  const getNewQueryHref = () => {
    const resourceId = parseResourceId(location.pathname, user.get('userName'));
    return '/new_query?context=' + encodeURIComponent(resourceId);
  };
  const handleClick = (e) => {
    if (e.metaKey || e.ctrlKey) { // DX-10607, DX-11299 pass to default link behaviour, when cmd/ctrl is pressed on click
      return;
    }
    if (location.pathname === '/new_query') {
      if (currentSql && currentSql.trim()) {
        showConfirmationDialog({
          title: intl.formatMessage({id: 'Common.UnsavedWarning'}),
          text: [
            intl.formatMessage({id: 'NewQuery.UnsavedChangesWarning'}),
            intl.formatMessage({id: 'NewQuery.UnsavedChangesWarningPrompt'})
          ],
          confirmText: intl.formatMessage({id: 'Common.Continue'}),
          cancelText: intl.formatMessage({id: 'Common.Cancel'}),
          confirm: () => {
            resetNewQuery(EXPLORE_VIEW_ID);
          }
        });
      } else {
        resetNewQuery(EXPLORE_VIEW_ID); // even if there's no SQL, clear any errors
      }
    } else {
      router.push(getNewQueryHref());
    }
    e.preventDefault();
  };

  const LogoAction = <TopAction
    url='/'
    icon={logoSVG}
    alt='Logo'
    logo
    tooltip={false}
    socketIsOpen={socketIsOpen}
    tooltipProps={{placement: 'right'}}
  />;

  // display only the company logo
  if (narwhalOnly) {
    return (
      <div className='sideNav'>
        <div className='sideNav__topSection'>
          {LogoAction}
        </div>
      </div>
    );
  }

  return (
    <div className='sideNav'>
      <div className='sideNav__topSection'>
        {LogoAction}
        <TopAction
          tooltipProps={{placement: 'right'}}
          active={isActive({name:'/', dataset: true, loc, isDDPOnly})}
          url='/'
          icon='SideNav-table.svg'
          alt='SideNav.Datasets'
        />
        {!isDDPOnly && (
          <>
            <TopAction
              tooltipProps={{placement: 'right'}}
              active={isActive({name:'/new_query', loc, sql: true})}
              url={getNewQueryHref()} icon='SideNav-newQuery.svg'
              alt='SideNav.NewQuery' data-qa='new-query-button'
              onClick={() => handleClick}
            />
            <TopAction
              tooltipProps={{placement: 'right'}}
              active={isActive({loc, jobs: true})}
              url='/jobs'
              icon='SideNav-activity.svg'
              alt='SideNav.Jobs'
              data-qa='select-jobs'
            />
          </>
        )}
      </div>

      <div className='sideNav__bottomSection'>
        <SideNavExtra />
        <SideNavAdmin user={user} />
        <SideNavHoverMenu
          tooltipStringId={'SideNav.Help'}
          menu={<HelpMenu/>}
          icon={'SideNav-help.svg'}
          menuDisplayUp
        />
        <SideNavHoverMenu
          tooltipString={userTooltip}
          menu={<AccountMenu/>}
          menuDisplayUp
          isActive={isActive({name: '/account/info', loc})}
          divBlob = {
            <div className={'sideNav-item__customHoverMenu'}>
              <div className='sideNav-items'>
                <div className='sideNav__customOuter'>
                  <div
                    className='sideNav__user sideNav-item__dropdownIcon'
                    style={{ backgroundColor: userBgColor, color: userColor }}
                  >
                    <span>{userNameFirst2}</span>
                  </div>
                </div>
              </div>
            </div>
          }
        />
      </div>
    </div>
  );
};

SideNav.propTypes = {
  narwhalOnly: PropTypes.bool,
  location: PropTypes.object.isRequired,
  currentSql: PropTypes.string,
  user: PropTypes.instanceOf(Immutable.Map),
  socketIsOpen: PropTypes.bool.isRequired,
  showConfirmationDialog: PropTypes.func,
  resetNewQuery: PropTypes.func,
  router: PropTypes.shape({
    isActive: PropTypes.func,
    push: PropTypes.func
  })
};

const mapStateToProps = state => {
  const explorePage = getExploreState(state); //todo explore page state should not be here
  return {
    user: state.account.get('user'),
    socketIsOpen: state.serverStatus.get('socketIsOpen'),
    location: getLocation(state),
    currentSql: explorePage ? explorePage.view.currentSql : null
  };
};

const mapDispatchToProps = {
  showConfirmationDialog,
  resetNewQuery
};

export default compose(withRouter, connect(mapStateToProps, mapDispatchToProps))(SideNav);

