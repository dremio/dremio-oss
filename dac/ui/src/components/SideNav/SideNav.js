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
import {useIntl, FormattedMessage} from 'react-intl';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { compose } from 'redux';
import { withRouter, Link } from 'react-router';
import classNames from 'classnames';

/****************************************************/
/*                                                  */
/* THE COMMENTED OUT CODE IS FOR WIDE MODE, WHICH   */
/* FOR NOW IS HIDDEN                                */
/*                                                  */
/****************************************************/

import logoNarrow from '@app/components/Icon/icons/DremioLogo64x64.svg';
// import logoWide from '@app/components/Icon/icons/DremioLogo64x224.svg';
import { getExploreState } from '@app/selectors/explore';
import { getLocation } from 'selectors/routing';
// import localStorageUtils from '@inject/utils/storageUtils/localStorageUtils';

import { showConfirmationDialog } from 'actions/confirmation';
import { resetNewQuery } from 'actions/explore/view';
import { EXPLORE_VIEW_ID } from 'reducers/explore/view';
import { parseResourceId } from 'utils/pathUtils';

import SideNavAdmin from 'dyn-load/components/SideNav/SideNavAdmin';
import SideNavExtra from 'dyn-load/components/SideNav/SideNavExtra';
import SideNavHoverMenu from './SideNavHoverMenu';
import HelpMenu from './HelpMenu';
import AccountMenu from './AccountMenu';

import '@app/components/IconFont/css/DremioIcons-old.css';
import '@app/components/IconFont/css/DremioIcons.css';

import {DEFAULT_ICON_COLOR, ACTIVE_ICON_COLOR, ICON_BACKGROUND_COLOR, hashCode /*, dropdownMenuStyle*/} from './SideNavConstants';
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

  const intl = useIntl();
  // const [wideNarrowState, setWideNarrow] = useState(localStorageUtils.isSideNavWide() ? 'wide' : 'narrow');


  const userColorIndex = Math.abs(hashCode(user.get('userId'))) % ICON_BACKGROUND_COLOR.length;
  const userColor = ICON_BACKGROUND_COLOR[userColorIndex];

  const  userName = user.get('userName');
  const userNameFirst2 = (user.get('firstName') && user.get('lastName'))
    ? user.get('firstName').substring(0, 1).toUpperCase() + user.get('lastName').substring(0, 1).toUpperCase()
    : userName && userName.substring(0, 2).toUpperCase();

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

  const defaultClickHandler = (e) => {
    e.stopPropagation();
  };

  const renderTopAction = (name, active, anchorUrl, icon, labelTooltip, dataqa = 'data-qa', anchorOnChange = undefined, iconStyle = '') => {
    if (!anchorOnChange) {
      anchorOnChange = defaultClickHandler;
    }
    let activeStyle = {color: DEFAULT_ICON_COLOR};
    if (active !== '') {
      activeStyle = {color: ACTIVE_ICON_COLOR};
    }

    return (
      <div className={'sideNav-item'}>
        <div className={name + active} title={displayTooltip ? intl.formatMessage({id: labelTooltip}) : ''}>
          <Link to={anchorUrl} dataqa={dataqa} style={{...activeStyle}}>
            <div className={'sideNav-items'}>
              <div className={'sideNav-item__icon'}>
                <div className={icon + ' sideNav-item__iconType'} style={{...iconStyle}}/>
              </div>
              <div className={'sideNav-item__labelNext' + displayLabel}>
                <FormattedMessage id={labelTooltip}/>
              </div>
            </div>
          </Link>
        </div>
      </div>
    );
  };

  // const wideNarrowHandler = () => {
  //   localStorageUtils.setSideNavWide(wideNarrowState === 'narrow');  // flip state
  //   setWideNarrow(wideNarrowState === 'wide' ? 'narrow' : 'wide');
  // };

  const loc = location.pathname;
  const jobsIsActive = loc === '/jobs' ? ' --active' : '';
  const datasetIsActive = loc === '/' || loc.startsWith('/space') || loc.startsWith('/home') ? ' --active' : '';
  const newQueryIsActive = loc === '/new_query' ? ' --active' : '';
  const userIsActive = loc === '/account/info' ? ' --active' : '';

  // const wideNarrowIcon = wideNarrowState === 'narrow' ? 'dremioIcon-Expand' : 'dremioIcon-Collapse';
  // const wideNarrowTooltip = wideNarrowState === 'narrow' ? intl.formatMessage({id: 'SideNav.OpenNav'}) : intl.formatMessage({id: 'SideNav.CloseNav'});
  // const wideNarrowWidth = wideNarrowState === 'narrow' ? ' --narrow' : ' --wide';
  // const helpPopupPosition = wideNarrowState === 'narrow' ? {marginTop: '-123px', marginLeft: '48px'} : {marginTop: '-123px', marginLeft: '192px'};
  // const userPopupPosition = wideNarrowState === 'narrow' ? {marginTop: '-65px', marginLeft: '48px'} : {marginTop: '-65px', marginLeft: '192px'};
  // const displayTooltip = wideNarrowState === 'narrow';
  // const displayLabel = wideNarrowState === 'wide' ? ' --show' : ' --hide';
  // const helpPopupPosition = {marginTop: '-59px', marginLeft: '48px'};
  // const userPopupPosition = {marginTop: '-1px', marginLeft: '48px'};
  const logo =  logoNarrow; //wideNarrowState === 'narrow' ? logoNarrow : logoWide;
  const wideNarrowWidth = ' --narrow';
  const displayTooltip = true;
  const displayLabel = ' --hide';


  // display only the company logo
  if (narwhalOnly) {
    return (
      <div className={'sideNav ' + wideNarrowWidth}>
        <div className={'sideNav__topSection'}>
          <div className={'sideNav-item'}>
            <div className={'sideNav__logo'}>
              <Link to={'/'}>
                <div className={classNames('sideNav__imageDiv', wideNarrowWidth)}>
                  <img
                    src={logo}
                    alt={intl.formatMessage({id: 'SideNav.DremioLogoLabel'})}
                    style={{filter: `saturate(${socketIsOpen ? 1 : 0})`}}
                  />
                </div>
              </Link>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className={'sideNav ' + wideNarrowWidth}>
      <div className={'sideNav__topSection'}>
        <div className={'sideNav-item'}>
          <div className={'sideNav__logo'}>
            <Link to={'/'}>
              <div className={classNames('sideNav__imageDiv', wideNarrowWidth)}>
                <img
                  src={logo}
                  alt={intl.formatMessage({id: 'SideNav.DremioLogoLabel'})}
                  style={{filter: `saturate(${socketIsOpen ? 1 : 0})`}}
                />
              </div>
            </Link>
          </div>
        </div>
        {renderTopAction('sideNav-item__link', datasetIsActive, '/', 'datasetsIcon dremioIcon-SideNavDatasets', 'SideNav.Datasets', 'select-datasets')}
        {renderTopAction('sideNav-item__link', newQueryIsActive, getNewQueryHref(), 'newQuery dremioIcon-SQL', 'SideNav.NewQuery', 'new-query-button', handleClick, {fontSize: '1.9rem'})}
        {renderTopAction('sideNav-item__link', jobsIsActive,  '/jobs', 'jobsIcon dremioIcon-SideNavjobs', 'SideNav.Jobs', 'select-jobs')}
      </div>

      <div className='sideNav__bottomSection'>
        <SideNavExtra displayTooltip={displayTooltip} displayLabel={displayLabel} wideNarrowWidth={wideNarrowWidth}/>
        <SideNavAdmin user={user} wideNarrowWidth={wideNarrowWidth} displayTooltip={displayTooltip} displayLabel={displayLabel}/>
        <SideNavHoverMenu
          wideNarrowWidth={wideNarrowWidth}
          tooltipStringId={'SideNav.Help'}
          menu={<HelpMenu/>}
          icon={'dremioIcon-SideNavHelp'}
          menuDisplayUp
        />
        <SideNavHoverMenu
          wideNarrowWidth={wideNarrowWidth}
          tooltipString={userTooltip}
          menu={<AccountMenu/>}
          menuDisplayUp
          divBlob = {
            <>
              <div className={'sideNav-item__customHoverMenu' + wideNarrowWidth + userIsActive}>
                <div className={classNames('sideNav-items', wideNarrowWidth)}>
                  <div className='sideNav__customOuter' title={displayTooltip ? userTooltip : ''}>
                    <div
                      className={classNames('sideNav__user sideNav-item__dropdownIcon', wideNarrowWidth)}
                      style={{backgroundColor: userColor}}
                    >
                      <span>{userNameFirst2}</span>
                    </div>
                  </div>
                  <div className={'sideNav-item__labelNext' + displayLabel}>
                    {userName}
                  </div>
                </div>
              </div>
            </>
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

