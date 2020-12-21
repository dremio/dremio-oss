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
import { connect } from 'react-redux';
import { Link } from 'react-router';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { FormattedMessage, injectIntl } from 'react-intl';

import Art from 'components/Art';

import MainHeaderMixin from 'dyn-load/components/MainHeaderMixin';
import config from '../utils/config';
import MainHeaderItem from './MainHeaderItem';
import HeaderLink from './HeaderItemsTypes/HeaderLink';
import SearchItem from './HeaderItemsTypes/SearchItem';
import NewQueryButton from './HeaderItemsTypes/NewQueryButton';
import HeaderDropdown from './HeaderItemsTypes/HeaderDropdown';
import AccountMenu from './AccountMenu';
import HelpMenu from './HelpMenu';

import './MainHeader.less';
import './IconFont/css/DremioIcons.css';

@injectIntl
@Radium
@MainHeaderMixin
export class MainHeader extends PureComponent {

  static propTypes = {
    user: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired,
    socketIsOpen: PropTypes.bool.isRequired
  };

  static defaultProps = {
    user: Immutable.Map()
  };

  static contextTypes = {
    location: PropTypes.object,
    routeParams: PropTypes.object,
    router: PropTypes.object
  }

  render() {
    let datasetsActiveStyle = 'datasetsIcon dremioIcon-HeaderDataset icon-type';
    let jobsActiveStyle = 'jobsIcon dremioIcon-HeaderJobs icon-type';
    const {router} = this.context;
    if (router.isActive('/jobs')) {
      jobsActiveStyle = 'jobsIcon dremioIcon-HeaderJobs icon-type active';
    } else if (!router.isActive('/admin')) {
      datasetsActiveStyle = 'datasetsIcon dremioIcon-HeaderDataset icon-type active';
    }
    const { user, socketIsOpen } = this.props;

    return (
      <div className='explore-header'>
        <Link
          className='dremio'
          to='/'
          style={styles.logo}>
          <span className={'dremioLogoWithTextContainer'}>
            <Art
              className={'dremioLogoWithText'}
              src={'NarwhalLogoWithNameLight.svg'}
              alt={this.props.intl.formatMessage({id: 'App.NarwhalLogo'})}
              style={{...styles.logoIcon, filter: `saturate(${socketIsOpen ? 1 : 0})`}}/>
          </span>
        </Link>
        <div className='header-wrap'>
          <div className='left-part'>
            {this.renderHeaderExtra()}
            <MainHeaderItem>
              <HeaderLink to='/'>
                <div className='headerLinkContent'>
                  <div className={datasetsActiveStyle}></div>
                  <div className='text'>
                    <FormattedMessage id='Dataset.Datasets'/>
                  </div>
                </div>
              </HeaderLink>
            </MainHeaderItem>
            <MainHeaderItem>
              <HeaderLink to='/jobs'>
                <div className='headerLinkContent'>
                  <div className={jobsActiveStyle}></div>
                  <div className='text'>
                    <FormattedMessage id='Job.Jobs'/>
                  </div>
                </div>
              </HeaderLink>
            </MainHeaderItem>
            <MainHeaderItem>
              <SearchItem/>
            </MainHeaderItem>
            <MainHeaderItem>
              <NewQueryButton/>
            </MainHeaderItem>
          </div>
          <div className='right-part' style={styles.rightPart}>
            {config.displayTutorialsLink &&
              <MainHeaderItem>
                <div className='linkIcon'>
                  <a href='https://www.dremio.com/tutorials/' target='_blank'>
                    <div className='headerLinkContent'>
                      <div className='tutorialsIcon dremioIcon-HeaderTutorials icon-type' title={'Dremio Tutorials'}></div>
                    </div>
                  </a>
                </div>
              </MainHeaderItem>
            }
            <MainHeaderItem>
              <HeaderDropdown
                icon='helpIcon dremioIcon-HeaderHelp icon-type'
                tooltip='Help'
                hideArrow
                menu={<HelpMenu />}/>
            </MainHeaderItem>

            {this.renderAdmin(styles)}

            <div className='headerSeparator'></div>

            <MainHeaderItem>
              <HeaderDropdown
                dataQa='logout-menu'
                name={user.get('userName')}
                menu={<AccountMenu />}
                arrowStyle={styles.arrowStyle}
                nameStyle={styles.nameStyle}
              />
            </MainHeaderItem>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = state => ({
  user: state.account.get('user'),
  socketIsOpen: state.serverStatus.get('socketIsOpen')
});

export default connect(mapStateToProps)(MainHeader);

const styles = {
  logo: {
    margin: '5px 0 0 7px'
  },
  logoIcon: {
    width: 115,
    height: 36,
    position: 'relative',
    top: -2,
    left: -4
  },
  rightPart: {
    margin: '0 4px 0 0'
  },
  arrowStyle: {
    color: '#77818f'
  },
  settingArrowStyle: {
    marginTop: -3
  },
  nameStyle: {
    fontSize: 13
  },
  dropdownSettingsStyle: {
    marginTop: 6
  }
};
