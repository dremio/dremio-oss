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
import { Component } from 'react';
import { connect } from 'react-redux';
import { Link } from 'react-router';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { FormattedMessage, injectIntl } from 'react-intl';

import FontIcon from 'components/Icon/FontIcon';

import ChatItem from 'dyn-load/components/HeaderItemsTypes/ChatItem';

import MainHeaderItem from './MainHeaderItem';
import HeaderLink from './HeaderItemsTypes/HeaderLink';
import SearchItem from './HeaderItemsTypes/SearchItem';
import NewQueryButton from './HeaderItemsTypes/NewQueryButton';
import HeaderDropdown from './HeaderItemsTypes/HeaderDropdown';
import AccountMenu from './AccountMenu';
import HelpMenu from './HelpMenu';

import './MainHeader.less';

@injectIntl
@Radium
export class MainHeader extends Component {

  static propTypes = {
    user: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired
  };

  static defaultProps = {
    user: Immutable.Map()
  };

  render() {
    const { user } = this.props;
    return (
      <div className='explore-header'>
        <Link
          className='dremio'
          to='/'
          style={styles.logo}>
          <FontIcon
            type='NarwhalLogoWithNameLight'
            theme={styles.logoIcon} />
        </Link>
        <div className='header-wrap'>
          <div className='left-part'>
            <MainHeaderItem>
              <HeaderLink to='/'><FormattedMessage id='Dataset.Datasets'/></HeaderLink>
            </MainHeaderItem>
            <MainHeaderItem>
              <HeaderLink to='/jobs'><FormattedMessage id='Job.Jobs'/></HeaderLink>
            </MainHeaderItem>
            <MainHeaderItem>
              <SearchItem/>
            </MainHeaderItem>
            <MainHeaderItem>
              <NewQueryButton/>
            </MainHeaderItem>
          </div>
          <div className='right-part' style={styles.rightPart}>
            {ChatItem && <ChatItem />}
            <MainHeaderItem>
              <HeaderDropdown
                name={this.props.intl.formatMessage({id: 'App.Help'})}
                menu={<HelpMenu />}/>
            </MainHeaderItem>
            {
              user.get('admin') &&
                <MainHeaderItem>
                  <HeaderLink to='/admin'><FormattedMessage id='App.Admin'/></HeaderLink>
                </MainHeaderItem>
            }
            <MainHeaderItem>
              <HeaderDropdown
                dataQa='logout-menu'
                name={user.get('userName')}
                menu={<AccountMenu />}/>
            </MainHeaderItem>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = state => ({
  user: state.account.get('user')
});

export default connect(mapStateToProps)(MainHeader);

const styles = {
  logo: {
    margin: '5px 0 0 7px'
  },
  logoIcon: {
    'Icon': {
      'width': 115,
      'height': 36
    }
  },
  rightPart: {
    margin: '0 4px 0 0'
  }
};
