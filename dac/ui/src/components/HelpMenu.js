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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { injectIntl } from 'react-intl';

import { getLocation } from 'selectors/routing';

import Menu from 'components/Menus/Menu';
import MenuItemLink from 'components/Menus/MenuItemLink';

@injectIntl
export class HelpMenu extends Component {
  static propTypes = {
    closeMenu: PropTypes.func.isRequired,
    location: PropTypes.object.isRequired,
    intl: PropTypes.object.isRequired
  };

  render() {
    const { closeMenu, location, intl } = this.props;

    return <Menu>
      <MenuItemLink href='https://docs.dremio.com' external newWindow closeMenu={closeMenu}
        text={intl.formatMessage({ id: 'HeaderMenu.Documentation' })} />
      <MenuItemLink href='https://community.dremio.com' external newWindow closeMenu={closeMenu}
        text={intl.formatMessage({ id: 'HeaderMenu.CommunitySite' })} />
      <MenuItemLink href={{...location, state: {modal: 'AboutModal'}}} closeMenu={closeMenu}
        text={intl.formatMessage({ id: 'App.AboutHeading' })} />
    </Menu>;
  }
}

const mapStateToProps = state => ({
  location: getLocation(state)
});

export default connect(mapStateToProps)(HelpMenu);
