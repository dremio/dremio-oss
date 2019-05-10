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
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { showConfirmationDialog } from 'actions/confirmation';
import menuUtils from 'utils/menuUtils';
import Menu from 'components/Menus/Menu';
import MenuItem from 'components/Menus/MenuItem';
import MenuItemLink from 'components/Menus/MenuItemLink';
import { EntityLinkProvider } from '@app/pages/HomePage/components/EntityLink';

import { removeSource } from 'actions/resources/sources';
import { RestrictedArea } from '@app/components/Auth/RestrictedArea';
import { manageSourceRule } from '@app/utils/authUtils';

export class AllSourcesMenu extends Component {

  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    closeMenu: PropTypes.func.isRequired,
    removeItem: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func.isRequired
  }
  static contextTypes = {
    location: PropTypes.object
  }

  handleRemoveSource = () => {
    menuUtils.showConfirmRemove(this.props);
  }

  render() {
    const { item, closeMenu } = this.props;
    const { location } = this.context;
    const name = item.get('name');
    return (
      <Menu>
        {
          <EntityLinkProvider entityId={item.get('id')}>
            {(link) => (
              <MenuItemLink
                href={link}
                text={la('Browse')}
                closeMenu={closeMenu}
              />
            )}
          </EntityLinkProvider>
        }
        <RestrictedArea rule={manageSourceRule}>
          <MenuItemLink
            href={{
              ...location,
              state: {
                modal: 'EditSourceModal',
                query: {
                  name,
                  type: item.get('type')
                }
              }
            }}
            text={la('Edit Details')}
            closeMenu={closeMenu}
          />
          <MenuItem onClick={this.handleRemoveSource}>{la('Remove Source')}</MenuItem>
        </RestrictedArea>
      </Menu>
    );
  }
}

export default connect(null, {
  removeItem: removeSource,
  showConfirmationDialog
})(AllSourcesMenu);
