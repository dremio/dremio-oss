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
import Menu from 'components/Menus/Menu';
import MenuItem from 'components/Menus/MenuItem';
import MenuItemLink from 'components/Menus/MenuItemLink';

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    render() {
      const { source, closeMenu } = this.props;
      const name = source.get('name');
      return (
        <Menu>
          {
            <MenuItemLink
              href={source.getIn(['links', 'self'])}
              text={la('Browse')}
              closeMenu={closeMenu}/>
          }
          {
            <MenuItemLink
              href={{...location, state: {modal: 'EditSourceModal',
                query: {name, type: source.get('type')}}}}
              text={la('Edit Details')}
              closeMenu={closeMenu}/>
          }
          {
            <MenuItem onTouchTap={this.handleRemoveSource}>{la('Remove Source')}</MenuItem>
          }
        </Menu>
      );
    }
  });
}
