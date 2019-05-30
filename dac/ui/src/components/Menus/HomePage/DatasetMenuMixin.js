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
import Menu from 'components/Menus/Menu';
import MenuItem from 'components/Menus/MenuItem';
import DividerHr from 'components/Menus/DividerHr';
import MenuItemLink from 'components/Menus/MenuItemLink';

import AnalyzeMenuItem from 'components/Menus/HomePage/AnalyzeMenuItem';

import { abilities } from 'utils/datasetUtils';

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    getGraphLink() {
      return null;
    },

    render() {
      const { entity, closeMenu, entityType } = this.props;

      const { canRemoveFormat, canEdit, canMove, canDelete } = abilities(entity, entityType);

      return <Menu>
        {
          <MenuItemLink
            href={entity.getIn(['links', 'query'])}
            text={la('Query')}
            closeMenu={closeMenu}/>
        }

        {
          // feature has a bug see DX-7054
          /*
          entityType === 'folder' && <MenuItemLink
            text={la('Browse Contents')}
            href={entity.getIn(['links', 'self'])}
            closeMenu={closeMenu} />
          */
        }

        {
          canEdit && <MenuItemLink
            href={entity.getIn(['links', 'edit'])} text={la('Edit')}/>
        }

        {
          <MenuItemLink href={this.getMenuItemUrl('wiki')} text={la('Catalog')}/>
        }

        {
          // EE has data graph menu item here
        }

        { <AnalyzeMenuItem entity={entity} closeMenu={closeMenu} /> }

        <DividerHr />

        {
          canDelete &&
          (entityType !== 'file' && <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRemoveLocation()}
              text={la('Remove')}/> ||
            entityType === 'file' && <MenuItem
              onClick={this.handleRemoveFile}>
              {la('Remove')}
              </MenuItem>
          )
        }

        {
          canMove && <MenuItemLink
            closeMenu={closeMenu}
            href={this.getRenameLocation()}
            text={la('Rename')}/>
        }

        {
          canMove && <MenuItemLink
            closeMenu={closeMenu}
            href={this.getMoveLocation()}
            text={la('Move')}/>
        }

        <MenuItem onClick={this.copyPath}>{la('Copy Path')}</MenuItem>

        <DividerHr />

        <MenuItemLink
          closeMenu={closeMenu}
          href={this.getSettingsLocation()}
          text={la('Settings')}/>

        {
          canRemoveFormat && <MenuItemLink
            closeMenu={closeMenu}
            href={this.getRemoveFormatLocation()}
            text={la('Remove Format')}/>
        }
      </Menu>;
    }
  });
}
