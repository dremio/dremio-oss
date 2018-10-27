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
import { Link } from 'react-router';
import { styles } from 'pages/HomePage/components/MainInfo';
import { getSettingsLocation } from 'components/Menus/HomePage/DatasetMenu';


export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties

    renderConvertButton(entity, folderModalButton) {
      return (
        <div className='main-settings-btn convert-to-dataset'>
          <Link
            className='settings-button'
            to={folderModalButton.to ? folderModalButton.to : '.'}
            style={styles.button}>
            {folderModalButton.icon}
          </Link>
        </div>
      );
    },

    getShortcutButtonsData(item, entityType, btnTypes) {
      const allBtns = [
        // Per DX-13304 we leave only Edit and Cog (Settings.svg) buttons
        {
          label: this.getInlineIcon('Edit.svg', 'edit'),
          link: item.getIn(['links', 'edit']),
          type: btnTypes.edit,
          isShown: entityType === 'dataset'
        },
        {
          label: this.getInlineIcon('Settings.svg', 'settings'),
          link: getSettingsLocation(this.context.location, item, entityType),
          type: btnTypes.settings,
          isShown: true
        }
      ];
      return allBtns;
    }
  });
}
