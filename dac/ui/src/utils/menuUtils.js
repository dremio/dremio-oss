import { ENTITY_TYPES } from '@app/constants/Constants';

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
class MenuUtils {
  getShowState({disabled, columnType, availableTypes}) {
    if (availableTypes.indexOf(columnType) === -1) {
      return 'none';
    } else if (!disabled) {
      return 'block';
    }
    return 'disabled';
  }

  showConfirmRemove({item, closeMenu, showConfirmationDialog, removeItem}) {
    showConfirmationDialog({
      title: (item.get('entityType') === ENTITY_TYPES.space) ? la('Remove Space') : la('Remove Source'),
      text: la(`Are you sure you want to remove "${item.get('name')}"?`),
      confirmText: la('Remove'),
      confirm: () => removeItem(item)
    });
    closeMenu();
  }
}

const menuUtils = new MenuUtils();
export default menuUtils;
