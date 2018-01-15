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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import UnformattedEntityMenuMixin from 'dyn-load/components/Menus/HomePage/UnformattedEntityMenuMixin';

@UnformattedEntityMenuMixin
export class UnformattedEntityMenu extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    closeMenu: PropTypes.func
  };

  getHrefForFormatSettings = () => ({
    ...this.context.location,
    state: {
      modal: 'DatasetSettingsModal',
      tab: 'format',
      entityType: this.props.entity.get('entityType'),
      entityId: this.props.entity.get('id'),
      fullPath: this.props.entity.get('filePath'),
      queryable: this.props.entity.get('queryable'),
      query: {then: 'query'}
    }
  })
}
