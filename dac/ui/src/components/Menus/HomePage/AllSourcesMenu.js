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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { showConfirmationDialog } from 'actions/confirmation';
import menuUtils from 'utils/menuUtils';

import { removeSource } from 'actions/resources/sources';
import AllSourcesMenuMixin from 'dyn-load/components/Menus/HomePage/AllSourcesMenuMixin';

@AllSourcesMenuMixin
export class AllSourcesMenu extends PureComponent {

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
}

export default connect(null, {
  removeItem: removeSource,
  showConfirmationDialog
})(AllSourcesMenu);

