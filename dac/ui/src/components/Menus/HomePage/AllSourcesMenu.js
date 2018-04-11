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
import Immutable from 'immutable';

import { removeSource } from 'actions/resources/sources';
import { showConfirmationDialog } from 'actions/confirmation';

import AllSourcesMenuMixin from 'dyn-load/components/Menus/HomePage/AllSourcesMenuMixin';

@AllSourcesMenuMixin
export class AllSourcesMenu extends Component {
  static propTypes = {
    source: PropTypes.instanceOf(Immutable.Map).isRequired,
    closeMenu: PropTypes.func.isRequired,
    removeSource: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func.isRequired
  }

  static contextTypes = {
    location: PropTypes.object
  }

  handleRemoveSource = () => {
    const {source, closeMenu} = this.props;
    this.props.showConfirmationDialog({
      title: la('Remove Source'),
      text: la('Are you sure you want to remove this source?'),
      confirmText: la('Remove'),
      confirm: () => this.props.removeSource(source)
    });
    closeMenu();
  }
}

export default connect(null, {
  removeSource,
  showConfirmationDialog
})(AllSourcesMenu);
