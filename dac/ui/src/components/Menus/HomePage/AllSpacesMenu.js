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

import AllSpacesMenuMixin from 'dyn-load/components/Menus/HomePage/AllSpacesMenuMixin';

@AllSpacesMenuMixin
export class AllSpacesMenu extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired
  }

  static propTypes = {
    space: PropTypes.instanceOf(Immutable.Map).isRequired,
    closeMenu: PropTypes.func,
    removeSpace: PropTypes.func
  }

  handleRemoveSpace = () => {
    const {space, closeMenu} = this.props;
    this.props.removeSpace(space);
    closeMenu();
  }
}
