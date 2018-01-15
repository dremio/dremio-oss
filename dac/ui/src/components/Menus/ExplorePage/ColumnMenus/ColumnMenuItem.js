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
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import menuUtils from 'utils/menuUtils';

import MenuItem from './../MenuItem';

@pureRender

export default class ColumnMenuItem extends Component {
  static propTypes = {
    disabled: PropTypes.bool,
    columnType: PropTypes.string,
    availableTypes: PropTypes.array,
    title: PropTypes.string,
    onClick: PropTypes.func,
    actionType: PropTypes.string
  };

  render() {
    const { disabled, columnType, availableTypes, title, onClick, actionType } = this.props;
    const showState = menuUtils.getShowState({disabled, columnType, availableTypes, actionType});
    if (showState === 'none') {
      return null;
    }
    const action = disabled ? () => {} : onClick.bind(this, actionType);
    return (
      <MenuItem disabled={showState === 'disabled'} onClick={action}>
        {title}
      </MenuItem>
    );
  }
}
