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
import { FormattedMessage } from 'react-intl';

import MenuItem from 'components/Menus/MenuItem';
import DividerHr from 'components/Menus/DividerHr';
import { DREMIO_CONNECTOR } from 'constants/links.json';

export default class AnalyzeMenuItems extends Component {
  static propTypes = {
    openTableau: PropTypes.func,
    openQlikSense: PropTypes.func,
    openPowerBI: PropTypes.func
  };

  render() {
    return (
      <div>
        <MenuItem onClick={this.props.openTableau}><FormattedMessage id='Dataset.Tableau'/></MenuItem>
        <MenuItem onClick={this.props.openPowerBI}><FormattedMessage id='Dataset.PowerBI'/></MenuItem>
        <MenuItem onClick={this.props.openQlikSense}><FormattedMessage id='Dataset.QlikSense'/></MenuItem>
        <DividerHr />
        <MenuItem isInformational> {/* todo: loc safety (string concat) */}
          <span>
            {/* todo: loc safety (string concat) */}
            <a href={DREMIO_CONNECTOR} target='_blank'><FormattedMessage id='Dataset.DremioConnector'/></a> <FormattedMessage id='Dataset.Required'/>
          </span>
        </MenuItem>
      </div>
    );
  }
}
