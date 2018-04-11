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
import Immutable from 'immutable';
import { formDescription } from 'uiTheme/radium/typography';
import NumberFormatUtils from 'utils/numberFormatUtils';

export default class ResourceSummary extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    totalRam: PropTypes.number,
    totalCores: PropTypes.number
  }

  render() {
    const { entity } = this.props;
    const totalRam = NumberFormatUtils.roundNumberField(entity.getIn(['workersSummary', 'totalRAM']) / 1024);
    const totalCores = entity.getIn(['workersSummary', 'totalCores']);
    return (
      <div>
        <div style={styles.infoRow}>
          <div style={styles.infoRowKey}>{la('Total Cores')}</div>
          <div style={{fontSize: formDescription.fontSize}}>{totalCores}</div>
        </div>
        <div style={{...styles.infoRow, marginBottom: 12}}>
          <div style={styles.infoRowKey}>{la('Total Memory')}</div>
          <div style={{fontSize: formDescription.fontSize}}>{totalRam}{'GB'}</div>
        </div>
      </div>
    );
  }
}

const styles = {
  infoRow: {
    display: 'flex',
    marginBottom: 6
  },
  infoRowKey: {
    ...formDescription,
    width: 100
  }
};
