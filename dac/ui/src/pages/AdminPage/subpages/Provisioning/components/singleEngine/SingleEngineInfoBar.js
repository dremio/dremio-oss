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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import { isYarn, getYarnSubProperty, getEngineSizeLabel } from '@app/pages/AdminPage/subpages/Provisioning/provisioningUtils';
import SingleEngineInfoBarMixin from 'dyn-load/pages/AdminPage/subpages/Provisioning/components/singleEngine/SingleEngineInfoBarMixin';
import NumberFormatUtils from '@app/utils/numberFormatUtils';
import timeUtils from '@app/utils/timeUtils';
import { YARN_HOST_PROPERTY } from '@app/pages/AdminPage/subpages/Provisioning/ClusterListView';
import { CLUSTER_STATE_ICON } from '@app/constants/provisioningPage/provisioningConstants';
import { EDITION } from '@inject/constants/serverStatus';
import * as VersionUtils from '@app/utils/versionUtils';

@SingleEngineInfoBarMixin
export class SingleEngineInfoBar extends PureComponent {
  static propTypes = {
    engine: PropTypes.instanceOf(Immutable.Map)
  };

  getEntries(engine) {
    const yarnMode = isYarn(engine);
    return [
      {label: la('SIZE'), value: this.getSize(engine)},
      yarnMode && {label: la('CORES PER WORKER'), value: this.getCores(engine)},
      yarnMode && {label: la('MEMORY PER WORKER'), value: this.getMemory(engine)},
      yarnMode && {label: la('IP ADDRESS'), value: this.getIp(engine)},
      !yarnMode && {label: la('ENGINE TYPE'), value: this.getEngineType(engine)},
      {label: la('LAST STATUS CHANGE'), value: this.getLastChange(engine)}
    ].filter(Boolean);
  }

  getSize = engine => {
    const nodeCount = engine.getIn(['dynamicConfig', 'containerCount']);
    if (VersionUtils.getEditionFromConfig() === EDITION.ME) {
      return getEngineSizeLabel(nodeCount);
    } else if (isYarn(engine)) {
      return nodeCount;
    } else {
      return engine.get('size');
    }
  };

  getCores = engine => (engine.getIn(['yarnProps', 'virtualCoreCount']) || '-');

  getMemory = engine => NumberFormatUtils.roundNumberField(engine.getIn(['yarnProps', 'memoryMB']) / 1024);

  getIp = engine => getYarnSubProperty(engine, YARN_HOST_PROPERTY) || '-';

  getEngineType = engine => {
    if (VersionUtils.getEditionFromConfig() === EDITION.ME || isYarn(engine)) {
      return (engine.getIn(['awsProps', 'instanceType']) || '-');
    } else {
      return engine.get('instanceType');
    }
  }

  getLastChange = engine => {
    const status = engine.get('currentState');
    const dateTime = engine.get('stateChangeTime');
    if (!status || !dateTime) return '-';
    const statusIcon = CLUSTER_STATE_ICON[status];
    const statusLabel = statusIcon ? statusIcon.text : la('N/A');
    const dateLabel = timeUtils.formatTime(dateTime);
    return `${statusLabel} ${'since'} ${dateLabel}`;
  };

  getLastQueried = engine => {
    const value = engine.get('lastQueried');
    if (!value) return '-';
    return timeUtils.fromNow(value);
  };



  render() {
    const { engine } = this.props;
    const entries = this.getEntries(engine);

    return (
      <div style={styles.container}>
        {entries.map(entry => <div key={entry.label} style={styles.entry}>
          <div style={styles.label}>{entry.label}</div>
          <div style={styles.value}>{entry.value}</div>
        </div>)}
      </div>
    );
  }
}

const styles = {
  container: {
    display: 'flex',
    backgroundColor: '#f3f3f3',
    padding: 8
  },
  entry: {
    marginRight: 50,
    padding: 2
    //flex: 'auto'
  },
  label: {
    fontSize: 12,
    fontWeight: 400,
    color: '#77818F',
    padding: '2px 0'
  },
  value: {
    fontSize: 14,
    fontWeight: 300,
    color: '#333333'
  }
};
