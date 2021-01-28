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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import { EngineStatusIcon } from '@app/pages/AdminPage/subpages/Provisioning/components/EngineStatus';
import { getEngineStatusCounts } from 'dyn-load/pages/AdminPage/subpages/Provisioning/provisioningUtils';
import { getItems } from '@inject/pages/AdminPage/subpages/Provisioning/components/singleEngine/EngineStatusBarMixin';
export function EngineStatusBar(props) {
  const { engine } = props;

  if (!engine) return null;

  const statusCounts = getEngineStatusCounts(engine);

  return (<div style={styles.statusBar}>
    {getItems(statusCounts).map(item => <div key={item.status} style={styles.statusItem}>
      <EngineStatusIcon status={item.status} style={styles.statusIcon}/>{item.label}: {item.count}
    </div>)}
  </div>);
}

EngineStatusBar.propTypes = {
  engine: PropTypes.instanceOf(Immutable.Map)
};

const styles = {
  statusBar: {
    display: 'flex',
    margin: '10px 0'
  },
  statusItem: {
    marginRight: 30
  },
  statusIcon: {
    margin: '0 3px -8px 0'
  }
};
