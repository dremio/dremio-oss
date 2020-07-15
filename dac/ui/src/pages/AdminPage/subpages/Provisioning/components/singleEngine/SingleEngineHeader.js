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
import Header from '@app/pages/AdminPage/components/Header';
import { YARN_NODE_TAG_PROPERTY } from '@app/pages/AdminPage/subpages/Provisioning/ClusterListView';
import { isYarn, getEntityName } from '@app/pages/AdminPage/subpages/Provisioning/provisioningUtils';
import EngineStatus from '@app/pages/AdminPage/subpages/Provisioning/components/EngineStatus';
import { StartStopButton } from '@app/pages/AdminPage/subpages/Provisioning/components/EngineActionCell';
import {CLUSTER_STATE} from '@app/constants/provisioningPage/provisioningConstants';
import Button from '@app/components/Buttons/Button';
import * as ButtonTypes from '@app/components/Buttons/ButtonTypes';

export const VIEW_ID = 'EngineHeader';

export class SingleEngineHeader extends PureComponent {
  static propTypes = {
    engine: PropTypes.instanceOf(Immutable.Map),
    unselectEngine: PropTypes.func,
    handleEdit: PropTypes.func,
    handleStartStop: PropTypes.func
  };

  onStartStop = () => {
    const { engine, handleStartStop } = this.props;
    const nextState = engine.get('currentState') === CLUSTER_STATE.running ?
      CLUSTER_STATE.stopped : CLUSTER_STATE.running;
    handleStartStop(nextState, engine, VIEW_ID);
  };

  onEdit = () => {
    const { engine, handleEdit } = this.props;
    handleEdit(engine);
  };

  render() {
    const { engine } = this.props;
    const doubleCaretIcon = <div style={styles.doubleCaret}>»</div>;
    const statusIcon = <EngineStatus engine={engine} style={styles.statusIcon} />;
    const engineName = engine && getEntityName(engine, YARN_NODE_TAG_PROPERTY);
    const region = engine && !isYarn(engine) && engine.getIn(['awsProps', 'connectionProps', 'region']);
    //TODO enhancement: show spinner while start/stop inProgress
    const startStopButton = <StartStopButton
      engine={engine}
      handleStartStop={this.onStartStop}
      style={styles.startStop}
      textStyle={{width: 65}}
    />;
    const editButton = <Button
      style={styles.edit}
      onClick={this.onEdit}
      type={ButtonTypes.NEXT}
      text={la('Edit Settings')}
    />;

    return (
      <Header endChildren={
        <div style={{display: 'flex'}}>{startStopButton} {editButton}</div>
      }>
        <div  style={styles.lefChildren}>
          <div className='link' onClick={this.props.unselectEngine}>{la('Engines')}</div>
          {doubleCaretIcon} {statusIcon} {engineName}
          {region && <div style={styles.region}>({region})</div>}
        </div>
      </Header>
    );
  }
}

const styles = {
  lefChildren: {
    display: 'flex',
    fontSize: 20,
    color: '#333'
  },
  doubleCaret: {
    padding: '0 6px'
  },
  statusIcon: {
    marginRight: 5
  },
  startStop: {
    marginRight: 10,
    marginTop: 5,
    height: 28,
    width: 100,
    paddingTop: 3
  },
  edit: {
    width: 100,
    marginTop: 5
  },
  region: {
    marginLeft: 10
  }

};
