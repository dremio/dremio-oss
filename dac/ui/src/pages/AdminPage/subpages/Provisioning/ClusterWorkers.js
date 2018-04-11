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
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { startCase } from 'lodash';

import Button from 'components/Buttons/Button';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import { formDescription } from 'uiTheme/radium/typography';
import { PALE_BLUE, PALE_NAVY } from 'uiTheme/radium/colors';
import { openMoreInfoProvisionModal } from 'actions/resources/provisioning';
import AdjustWorkers from './components/AdjustWorkers';
import ResourceSummary from './components/ResourceSummary';
import WorkersGrid from './components/WorkersGrid';

const buttonTextForCurrentState = {
  RUNNING: 'Stop',
  STOPPED: 'Start',
  FAILED: 'Start',
  CREATED: null,
  DELETED: null,
  STARTING: null,
  STOPPING: null
};

// TODO: add loc
@Radium
export class ClusterWorkers extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    openMoreInfoProvisionModal: PropTypes.func,
    changeProvisionState: PropTypes.func,
    viewId: PropTypes.string,
    readonly: PropTypes.bool
  }

  openMoreInfoProvisionModal = () => {
    const provisionId = this.props.entity.get('id');
    this.props.openMoreInfoProvisionModal(provisionId);
  }

  toggleClusterState = () => {
    const { entity, viewId } = this.props;
    this.props.changeProvisionState(entity.get('currentState') === 'RUNNING' ? 'STOPPED' : 'RUNNING', entity, viewId);
  }

  render() {
    const { entity, readonly } = this.props;
    const totalWorkers = entity.getIn(['workersSummary', 'total']);
    const {desiredState, currentState} = entity.toJS();

    // TODO: add loc
    let status = 'Status: ' + startCase(currentState.toLowerCase());
    mixed: if (currentState !== desiredState) {
      if (desiredState === 'RUNNING' && currentState === 'STARTING') {
        break mixed;
      }
      if (desiredState === 'STOPPED' && currentState === 'STOPPING') {
        break mixed;
      }
      if (desiredState === 'RUNNING' && currentState === 'STOPPING') {
        status = 'Status: Restarting';
        break mixed;
      }
      if (desiredState === 'DELETED' && currentState === 'STOPPING') {
        status = 'Status: Deleting';
        break mixed;
      }
      status = 'Target: ' + startCase(desiredState.toLowerCase());
      status += ' (Currently: ' + startCase(currentState.toLowerCase()) + ')';
    }

    return (
      <div style={styles.base}>
        <div style={styles.title}>
          <div>
            <span>{totalWorkers} {totalWorkers !== 1 ? la('Workers') : la('Worker')}</span>
            <span style={formDescription}>{' (' + la('requested') + ')'} </span>
          </div>
          <div style={{display: 'flex', alignItems: 'center'}}>
            <span style={{ marginRight: 10 }}>{status}</span>
            {buttonTextForCurrentState[currentState] &&
              <Button
                onClick={this.toggleClusterState}
                style={{ marginTop: 4 }}
                type={ButtonTypes.NEXT}
                text={la(buttonTextForCurrentState[currentState])}
              />
            }
          </div>
        </div>
        <div style={styles.body}>
          <div style={styles.leftSide}>
            <ResourceSummary entity={entity} />
            <AdjustWorkers entity={entity} readonly={readonly} />
          </div>
          <div style={styles.rightSide}>
            <WorkersGrid entity={entity} />
            <div style={styles.buttonWrap}>
              <Button
                onClick={this.openMoreInfoProvisionModal}
                style={{width: 100}}
                type={ButtonTypes.NEXT}
                text={la('More Info')}
              />
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default connect(null, {
  openMoreInfoProvisionModal
})(ClusterWorkers);

const styles = {
  base: {
    width: 614,
    background: PALE_BLUE
  },
  title: {
    fontSize: 14,
    height: 38,
    fontWeight: 500,
    padding: '0 14px 0 10px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    background: PALE_NAVY
  },
  body: {
    padding: '12px 10px',
    display: 'flex'
  },
  leftSide: {
    width: 210
  },
  rightSide: {
    marginLeft: 12
  },
  buttonWrap: {
    display: 'flex',
    justifyContent: 'flex-end'
  }
};
