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
import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { connect } from 'react-redux';

import { getViewState } from '@app/selectors/resources';
import ViewStateWrapper from '@app/components/ViewStateWrapper';
import FontIcon from '@app/components/Icon/FontIcon';
import Message from '@app/components/Message';
import NumberFormatUtils from '@app/utils/numberFormatUtils';
import timeUtils from '@app/utils/timeUtils';
import { AWS_REGION_OPTIONS } from '@app/constants/provisioningPage/provisioningConstants';
import SingleClusterMixin from 'dyn-load/pages/AdminPage/subpages/Provisioning/SingleClusterMixin';

import ClusterWorkers from './ClusterWorkers';


const YARN_HOST_PROPERTY = 'yarn.resourcemanager.hostname';
const YARN_NODE_TAG_PROPERTY = 'services.node-tag';

@SingleClusterMixin
export class SingleCluster extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    removeProvision: PropTypes.func,
    editProvision: PropTypes.func,
    changeProvisionState: PropTypes.func,
    viewState: PropTypes.instanceOf(Immutable.Map)
  };

  getClusterCPUCores() {
    return this.props.entity.getIn(['yarnProps', 'virtualCoreCount']);
  }

  getClusterSubProperty(propName) {
    const subProperty = this.props.entity.getIn(['yarnProps', 'subPropertyList'])
      .find((i) => i.get('key') === propName);
    return subProperty && subProperty.get('value');
  }

  getClusterRAM() {
    return NumberFormatUtils.roundNumberField(this.props.entity.getIn(['yarnProps', 'memoryMB']) / 1024);
  }

  getWorkerInfoTitle() {
    const hostName = this.getClusterSubProperty(YARN_HOST_PROPERTY);
    const cores = this.getClusterCPUCores();
    const memory = this.getClusterRAM();
    return (
      <span>
        {hostName}
        {this.getVerticalDivider()}
        {cores} {cores === 1 ? la('core') : la('cores')}, {memory}{'GB'} {la('memory per worker')}
      </span>
    );
  }

  getVerticalDivider = () => <span style={{padding: '0 .5em'}}>|</span>;

  getIsInReadOnlyState() {
    const { entity } = this.props;
    const currentState = entity.get('currentState');
    return (currentState === 'STARTING'
      || currentState === 'STOPPING'
      || entity.get('desiredState') === 'DELETED');
  }

  renderClusterState() {
    const { entity } = this.props;
    if (entity.has('error')) {
      const message = Immutable.Map({
        errorMessage: entity.get('error'),
        moreInfo: entity.get('detailedError')
      });
      return (
        <Message
          style={{marginBottom: 10}}
          messageType='error'
          message={message}
          isDismissable
        />
      );
    }
    return null;
  }

  renderRuntimeStatus() {
    const { entity } = this.props;
    if (entity.get('clusterType') !== 'EC2') return null;

    const stateForTime = 'Start'; //TODO TBD
    const timeValue = entity.get('timestamp'); //TODO TBD
    const region = entity.getIn(['awsProps', 'connectionProps', 'endpoint'])
      || this.getRegionLabel(entity.getIn(['awsProps', 'connectionProps', 'region']))
      || la('Invalid region');
    const extraPart = this.renderExtraRuntimeStatus(entity); //defined in mixin
    return (
      <div style={styles.runtimeStatus}>
        <span>
          {la('AWS Region: ')}{region}
          {extraPart}
        </span>
        {timeValue &&
        <span style={styles.runtimeTime}>
          {`${stateForTime} Time: `}
          {timeUtils.formatTime(timeValue)}
        </span>
        }
      </div>
    );
  }

  getRegionLabel = region => {
    const option = AWS_REGION_OPTIONS.find(r => r.value === region);
    return option && option.label;
  };

  isYarn = entity => {
    return entity.get('clusterType') === 'YARN';
  };

  getClusterName = entity => {
    return entity.get('name')
      || this.isYarn(entity) && this.getClusterSubProperty(YARN_NODE_TAG_PROPERTY)
      || entity.get('clusterType');
  };

  render() {
    const { entity, editProvision, viewState } = this.props;
    const clusterInfo = this.isYarn(entity) && this.getWorkerInfoTitle() || null;
    const clusterName = this.getClusterName(entity);

    return (
      <div style={styles.base}>
        <ViewStateWrapper viewState={viewState}>
          {this.renderClusterState()}
          <div style={styles.title}>
            <span className='h3'>{clusterName}</span>
            <span style={styles.clusterInfoTitle}>
              {clusterInfo}
            </span>
            <span style={styles.titleActions}>
              {!this.getIsInReadOnlyState() &&
                <FontIcon
                  type='EditSmall'
                  iconStyle={styles.editIcon}
                  onClick={() => editProvision(entity)}
                />
              }
              {
                <FontIcon
                  type='Trash'
                  style={{cursor: 'pointer'}}
                  onClick={this.props.removeProvision.bind(null, entity)}
                />
              }
            </span>
          </div>
          {this.renderRuntimeStatus()}
          <ClusterWorkers
            viewId={viewState.get('viewId')}
            entity={entity}
            changeProvisionState={this.props.changeProvisionState}
            readonly={this.getIsInReadOnlyState()}/>
        </ViewStateWrapper>
      </div>
    );
  }
}

function mapStateToProps(state, props) {
  return { viewState: getViewState(state, props.entity.get('id')) };
}

export default connect(mapStateToProps, {})(SingleCluster);

const styles = {
  base: {
    marginTop: 10
  },
  title: {
    marginBottom: 10
  },
  editIcon: {
    height: 16,
    width: 20,
    cursor: 'pointer',
    marginRight: 5
  },
  clusterInfoTitle: {
    marginLeft: 5,
    color: '#9a9a9a'
  },
  titleActions: {
    marginLeft: 10
  },
  runtimeStatus: {
    color: '#9a9a9a',
    width: 614,
    marginTop: -5,
    marginBottom: 10,
    display: 'flex',
    justifyContent: 'space-between'
  }
};
