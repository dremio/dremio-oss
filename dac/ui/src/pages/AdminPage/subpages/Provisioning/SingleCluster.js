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
import { connect } from 'react-redux';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { getViewState } from 'selectors/resources';
import FontIcon from 'components/Icon/FontIcon';
import NumberFormatUtils from 'utils/numberFormatUtils';
import Message from 'components/Message';
import ClusterWorkers from './ClusterWorkers';


const YARN_HOST_PROPERTY = 'yarn.resourcemanager.hostname';

export class SingleCluster extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    removeProvision: PropTypes.func,
    editProvision: PropTypes.func,
    changeProvisionState: PropTypes.func,
    viewState: PropTypes.instanceOf(Immutable.Map)
  }

  getClusterCPUCores() {
    return this.props.entity.get('virtualCoreCount');
  }

  getClusterHostName() {
    return this.props.entity.get('subPropertyList')
      .find((i) => i.get('key') === YARN_HOST_PROPERTY).get('value');
  }

  getClusterRAM() {
    return NumberFormatUtils.roundNumberField(this.props.entity.get('memoryMB') / 1024);
  }

  getWorkerInfoTitle() {
    const hostName = this.getClusterHostName();
    const cores = this.getClusterCPUCores();
    const memory = this.getClusterRAM();
    return (
      <span>
        {hostName}
        <span style={{padding: '0 .5em'}}>|</span>
        {cores} {cores === 1 ? la('core') : la('cores')}, {memory}{'GB'} {la('memory per worker')}
      </span>
    );
  }

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
          isDismissable={false}
        />
      );
    }
    return null;
  }

  render() {
    const { entity, editProvision, viewState } = this.props;
    const clusterInfo = this.getWorkerInfoTitle();

    return (
      <div style={styles.base}>
        <ViewStateWrapper viewState={viewState}>
          {this.renderClusterState()}
          <div style={styles.title}>
            <span className='h3'>{entity.get('clusterType')}</span>
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
  }
};
