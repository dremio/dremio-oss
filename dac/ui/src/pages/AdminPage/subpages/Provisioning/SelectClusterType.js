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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import SelectConnectionButton from 'components/SelectConnectionButton';

@Radium
export default class SelectClusterType extends Component {

  static propTypes = {
    style: PropTypes.object,
    clusters: PropTypes.instanceOf(Immutable.List).isRequired,
    onSelectClusterType: PropTypes.func.isRequired
  };

  handleSelectClusterType(cluster) {
    if (!cluster.get('connected')) {
      return;
    }
    this.props.onSelectClusterType(cluster.get('clusterType'));
  }

  renderClusters() {
    const { clusters } = this.props;
    return clusters.map((cluster) => (
      <SelectConnectionButton
        key={cluster.get('clusterType')}
        label={cluster.get('label')}
        iconType={`provisionManagers/${cluster.get('iconType')}`}
        disabled={!cluster.get('connected')}
        pillText={cluster.get('connected') ? '' : la('coming soon')}
        onClick={this.handleSelectClusterType.bind(this, cluster)}
      />
    ));
  }

  render() {
    const { style } = this.props;
    return (
      <div style={style}>
        <div style={{maxWidth: 720, display: 'flex', flexWrap: 'wrap'}}>
          {this.renderClusters()}
        </div>
      </div>
    );
  }
}
