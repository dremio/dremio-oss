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

import SingleCluster from './SingleCluster';

export default class ClusterListView extends Component {
  static propTypes = {
    provisions: PropTypes.instanceOf(Immutable.List),
    removeProvision: PropTypes.func,
    editProvision: PropTypes.func,
    changeProvisionState: PropTypes.func
  }

  static defaultProps = {
    provisions: Immutable.List()
  }

  render() {
    return (
      <div>
        {this.props.provisions.map((item, i) =>
          <SingleCluster
            key={i}
            entity={item}
            removeProvision={this.props.removeProvision}
            editProvision={this.props.editProvision}
            changeProvisionState={this.props.changeProvisionState}
          />
        )}
        <div style={{marginTop: 10}}>
          {la('Provisioning containers and starting up Dremio can take up to five minutes.')}
        </div>
      </div>
    );
  }
}
