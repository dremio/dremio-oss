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

import { formLabel } from 'uiTheme/radium/typography';

import RecommendedJoins from './RecommendedJoins';

@Radium
export default class ChooseDataset extends Component {
  static propTypes = {
    currentDatasetName: PropTypes.string.isRequired,
    recommendedJoins: PropTypes.instanceOf(Immutable.List),
    activeRecommendedJoin: PropTypes.instanceOf(Immutable.Map),
    loadExploreEntities: PropTypes.func,
    location: PropTypes.object,
    fields: PropTypes.object
  };

  constructor(props) {
    super(props);
  }

  render() {
    return  (
      <div style={[styles.searchRoot]}>
        <div style={[styles.header, formLabel]}>
          <div style={styles.name}>
            Dataset
          </div>
          <div style={styles.type}>
            Join Type
          </div>
          <div style={styles.cur}>
            {la('Current Dataset Key')}
          </div>
          <div style={[styles.cur, {left: 10}]}>
            Matching Key
          </div>
        </div>
        {this.props.recommendedJoins.size > 0 && <RecommendedJoins
          fields={this.props.fields}
          recommendedJoins={this.props.recommendedJoins}
          activeRecommendedJoin={this.props.activeRecommendedJoin}
          loadExploreEntities={this.props.loadExploreEntities}
          location={this.props.location}
          previewRecommendation={this.previewRecommendation}/>}
      </div>
    );
  }
}

const styles = {
  header: {
    marginLeft: 24,
    height: 40,
    display: 'flex'
  },
  name: {
    display: 'flex',
    alignItems: 'center',
    width: '100%',
    minWidth: 300
  },
  type: {
    display: 'flex',
    alignItems: 'center',
    width: '100%',
    position: 'relative',
    left: -17,
    minWidth: 100
  },
  cur: {
    display: 'flex',
    alignItems: 'center',
    width: '100%',
    position: 'relative',
    left: -10,
    minWidth: 200
  },
  searchRoot: {
    flex: 1
  }
};
