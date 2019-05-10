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
import { connect } from 'react-redux';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import { getHistoryItems } from 'selectors/explore';

import HistoryLine from './HistoryLine';

@pureRender
@Radium
export class HistoryLineController extends Component {

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    historyItems: PropTypes.instanceOf(Immutable.List),
    location: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { dataset, historyItems, location } = this.props;
    return (
      <HistoryLine
        location={location}
        historyItems={historyItems}
        tipVersion={dataset.get('tipVersion')}
        activeVersion={dataset.get('datasetVersion')}
      />
    );
  }
}

function mapStateToProps(state, ownProps) {
  return {
    historyItems: getHistoryItems(state, ownProps.dataset.get('tipVersion'))
  };
}

export default connect(mapStateToProps)(HistoryLineController);
