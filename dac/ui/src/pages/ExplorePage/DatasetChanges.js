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
import { connect }   from 'react-redux';
import { withRouter } from 'react-router';

import { getDataset, getHistoryFromLocation, getExploreState } from '@app/selectors/explore';
import { isSqlChanged } from '@app/sagas/utils';


const mapStateToProp = (state, ownProps) => {
  const { location } = ownProps;// withRouter is required for this
  const isNewQuery = location.pathname === '/new_query';
  const { query } = location || {};

  let datasetSql = '';

  if (!isNewQuery) {
    const dataset = getDataset(state, query.version);
    if (dataset) {
      datasetSql = dataset.get('sql');
    }
  }

  return {
    datasetSql,
    history: getHistoryFromLocation(state, location),
    currentSql: getExploreState(state).view.currentSql
  };
};

// adds getDatasetChangeDetails to childComp properties
// getDatasetChangeDetails: () => {
//   sqlChanged: bool,
//   historyChanged: bool
// }
export class DatasetChangesView extends Component {
  static propTypes = {
    currentSql: PropTypes.string,
    datasetSql: PropTypes.string,
    history: PropTypes.instanceOf(Immutable.Map),
    childComp: PropTypes.any
  };

  hasChanges = () => {
    const { datasetSql, currentSql, history } = this.props;
    return {
      // leaving modified sql?
      // currentSql === null means sql is unchanged.
      sqlChanged: isSqlChanged(datasetSql, currentSql),
      historyChanged: history ? history.get('isEdited') : false
    };
  }

  render() {
    const {
      childComp: ChildComponent,
      ...rest
     } = this.props;
    return <ChildComponent getDatasetChangeDetails={this.hasChanges} {...rest} />;
  }
}

//withRouter is required for mapStateToProps
export const DatasetChanges = withRouter(connect(mapStateToProp)(DatasetChangesView));

export const withDatasetChanges = childComp => (props) => {
  return <DatasetChanges childComp={childComp} {...props} />;
};
