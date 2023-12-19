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
import { Component } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { connect } from "react-redux";
import { withRouter } from "react-router";

import {
  getDataset,
  getHistoryFromLocation,
  getExploreState,
} from "@app/selectors/explore";
import { isSqlChanged } from "@app/sagas/utils";
import { isNewQueryUrl } from "@app/utils/explorePageTypeUtils";

const mapStateToProp = (state, ownProps) => {
  const { location } = ownProps; // withRouter is required for this
  // urlability
  const { query } = location || {};
  const isNewQuery = isNewQueryUrl(location);
  const explorePageState = getExploreState(state);

  let datasetSql = "";

  if (!isNewQuery) {
    const dataset = getDataset(state, query.version);
    if (dataset) {
      datasetSql = dataset.get("sql");
    }
  }

  return {
    datasetSql,
    history: getHistoryFromLocation(state, location),
    currentSql: explorePageState.view.currentSql,
    customDefaultSql: explorePageState.view.customDefaultSql || "",
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
    customDefaultSql: PropTypes.string,
    datasetSql: PropTypes.string,
    history: PropTypes.instanceOf(Immutable.Map),
    childComp: PropTypes.any,
  };

  hasChanges = () => {
    const { datasetSql, currentSql, customDefaultSql, history } = this.props;
    return {
      // leaving modified sql?
      // currentSql === null means sql is unchanged.

      // if viewing a dataset, compare currentSql to datasetSql
      // if querying a dataset, compare currentSql to pre-populated "SELECT * FROM ..."
      // otherwise compare currentSql to an empty string
      sqlChanged: isSqlChanged(datasetSql || customDefaultSql, currentSql),
      historyChanged: history ? history.get("isEdited") : false,
    };
  };

  render() {
    const { childComp: ChildComponent, ...rest } = this.props;
    return (
      <ChildComponent getDatasetChangeDetails={this.hasChanges} {...rest} />
    );
  }
}

//withRouter is required for mapStateToProps
export const DatasetChanges = withRouter(
  connect(mapStateToProp)(DatasetChangesView)
);

export const withDatasetChanges = (childComp) => (props) => {
  return <DatasetChanges childComp={childComp} {...props} />;
};
