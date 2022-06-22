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
import { PureComponent } from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import exploreUtils from "utils/explore/exploreUtils";
import { PageTypes, pageTypesProp } from "@app/pages/ExplorePage/pageTypes";
import { changePageTypeInUrl } from "@app/pages/ExplorePage/pageTypeUtils";
import { collapseExploreSql } from "actions/explore/ui";
import {
  getExploreState,
  getTableColumns,
  getApproximate,
  getExplorePageDataset,
  getColumnFilter,
} from "@app/selectors/explore";
import { setQueryFilter as setQueryFilterFunc } from "@app/actions/explore/view";

import { performTransform } from "actions/explore/dataset/transform";

import TableControlsView from "./TableControlsView";

export class TableControls extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    tableColumns: PropTypes.instanceOf(Immutable.List),
    defaultColumnName: PropTypes.string, // would be used for addField button as default value
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    pageType: pageTypesProp.isRequired,
    sqlState: PropTypes.bool.isRequired,
    sqlSize: PropTypes.number.isRequired,
    toggleExploreSql: PropTypes.func,
    collapseExploreSql: PropTypes.func.isRequired,
    location: PropTypes.object.isRequired,
    rightTreeVisible: PropTypes.bool,
    approximate: PropTypes.bool,
    disableButtons: PropTypes.bool,
    jobsCount: PropTypes.number,
    showJobsTable: PropTypes.bool,
    version: PropTypes.any,
    columnCount: PropTypes.number,
    filteredColumnCount: PropTypes.number,
    columnFilter: PropTypes.any,
    setQueryFilter: PropTypes.func,
    queryTabNumber: PropTypes.any,
    queryStatuses: PropTypes.array,
    queryFilter: PropTypes.string,
    curDataset: PropTypes.instanceOf(Immutable.Map),
    curVersion: PropTypes.any,

    // actions
    performTransform: PropTypes.func.isRequired,
  };

  static contextTypes = {
    router: PropTypes.object.isRequired,
    location: PropTypes.object.isRequired,
  };

  constructor(props) {
    super(props);

    this.filterQueries = this.filterQueries.bind(this);
  }

  getLocationWithoutGraph(location) {
    let newLocation = location;

    newLocation = {
      ...newLocation,
      pathname: changePageTypeInUrl(newLocation.pathname, PageTypes.default),
    };

    return newLocation;
  }

  navigateToTransformWizard(wizardParams) {
    const { router } = this.context;
    const {
      dataset,
      currentSql,
      queryContext,
      exploreViewState,
      queryStatuses,
      queryTabNumber,
    } = this.props;

    const callback = () => {
      const locationWithoutGraph = this.getLocationWithoutGraph(
        this.props.location
      );

      router.push(
        exploreUtils.getLocationToGoToTransformWizard({
          ...wizardParams,
          location: locationWithoutGraph,
        })
      );
    };

    const sqlProp = queryStatuses[queryTabNumber - 1]
      ? queryStatuses[queryTabNumber - 1].sqlStatement
      : currentSql;

    this.props.performTransform({
      dataset,
      currentSql: sqlProp,
      queryContext,
      viewId: exploreViewState.get("viewId"),
      callback,
    });
  }

  addField = () => {
    // use first column by default for just the expression
    const defaultColumn = this.props.defaultColumnName;
    this.navigateToTransformWizard({
      detailType: "CALCULATED_FIELD",
      column: "",
      props: {
        initialValues: {
          expression: defaultColumn
            ? exploreUtils.escapeFieldNameForSQL(defaultColumn)
            : "",
        },
      },
    });
  };

  groupBy = () => {
    this.navigateToTransformWizard({ detailType: "GROUP_BY", column: "" });
  };

  join = () => {
    this.navigateToTransformWizard({
      detailType: "JOIN",
      column: "",
      location: this.context.location,
    });
  };

  preventTooltipHide() {
    clearTimeout(this.timer);
  }

  union() {}

  filterQueries(value) {
    const { setQueryFilter } = this.props;
    setQueryFilter({ term: value });
  }

  render() {
    const {
      dataset,
      sqlState,
      approximate,
      rightTreeVisible,
      exploreViewState,
      tableColumns,
      disableButtons,
      columnFilter,
      queryFilter,
      filteredColumnCount,
      columnCount,
      version,
      showJobsTable,
      jobsCount,
      curVersion,
      curDataset,
      queryStatuses,
      queryTabNumber,
    } = this.props;

    const isQuerySuccess =
      queryTabNumber > 0 &&
      queryStatuses[queryTabNumber - 1] &&
      !queryStatuses[queryTabNumber - 1].cancelled &&
      !queryStatuses[queryTabNumber - 1].error &&
      !!queryStatuses[queryTabNumber - 1].jobId;

    return (
      <TableControlsView
        dataset={curDataset || dataset}
        exploreViewState={exploreViewState}
        addField={this.addField}
        sqlState={sqlState}
        groupBy={this.groupBy.bind(this)}
        join={this.join}
        approximate={approximate}
        rightTreeVisible={rightTreeVisible}
        tableColumns={tableColumns}
        disableButtons={disableButtons}
        columnFilter={columnFilter}
        queryFilter={queryFilter}
        filteredColumnCount={filteredColumnCount}
        columnCount={columnCount}
        version={curVersion || version}
        showJobsTable={showJobsTable}
        jobsCount={jobsCount}
        filterQueries={this.filterQueries}
        isQuerySuccess={isQuerySuccess}
      />
    );
  }
}

function mapStateToProps(state, props) {
  const location = state.routing.locationBeforeTransitions || {};
  const datasetVersion = props.dataset.get("datasetVersion");
  const explorePageState = getExploreState(state);
  const queryStatuses = state.modulesState.explorePage.data.view.queryStatuses;
  const queryFilter = state.modulesState.explorePage.data.view.queryFilter;

  const currentDataset = queryStatuses[props.queryTabNumber - 1];
  const curDataset = getExplorePageDataset(
    state,
    currentDataset ? currentDataset : undefined
  );
  const version =
    currentDataset && currentDataset.version
      ? currentDataset.version
      : datasetVersion;

  const columns = getTableColumns(state, version, location);
  const columnFilter = getColumnFilter(state);

  return {
    currentSql: explorePageState.view.currentSql,
    queryContext: explorePageState.view.queryContext,
    defaultColumnName: (columns && columns.getIn([0, "name"])) || "",
    tableColumns: getTableColumns(state, version),
    approximate: getApproximate(state, version),
    queryStatuses,
    queryFilter,
    curDataset,
    curVersion: currentDataset && currentDataset.version,
    columnFilter,
    columnCount: columns.size,
    filteredColumnCount: exploreUtils.getFilteredColumnCount(
      columns,
      columnFilter
    ),
  };
}

export default connect(mapStateToProps, {
  performTransform,
  collapseExploreSql,
  setQueryFilter: setQueryFilterFunc,
})(TableControls);
