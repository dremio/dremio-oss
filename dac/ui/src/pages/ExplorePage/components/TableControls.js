/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Component, PropTypes } from 'react';
import { connect }   from 'react-redux';
import pureRender from 'pure-render-decorator';
import Radium from 'radium';
import Immutable from 'immutable';
import exploreUtils from 'utils/explore/exploreUtils';

import { performTransform } from 'actions/explore/dataset/transform';

import { getTableColumns, getApproximate } from 'selectors/explore';
import TableControlsView from './TableControlsView';

@pureRender
@Radium
export class TableControls extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    columns: PropTypes.instanceOf(Immutable.List),
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    isGraph: PropTypes.bool.isRequired,
    sqlState: PropTypes.bool.isRequired,
    sqlSize: PropTypes.number.isRequired,
    toggleExploreSql: PropTypes.func,
    location: PropTypes.object.isRequired,
    rightTreeVisible: PropTypes.bool,
    approximate: PropTypes.bool,

    // actions
    performTransform: PropTypes.func.isRequired
  };

  static contextTypes = {
    router: PropTypes.object.isRequired,
    location: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.toogleDropdown = this.toogleDropdown.bind(this);
    this.closeDropdown = this.closeDropdown.bind(this);

    this.state = {
      dropdownState: false,
      columnsModalVisibility: false
    };
  }

  getLocationWithoutGraph(location) {
    let newLocation = location;
    if (this.props.isGraph) {
      newLocation = {
        ...newLocation,
        pathname: newLocation.pathname.substr(0, location.pathname.indexOf('/graph'))
      };
    }

    return newLocation;
  }

  closeDropdown() {
    this.setState({
      dropdownState: false
    });
  }

  dataGraph() {
    const { location, router } = this.context;
    const pathname = location.pathname.indexOf('/graph') === -1
      ? `${location.pathname}/graph`
      : location.pathname.substr(0, location.pathname.indexOf('/graph'));
    router.push({...location, pathname});
  }

  navigateToTransformWizard(wizardParams) {
    const { router } = this.context;
    const { dataset, currentSql, queryContext, exploreViewState } = this.props;

    const callback = () => {
      const locationWithoutGraph = this.getLocationWithoutGraph(this.props.location);
      router.push(exploreUtils.getLocationToGoToTransformWizard({...wizardParams, location: locationWithoutGraph}));
    };
    this.props.performTransform({dataset, currentSql, queryContext, viewId: exploreViewState.get('viewId'), callback});
  }

  addField = () => {
    // use first column by default for just the expression
    const defaultColumn = this.props.columns.getIn([0, 'name']);
    this.navigateToTransformWizard({
      detailType: 'CALCULATED_FIELD',
      column: '',
      props: {
        initialValues: {
          expression: defaultColumn ? exploreUtils.escapeFieldNameForSQL(defaultColumn) : ''
        }
      }
    });
  }

  groupBy = () => {
    this.navigateToTransformWizard({ detailType: 'GROUP_BY', column: '' });
  }

  join = () => {
    this.navigateToTransformWizard({ detailType: 'JOIN', column: '', location });
  }

  handleRequestClose = () => {
    this.setState({
      dropdownState: false
    });
  };

  preventTooltipHide() {
    clearTimeout(this.timer);
  }

  toogleDropdown(e) {
    this.setState({
      dropdownState: !this.state.dropdownState,
      anchorEl: e.currentTarget
    });
  }

  union() {}

  render() {
    const { isGraph, dataset, columns, sqlState, approximate, rightTreeVisible, exploreViewState } = this.props;
    const { anchorEl, dropdownState } = this.state;
    return (
      <TableControlsView
        isGraph={isGraph}
        dataset={dataset}
        exploreViewState={exploreViewState}
        columns={columns}
        addField={this.addField}
        sqlState={sqlState}
        groupBy={this.groupBy.bind(this)}
        dataGraph={this.dataGraph.bind(this)}
        dropdownState={dropdownState}
        closeDropdown={this.closeDropdown.bind(this)}
        toogleDropdown={this.toogleDropdown.bind(this)}
        handleRequestClose={this.handleRequestClose.bind(this)}
        anchorEl={anchorEl}
        join={this.join}
        approximate={approximate}
        rightTreeVisible={rightTreeVisible}
      />
    );
  }
}

function mapStateToProps(state, props) {
  const location = state.routing.locationBeforeTransitions || {};
  const datasetVersion = props.dataset.get('datasetVersion');
  return {
    currentSql: state.explore.view.get('currentSql'),
    queryContext: state.explore.view.get('queryContext'),
    columns: getTableColumns(state, datasetVersion, location),
    approximate: getApproximate(state, datasetVersion)
  };
}

export default connect(mapStateToProps, {
  performTransform
})(TableControls);
