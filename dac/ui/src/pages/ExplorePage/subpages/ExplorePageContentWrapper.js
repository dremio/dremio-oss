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
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import DataGraph from 'dyn-load/pages/ExplorePage/subpages/datagraph/DataGraph';
import DetailsWizard from 'components/Wizards/DetailsWizard';
import { HISTORY_PANEL_SIZE } from 'uiTheme/radium/sizes';

import { RECOMMENDED_JOIN } from 'constants/explorePage/joinTabs';

import ExploreTableController from './../components/ExploreTable/ExploreTableController';
import JoinTables from './../components/ExploreTable/JoinTables';
import TableControls from './../components/TableControls';
import ExplorePageUpperContent from './ExplorePageUpperContent';

import SqlErrorSection from './../components/SqlEditor/SqlErrorSection';


const EXPLORE_DRAG_TYPE = 'explorePage';

@pureRender
@Radium
class ExplorePageContentWrapper extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    pageType: PropTypes.string.isRequired,
    rightTreeVisible: PropTypes.bool.isRequired,
    sqlSize: PropTypes.number.isRequired,
    sqlState: PropTypes.bool.isRequired,
    toggleRightTree: PropTypes.func.isRequired,
    startDrag: PropTypes.func.isRequired,
    errorData: PropTypes.object.isRequired,
    isError: PropTypes.bool.isRequired,
    location: PropTypes.object.isRequired,
    setRecommendationInfo: PropTypes.func,
    exploreViewState: PropTypes.instanceOf(Immutable.Map)
  };

  constructor(props) {
    super(props);
    this.renderUpperContent = this.renderUpperContent.bind(this);
    this.getBottomContent = this.getBottomContent.bind(this);
    this.getControlsBlock = this.getControlsBlock.bind(this);
  }

  getBottomContent() {
    const {dataset, pageType, location} = this.props;
    const locationQuery = location.query;
    const hash = {
      table: (
        <ExploreTableController
          pageType={pageType}
          dataset={dataset}
          dragType={EXPLORE_DRAG_TYPE}
          location={location}
          sqlSize={this.props.sqlSize}
          sqlState={this.props.sqlState}
          rightTreeVisible={this.props.rightTreeVisible}
          exploreViewState={this.props.exploreViewState}
        />
      ),
      graph: (
        DataGraph && <DataGraph
          ref='gridTable'
          dragType={EXPLORE_DRAG_TYPE}
          dataset={dataset}
          sqlState={this.props.sqlState}
          rightTreeVisible={this.props.rightTreeVisible}
        />
      )
    };
    if (locationQuery.type === 'JOIN' && locationQuery.joinTab !== RECOMMENDED_JOIN) {
      return (
        <JoinTables
          pageType={pageType}
          dataset={dataset}
          location={location}
          sqlSize={this.props.sqlSize}
          rightTreeVisible={this.props.rightTreeVisible}
          exploreViewState={this.props.exploreViewState}
        />
      );
    }
    return hash[pageType] || hash.table;
  }

  getControlsBlock() {
    if (this.props.pageType === 'default' || this.props.pageType === 'graph') {
      return <TableControls
        dataset={this.props.dataset}
        sqlSize={this.props.sqlSize}
        location={this.props.location}
        isGraph={this.props.pageType === 'graph'}
        sqlState={this.props.sqlState}
        rightTreeVisible={this.props.rightTreeVisible}
        exploreViewState={this.props.exploreViewState}
      />;
    }
  }

  renderUpperContent() {
    const { props } = this;
    if (props.pageType === 'details') {
      return <DetailsWizard
        dataset={props.dataset}
        location={props.location}
        startDrag={props.startDrag}
        dragType={EXPLORE_DRAG_TYPE}
        exploreViewState={this.props.exploreViewState}
      />;
    }
    return (
      <ExplorePageUpperContent
        dataset={props.dataset}
        pageType={props.pageType}
        rightTreeVisible={props.rightTreeVisible}
        sqlSize={props.sqlSize}
        sqlState={props.sqlState}
        dragType={EXPLORE_DRAG_TYPE}
        toggleRightTree={props.toggleRightTree}
        startDrag={props.startDrag}
        exploreViewState={this.props.exploreViewState}
      />
    );
  }

  render() {
    const tableViewerStyle = this.props.rightTreeVisible
      ? styles.tableViewerStyleShort
      : styles.tableViewerStyleFull;

    return (
      <div className='table-parent' style={[styles.base, styles.tableParent, tableViewerStyle]}>
        {this.renderUpperContent()}
        <div className='table-control-wrap' style={styles.tableControlWrap}>
          {this.getControlsBlock()}
          <SqlErrorSection
            visible={this.props.isError}
            errorData={this.props.errorData}
          />
          {this.getBottomContent()}
        </div>
      </div>
    );
  }
}

export default ExplorePageContentWrapper;

const styles = {
  base: {
    overflow: 'hidden',
    display: 'flex',
    flexDirection: 'column'
  },
  tableControlWrap: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1
  },
  tableViewerStyleShort: {
    width: '100%'
  },
  tableViewerStyleFull: {
    width: `calc(100% - ${HISTORY_PANEL_SIZE}px)`,
    flexShrink: 0
  }
};
