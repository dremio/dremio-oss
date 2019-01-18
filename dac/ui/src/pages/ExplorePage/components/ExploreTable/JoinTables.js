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
import Radium from 'radium';
import { connect } from 'react-redux';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import EllipsedText from 'components/EllipsedText';
import { formLabel, formDescription } from 'uiTheme/radium/typography';
import { getJoinTable, getExploreState } from 'selectors/explore';
import { getViewState } from 'selectors/resources';
import Spinner from 'components/Spinner';
import { constructFullPath } from 'utils/pathUtils';
import { JOIN_TABLE_VIEW_ID } from 'components/Wizards/JoinWizard/JoinController';
import { accessEntity } from 'actions/resources/lru';

import { CUSTOM_JOIN } from 'constants/explorePage/joinTabs';

import { ExploreInfoHeader } from '../ExploreInfoHeader';
import ExploreTableController from './ExploreTableController';

const WIDTH_SCALE = 0.495;

@Radium
@pureRender
export class JoinTables extends Component {

  static propTypes = {
    pageType: PropTypes.string.isRequired,
    dataset: PropTypes.instanceOf(Immutable.Map),
    joinTableData: PropTypes.object,
    exploreViewState: PropTypes.instanceOf(Immutable.Map),
    joinViewState: PropTypes.instanceOf(Immutable.Map),
    location: PropTypes.object,
    sqlSize: PropTypes.number,
    rightTreeVisible: PropTypes.bool,
    accessEntity: PropTypes.func.isRequired,
    joinDatasetFullPath: PropTypes.string,
    joinVersion: PropTypes.string,
    joinTab: PropTypes.string,
    joinStep: PropTypes.number
  };

  componentWillReceiveProps(nextProps) {
    // because we have two tables visible, it's possible to evict the left table by looking at enough different
    // right tables. So accesss the left tableData every time the right tableData changes
    if (nextProps.joinVersion !== this.props.joinVersion) {
      nextProps.accessEntity('tableData', this.props.dataset.get('datasetVersion'));
    }
  }

  shouldRenderSecondTable() {
    const isOnCustomJoinSelectStep = this.props.joinTab === CUSTOM_JOIN && this.props.joinStep !== 2;
    return isOnCustomJoinSelectStep && !this.props.exploreViewState.get('isInProgress');
  }

  renderSecondTable() {
    if (!this.shouldRenderSecondTable()) {
      return null;
    }
    const { joinTableData, joinViewState, rightTreeVisible, sqlSize, location } = this.props;
    if ((!joinTableData || !joinTableData.get('columns').size) && !joinViewState.get('isFailed')) {
      const customTableContent = joinViewState && joinViewState.get('isInProgress')
        ? (
          <Spinner
            style={styles.emptyTable}
            iconStyle={{color: 'gray'}}
          />
        )
        : (
          <div style={[styles.emptyTable, formDescription]}>
            {la('Please add one or more join conditions to preview matching records.')}
          </div>
        );
      return customTableContent;
    }
    return (
      <ExploreTableController
        isDumbTable
        pageType={this.props.pageType}
        dataset={this.props.dataset}
        exploreViewState={joinViewState}
        tableData={joinTableData}
        widthScale={WIDTH_SCALE}
        dragType='join'
        location={location}
        sqlSize={sqlSize}
        rightTreeVisible={rightTreeVisible}
      />
    );
  }

  render() {
    const { location, sqlSize, rightTreeVisible, dataset, exploreViewState } = this.props;
    const nameForDisplay = ExploreInfoHeader.getNameForDisplay(dataset);
    const scale = this.shouldRenderSecondTable()
      ? WIDTH_SCALE
      : 1;
    const tableBlockStyle = !this.shouldRenderSecondTable() ? { maxWidth: '100%', width: 'calc(100% - 10px)'} : {};
    return (
      <div style={[styles.base]}>
        <div style={[styles.tableBlock, tableBlockStyle]}>
          <div style={styles.header}><EllipsedText text={nameForDisplay} /></div>
          <ExploreTableController
            pageType={this.props.pageType}
            dataset={this.props.dataset}
            dragType='groupBy'
            widthScale={scale}
            location={location}
            sqlSize={sqlSize}
            rightTreeVisible={rightTreeVisible}
            exploreViewState={exploreViewState}
          />
        </div>
        <div style={[styles.tableBlock, { marginLeft: 5}, !this.shouldRenderSecondTable() ? { display: 'none'} : {} ]}>
          <div style={[styles.header, !this.props.joinDatasetFullPath && {fontStyle: 'italic'}]}>
            <EllipsedText text={this.props.joinDatasetFullPath || la('No table selected.')}/>
          </div>
          {this.renderSecondTable()}
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state, ownProps) => {
  const explorePageState = getExploreState(state);
  return {
    joinTableData: getJoinTable(state, ownProps),
    joinViewState: getViewState(state, JOIN_TABLE_VIEW_ID),
    joinDatasetFullPath: constructFullPath(explorePageState.join.getIn(['custom', 'joinDatasetPathList'])),
    joinTab: explorePageState.join.get('joinTab'),
    joinVersion: explorePageState.join.getIn(['custom', 'joinVersion']),
    joinStep: explorePageState.join.get('step')
  };
};

export default connect(mapStateToProps, {
  accessEntity
})(JoinTables);

const styles = {
  base: {
    display: 'flex',
    flexGrow: 1,
    backgroundColor: '#F5FCFF'
  },
  tableBlock: {
    display: 'flex',
    maxWidth: 'calc(50% - 10px)',
    minWidth: 'calc(50% - 10px)',
    flexDirection: 'column',
    position: 'relative'
  },
  header: {
    ...formLabel,
    height: 25,
    backgroundColor: '#F3F3F3',
    zIndex: 1,
    display: 'flex',
    alignItems: 'center',
    padding: '0 10px',
    marginRight: -5, // hack for alignment
    flexGrow: 0
  },
  emptyTable: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    minWidth: '100%',
    minHeight: 300,
    backgroundColor: '#F3F3F3',
    border: '1px dotted gray',
    zIndex: 1,
    flexGrow: 1
  },
  spinner: {
    width: 'calc(50% - 26px)',
    height: 'calc(100% - 440px)',
    backgroundColor: '#F3F3F3',
    border: '1px dotted gray',
    alignItems: 'center',
    zIndex: 1,
    flexGrow: 1
  }
};
