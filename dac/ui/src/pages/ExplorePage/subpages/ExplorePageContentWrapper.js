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

import './ExplorePageContentWrapper.less';

import React, { PureComponent } from 'react';
import Immutable, { Map } from 'immutable';
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import Mousetrap from 'mousetrap';

import { withRouter } from 'react-router';
import { injectIntl } from 'react-intl';

import DataGraph from '@inject/pages/ExplorePage/subpages/datagraph/DataGraph';
import DetailsWizard from 'components/Wizards/DetailsWizard';
import { MARGIN_PANEL } from 'uiTheme/radium/sizes';

import { RECOMMENDED_JOIN } from '@app/constants/explorePage/joinTabs';
import { UNSAVED_DATASET_PATH } from '@app/constants/explorePage/paths';
import { Wiki } from '@app/pages/ExplorePage/components/Wiki/Wiki';
import { PageTypes, pageTypesProp } from '@app/pages/ExplorePage/pageTypes';
import { getApproximate, getColumnFilter, getDatasetEntityId, getExploreState, getJobProgress, getTableColumns } from '@app/selectors/explore';
import { runDatasetSql, previewDatasetSql } from 'actions/explore/dataset/run';
import { loadSourceListData } from 'actions/resources/sources';
import { navigateToExploreDefaultIfNecessary, constructFullPath } from 'utils/pathUtils';
import { getExploreViewState } from '@app/selectors/resources';
import Reflections from '@app/pages/ExplorePage/subpages/reflections/Reflections';
import Art from '@app/components/Art';

import exploreUtils from 'utils/explore/exploreUtils';
import { setCurrentSql, updateColumnFilter } from '@app/actions/explore/view';

import { HomePageTop } from '@inject/pages/HomePage/HomePageTop';
import { Tooltip } from 'components/Tooltip';
import { loadJobDetails } from '@app/actions/jobs/jobs';
import { addNotification } from '@app/actions/notification';
import HistoryLineController from '../components/Timeline/HistoryLineController';
import DatasetsPanel from '../components/SqlEditor/Sidebar/DatasetsPanel';
import ExploreTableJobStatus from '../components/ExploreTable/ExploreTableJobStatus';
import ExploreInfoHeader from '../components/ExploreInfoHeader';
import ExploreHeader from '../components/ExploreHeader';
import { withDatasetChanges } from '../DatasetChanges';
import SqlErrorSection from './../components/SqlEditor/SqlErrorSection';

import ExploreTableController from './../components/ExploreTable/ExploreTableController';
import JoinTables from './../components/ExploreTable/JoinTables';
import TableControls from './../components/TableControls';
import ExplorePageMainContent from './ExplorePageMainContent';

const EXPLORE_DRAG_TYPE = 'explorePage';
const VIEW_ID = 'JOB_DETAILS_VIEW_ID';
export class ExplorePageContentWrapper extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    datasetSummary: PropTypes.object,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    sqlSize: PropTypes.number,
    sqlState: PropTypes.bool,
    type: PropTypes.string,
    dragType: PropTypes.string,
    approximate: PropTypes.bool,
    jobProgress: PropTypes.object,
    jobId: PropTypes.string,
    outputRecords: PropTypes.number,
    runStatus: PropTypes.bool,

    pageType: pageTypesProp.isRequired,
    rightTreeVisible: PropTypes.bool.isRequired,

    toggleRightTree: PropTypes.func.isRequired,
    startDrag: PropTypes.func.isRequired,
    errorData: PropTypes.object.isRequired,
    isError: PropTypes.bool.isRequired,
    location: PropTypes.object.isRequired,
    setRecommendationInfo: PropTypes.func,
    intl: PropTypes.object.isRequired,

    columnFilter: PropTypes.string,
    filteredColumnCount: PropTypes.number,
    columnCount: PropTypes.number,
    haveRows: PropTypes.string,
    updateColumnFilter: PropTypes.func,

    // connected
    entityId: PropTypes.string,
    runDatasetSql: PropTypes.func,
    previewDatasetSql: PropTypes.func,
    canSelect: PropTypes.any,
    version: PropTypes.string,
    loadJobDetails: PropTypes.func,
    addNotification: PropTypes.func,
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    datasetSql: PropTypes.string,
    setCurrentSql: PropTypes.func,
    isSqlQuery: PropTypes.bool,
    loadSourceListData: PropTypes.func
  };

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.renderUpperContent = this.getUpperContent.bind(this);
    this.getBottomContent = this.getBottomContent.bind(this);
    this.getControlsBlock = this.getControlsBlock.bind(this);
    this.insertFullPathAtCursor = this.insertFullPathAtCursor.bind(this);
    this.onMouseEnter = this.onMouseEnter.bind(this);
    this.onMouseLeave = this.onMouseLeave.bind(this);

    this.state = {
      funcHelpPanel: false,
      datasetsPanel: !!(props.dataset && props.dataset.get('isNewQuery')),
      sidebarCollapsed: !props.isSqlQuery,
      isSqlQuery: props.isSqlQuery,
      tooltipHover: false,
      jobId: '',
      keyboardShortcuts: { // Default to Windows commands
        'run': 'CTRL + Shift + Enter',
        'preview': 'CTRL + Enter',
        'comment': 'CTRL + /',
        'find': 'CTRL + F'
      },
      currentSqlIsEmpty: true,
      currentSqlEdited: false,
      datasetSqlIsEmpty: true
    };

    this.editorRef = React.createRef();
  }

  componentDidMount() {
    Mousetrap.bind(['mod+enter', 'mod+shift+enter'], this.kbdShorthand);
    this.getUserOperatingSystem();
    this.onJobIdChange();
    this.props.loadSourceListData();
  }

  componentWillUnmount() {
    Mousetrap.unbind(['mod+enter', 'mod+shift+enter']);
  }

  componentDidUpdate(prevProps) {
    const {
      addNotification: addSuccessNotification,
      intl: { formatMessage },
      jobProgress,
      loadJobDetails: getJobDetails,
      setCurrentSql: newCurrentSql,
      datasetSql,
      currentSql
    } = this.props;

    if (prevProps.jobProgress && prevProps.jobProgress.jobId !== this.state.jobId) {
      if (jobProgress && jobProgress.jobId) {

        // Retrieve the Job's details to see if the results were truncated from the backend
        getJobDetails(jobProgress.jobId, VIEW_ID)
          .then((response) => {
            // added null check
            const responseStats = response && response.payload && !response.error ? response.payload.getIn(['entities', 'jobDetails', response.meta.jobId, 'stats']) : '';
            // isOutputLimited will be true if the results were truncated
            if (responseStats && responseStats.get('isOutputLimited')) {
              addSuccessNotification(formatMessage({ id: 'Explore.Run.Warning' }, { rows: responseStats.get('outputRecords').toLocaleString() }), 'success');
            }
          });

        // Reset the currentSql to the statement that was executed
        if (datasetSql !== currentSql) {
          newCurrentSql({sql: datasetSql});
        }
      }
      this.onJobIdChange();
    }

    // Update when switching between Dataset and SQL Runner
    if (prevProps.isSqlQuery !== this.props.isSqlQuery) {
      this.setPanelCollapse();
    }

    this.handleDisableButton(prevProps);
  }

  onJobIdChange() {
    if (this.props.jobProgress) {
      this.setState({ jobId: this.props.jobProgress.jobId });
    }
  }

  onMouseEnter() {
    this.setState({tooltipHover: true});
  }

  onMouseLeave() {
    this.setState({tooltipHover: false});
  }

  kbdShorthand = (e) => {
    if (!e) return;

    const { pageType, location } = this.props;
    navigateToExploreDefaultIfNecessary(pageType, location, this.context.router);

    if (e.shiftKey) {
      this.props.runDatasetSql();
    } else {
      this.props.previewDatasetSql();
    }
  };

  getMonacoEditorInstance() {
    // TODO: Refactor this `ref` to use Context
    return this.editorRef.current.explorePageMainContentRef.topSplitterContentRef.sqlEditorControllerRef.sqlAutoCompleteRef.monacoEditorComponent.editor;
  }

  getUserOperatingSystem() {
    // Change to Mac commands
    if (navigator.userAgent.indexOf('Mac OS X') !== -1) {
      this.setState({ keyboardShortcuts: {
        'run': 'CMD + Shift + Enter',
        'preview': 'CMD + Enter',
        'comment': 'CMD + /',
        'find': 'CMD + F'
      }});
    }
  }

  insertFullPathAtCursor(id) {
    this.insertFullPath(id);
  }

  insertFullPath(pathList, ranges) {
    if (typeof pathList === 'string') {
      this.insertAtRanges(pathList, ranges);
    } else {
      const text = constructFullPath(pathList);
      this.insertAtRanges(text, ranges);
    }
  }

  insertAtRanges(text, ranges = this.getMonacoEditorInstance().getSelections()) { // getSelections() falls back to cursor location automatically
    const edits = ranges.map(range => ({ identifier: 'dremio-inject', range, text }));

    // Remove highlighted string and place cursor to the end
    ranges[0].endColumn = ranges[0].endColumn * 10;
    ranges[0].startColumn = ranges[0].startColumn * 10;
    ranges[0].selectionStartColumn = ranges[0].selectionStartColumn * 10;
    ranges[0].positionColumn = ranges[0].positionColumn * 10;

    this.getMonacoEditorInstance().executeEdits('dremio', edits, ranges);
    this.getMonacoEditorInstance().pushUndoStop();
    this.getMonacoEditorInstance().focus();
  }

  handleSidebarCollapse = () => {
    this.setState({ sidebarCollapsed: !this.state.sidebarCollapsed });
  }

  getBottomContent() {
    const {dataset, pageType, location, entityId, canSelect} = this.props;
    const locationQuery = location.query;

    if (locationQuery && locationQuery.type === 'JOIN' && locationQuery.joinTab !== RECOMMENDED_JOIN) {
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

    switch (pageType) {
    case PageTypes.graph:
      return (DataGraph && <DataGraph
        ref='gridTable'
        dragType={EXPLORE_DRAG_TYPE}
        dataset={dataset}
        sqlState={this.props.sqlState}
        rightTreeVisible={this.props.rightTreeVisible}
      />);
    case PageTypes.wiki: {
      // should allow edit a wiki only if we receive a entity id and permissions allow this.
      // If we do not receive permissions object, that means the current user is admin (CE)
      const isWikiEditAllowed = entityId && dataset.getIn(['permissions', 'canManageWiki'], true) && dataset.getIn(['permissions', 'canAlter'], true);

      return <Wiki
        entityId={entityId}
        isEditAllowed={isWikiEditAllowed}
        className='bottomContent' />;
    }
    case PageTypes.reflections: {
      return <Reflections
        datasetId={entityId}
      />;
    }
    case PageTypes.default:
    case PageTypes.details:
      return (<ExploreTableController
        pageType={pageType}
        dataset={dataset}
        dragType={EXPLORE_DRAG_TYPE}
        location={location}
        sqlSize={this.props.sqlSize}
        sqlState={this.props.sqlState}
        rightTreeVisible={this.props.rightTreeVisible}
        exploreViewState={this.props.exploreViewState}
        canSelect={canSelect}
      />);
    default:
      throw new Error(`Not supported page type: '${pageType}'`);
    }
  }

  getActionsBlock() {
    const {
      approximate,
      pageType,
      columnFilter,
      filteredColumnCount,
      columnCount,
      version
    } = this.props;

    switch (pageType) {
    case PageTypes.graph:
    case PageTypes.details:
    case PageTypes.reflections:
    case PageTypes.wiki:
      return;
    case PageTypes.default:
      return <div className='dremioContent__actions'>
        <div
          className='dremioContent-leftCol'
          data-qa='columnFilterStats'>
          {columnFilter && <span data-qa='columnFilterCount'>{filteredColumnCount} of </span>}
          {columnCount} {la('fields')}
        </div>

        <div className='dremioContent-rightCol'>
          <div className='dremioContent-rightCol__time'>
            <ExploreTableJobStatus
              approximate={approximate}
              version={version} />
          </div>
          <div className='dremioContent-rightCol__shortcuts'>
            <div onMouseEnter={this.onMouseEnter} onMouseLeave={this.onMouseLeave} ref='target'>
              <Art src='keyboard.svg' alt='Keyboard Shortcuts' style={{ height: 24, width: 24}} className='keyboard' />
              <Tooltip
                key='tooltip'
                type='info'
                placement='left'
                target={() => this.state.tooltipHover ? this.refs.target : null}
                container={this}
                tooltipInnerClass='textWithHelp__tooltip --white'
                tooltipArrowClass='--white'>
                <p className='tooltip-content__heading'>Keyboard Shortcuts</p>
                <ul className='tooltip-content__list'>
                  <li>Run<span>{ this.state.keyboardShortcuts.run }</span></li>
                  <li>Preview<span>{ this.state.keyboardShortcuts.preview }</span></li>
                  <li>Comment Out/In<span>{ this.state.keyboardShortcuts.comment }</span></li>
                  <li>Find<span>{ this.state.keyboardShortcuts.find }</span></li>
                </ul>
              </Tooltip>
            </div>
          </div>
        </div>
      </div>;
    default:
      throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  getControlsBlock() {
    const {
      pageType
    } = this.props;

    switch (pageType) {
    case PageTypes.graph:
    case PageTypes.details:
    case PageTypes.reflections:
    case PageTypes.wiki:
      return;
    case PageTypes.default:
      return <TableControls
        dataset={this.props.dataset}
        sqlSize={this.props.sqlSize}
        location={this.props.location}
        pageType={this.props.pageType}
        sqlState={this.props.sqlState}
        rightTreeVisible={this.props.rightTreeVisible}
        exploreViewState={this.props.exploreViewState}
        disableButtons={this.isButtonDisabled()}
      />;
    default:
      throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  getContentHeader() {
    const {
      approximate,
      pageType,
      exploreViewState,
      dataset,
      location,
      rightTreeVisible,
      toggleRightTree,
      sqlState
    } = this.props;

    switch (pageType) {
    case PageTypes.graph:
    case PageTypes.details:
    case PageTypes.reflections:
    case PageTypes.wiki:
      return;
    case PageTypes.default:
      return <ExploreHeader
        dataset={dataset}
        pageType={pageType}
        toggleRightTree={toggleRightTree}
        rightTreeVisible={rightTreeVisible}
        exploreViewState={exploreViewState}
        approximate={approximate}
        location={location}
        sqlState={sqlState}
        keyboardShortcuts={this.state.keyboardShortcuts}
        disableButtons={this.isButtonDisabled()}
      />;
    default:
      throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  getSidebar() {
    const {
      pageType,
      exploreViewState,
      dataset,
      sqlSize,
      location
    } = this.props;

    switch (pageType) {
    case PageTypes.graph:
    case PageTypes.details:
    case PageTypes.reflections:
    case PageTypes.wiki:
      return;
    case PageTypes.default:
      return <div className='dremioSidebar'>
        <div className='dremioSidebarWrap'>
          <div className='dremioSidebarWrap__inner'>
            <DatasetsPanel
              dataset={dataset}
              height={sqlSize - MARGIN_PANEL}
              isVisible={this.state.datasetsPanel}
              dragType={EXPLORE_DRAG_TYPE}
              viewState={exploreViewState}
              insertFullPathAtCursor={this.insertFullPathAtCursor}
              location={location}
              sidebarCollapsed={this.state.sidebarCollapsed}
              handleSidebarCollapse={this.handleSidebarCollapse}
            />
          </div>
        </div>
      </div>;
    default:
      throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  getUpperContent() {
    const {
      pageType,
      canSelect,
      exploreViewState,
      dataset,
      location,
      startDrag,
      rightTreeVisible,
      sqlSize,
      sqlState,
      toggleRightTree
    } = this.props;

    switch (pageType) {
    case PageTypes.details:
      return <DetailsWizard
        dataset={dataset}
        location={location}
        startDrag={startDrag}
        dragType={EXPLORE_DRAG_TYPE}
        exploreViewState={exploreViewState}
        canSelect={canSelect}
      />;
    case PageTypes.default:
    case PageTypes.reflections:
    case PageTypes.graph:
    case PageTypes.wiki:
      return (

        <ExplorePageMainContent
          dataset={dataset}
          pageType={pageType}
          rightTreeVisible={rightTreeVisible}
          sqlSize={sqlSize}
          sqlState={sqlState}
          dragType={EXPLORE_DRAG_TYPE}
          toggleRightTree={toggleRightTree}
          startDrag={startDrag}
          exploreViewState={exploreViewState}
          sidebarCollapsed={this.state.sidebarCollapsed}
          handleSidebarCollapse={this.handleSidebarCollapse}
          ref={this.editorRef}
        />
      );
    default:
      throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  handleDisableButton(prevProps) {
    const {
      currentSql,
      datasetSql,
      location
    } = this.props;

    // Only update state if SQL statement has been edited
    if (prevProps.currentSql !== currentSql) {
      this.setState({ currentSqlEdited: true });

      if (currentSql === null || currentSql.length === 0) {
        this.setState({ currentSqlIsEmpty: true });
      } else {
        this.setState({ currentSqlIsEmpty: false });
      }
    }

    // Update state because datasetSql prop is not sent with the first component update
    if (prevProps.datasetSql !== datasetSql) {
      if (datasetSql.length > 0) {
        this.setState({ datasetSqlIsEmpty: false });
      } else {
        this.setState({ datasetSqlIsEmpty: true });
      }
    } else if (datasetSql.length > 0) {
      this.setState({ datasetSqlIsEmpty: false });
    }

    // Enable save button if datasetSql is not empty and making a new dataset
    if (!!datasetSql && location.pathname === UNSAVED_DATASET_PATH) {
      this.setState({ currentSqlIsEmpty: false });
    }
  }

  // Disabled Run and Preview buttons if SQL statement is empty
  isButtonDisabled = () => {

    // SQL statement has been edited
    if (this.state.currentSqlEdited) {
      if (this.state.currentSqlIsEmpty) {
        return true;
      }
    // On intial load of a dataset, show buttons
    } else if (!this.state.currentSqlEdited && this.state.currentSqlIsEmpty && !this.state.datasetSqlIsEmpty) {
      return false;
    }

    // Show buttons if SQL statement has any value
    if (!this.state.currentSqlIsEmpty) {
      return false;
    }

    return true;
  }

  setPanelCollapse = () => {
    this.setState({ sidebarCollapsed: !this.props.isSqlQuery });
  }

  updateColumnFilter = (columnFilter) => {
    this.props.updateColumnFilter(columnFilter);
  };

  render() {
    return (
      <>
        <HomePageTop />
        <div className={classNames('explorePage', this.state.sidebarCollapsed && '--collpase')}>

          <div className='dremioContent'>
            <div className='dremioContent__header'>
              <ExploreInfoHeader
                dataset={this.props.dataset}
                pageType={this.props.pageType}
                toggleRightTree={this.props.toggleRightTree}
                rightTreeVisible={this.props.rightTreeVisible}
                exploreViewState={this.props.exploreViewState}
              />
            </div>

            <div className='dremioContent__main'>
              {this.getSidebar()}

              <HistoryLineController
                dataset={this.props.dataset}
                location={this.props.location}
                pageType={this.props.pageType}
              />

              <div className='dremioContent__content'>
                {this.getContentHeader()}

                {this.getUpperContent()}

                {this.getActionsBlock()}

                <div className='dremioContent__table'>
                  {this.getControlsBlock()}

                  <SqlErrorSection
                    visible={this.props.isError}
                    errorData={this.props.errorData}
                  />

                  {this.getBottomContent()}
                </div>
              </div>

            </div>

          </div>
        </div>
      </>
    );
  }
}

function mapStateToProps(state, { location, dataset }) {
  const version = location.query && location.query.version;
  const entityId = getDatasetEntityId(state, location);
  const exploreViewState = getExploreViewState(state);
  const explorePageState = getExploreState(state);
  const datasetVersion = dataset.get('datasetVersion');
  const jobProgress = getJobProgress(state, version);
  const isSqlQuery = location.pathname === '/new_query' || location.pathname === '/tmp/tmp/UNTITLED';

  const columns = getTableColumns(state, datasetVersion, location);
  const columnFilter = getColumnFilter(state);

  // RBAC needs the permissions sent to the Acceleration components and passed down, in case any component along the way needs to be able to alter reflections
  const permissions = state.resources.entities.get('datasetUI')
  && state.resources.entities.get('datasetUI').first()
  && (Map.isMap(state.resources.entities.get('datasetUI').first()) && state.resources.entities.get('datasetUI').first().get('permissions'));

  const canSelect = permissions && permissions.get('canSelect');

  return {
    entityId,
    exploreViewState,
    approximate: getApproximate(state, datasetVersion),
    columnFilter,
    columnCount: columns.size,
    currentSql: explorePageState.view.currentSql,
    filteredColumnCount: exploreUtils.getFilteredColumnCount(columns, columnFilter),
    canSelect,
    version,
    jobProgress,
    queryContext: explorePageState.view.queryContext,
    isSqlQuery
  };
}

export default withRouter(connect(mapStateToProps, {
  runDatasetSql,
  previewDatasetSql,
  updateColumnFilter,
  loadJobDetails,
  addNotification,
  setCurrentSql,
  loadSourceListData
}, null, { forwardRef: true })(withDatasetChanges(injectIntl(ExplorePageContentWrapper))));
