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
import { PureComponent, Fragment } from 'react';
import { connect } from 'react-redux';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import DocumentTitle from 'react-document-title';
import { FormattedMessage, injectIntl } from 'react-intl';
import { withRouter } from 'react-router';

import CopyButton from '@app/components/Buttons/CopyButton';
import * as ButtonTypes from '@app/components/Buttons/ButtonTypes';

import DropdownMenu from '@app/components/Menus/DropdownMenu';
import EllipsedText from 'components/EllipsedText';
import modelUtils from 'utils/modelUtils';
import { constructFullPath, navigateToExploreDefaultIfNecessary } from 'utils/pathUtils';
import { formatMessage } from 'utils/locale';
import { needsTransform, isSqlChanged } from 'sagas/utils';

import { PHYSICAL_DATASET_TYPES } from '@app/constants/datasetTypes';
import { replace } from 'react-router-redux';
import explorePageInfoHeaderConfig from '@inject/pages/ExplorePage/components/explorePageInfoHeaderConfig';

//actions
import { saveDataset, saveAsDataset } from 'actions/explore/dataset/save';
import { performTransform, transformHistoryCheck } from 'actions/explore/dataset/transform';
import { performTransformAndRun, runDatasetSql, previewDatasetSql } from 'actions/explore/dataset/run';
import { showConfirmationDialog } from 'actions/confirmation';
import { PageTypes, pageTypesProp } from '@app/pages/ExplorePage/pageTypes';
import { withDatasetChanges } from '@app/pages/ExplorePage/DatasetChanges';
import { showUnsavedChangesConfirmDialog } from '@app/actions/confirmation';

import { startDownloadDataset } from 'actions/explore/download';
import { performNextAction, NEXT_ACTIONS } from 'actions/explore/nextAction';
import { editOriginalSql } from 'actions/explore/dataset/reapply';

import ExploreHeaderMixin from '@app/pages/ExplorePage/components/ExploreHeaderMixin';
import config from 'dyn-load/utils/config';
import { getAnalyzeToolsConfig } from '@app/utils/config';
import exploreUtils from '@app/utils/explore/exploreUtils';

import SaveMenu, { DOWNLOAD_TYPES } from 'components/Menus/ExplorePage/SaveMenu';
import BreadCrumbs, {formatFullPath} from 'components/BreadCrumbs';
import FontIcon from 'components/Icon/FontIcon';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import Art from '@app/components/Art';
import { Button } from 'dremio-ui-lib';
import { showQuerySpinner } from '@inject/pages/ExplorePage/utils';
import { getIconDataTypeFromDatasetType } from 'utils/iconUtils';

import { getHistory, getTableColumns, getJobProgress, getRunStatus, getExploreJobId, getExploreState, isWikAvailable } from 'selectors/explore';
import { getActiveScript } from 'selectors/scripts';

import './ExploreHeader.less';
import { cancelJobAndShowNotification } from '@app/actions/jobs/jobs';
import SQLScriptDialog from '@app/components/SQLScripts/components/SQLScriptDialog/SQLScriptDialog';
import { setCurrentSql } from 'actions/explore/view';
import { createScript, fetchScripts, updateScript, setActiveScript } from 'actions/resources/scripts';
import openPopupNotification from '@app/components/PopupNotification/PopupNotification';
import { ExploreActions } from './ExploreActions';
import ExploreTableJobStatusSpinner from './ExploreTable/ExploreTableJobStatusSpinner';
import { JOB_STATUS } from './ExploreTable/ExploreTableJobStatus';

export const TABLEAU_TOOL_NAME = 'Tableau';
export const QLIK_TOOL_NAME = 'Qlik Sense';

@injectIntl
@Radium
@ExploreHeaderMixin
export class ExploreHeader extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    datasetSummary: PropTypes.object,
    datasetSql: PropTypes.string,
    pageType: pageTypesProp,
    toggleRightTree: PropTypes.func.isRequired,
    grid: PropTypes.object,
    space: PropTypes.object,
    rightTreeVisible: PropTypes.bool,
    location: PropTypes.object,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,
    approximate: PropTypes.bool,
    sqlState: PropTypes.bool,
    keyboardShortcuts: PropTypes.object,
    disableButtons: PropTypes.bool,
    router: PropTypes.object,

    // connected
    history: PropTypes.instanceOf(Immutable.Map),
    queryContext: PropTypes.instanceOf(Immutable.List),
    currentSql: PropTypes.string,
    tableColumns: PropTypes.instanceOf(Immutable.List),
    settings: PropTypes.instanceOf(Immutable.Map),
    jobProgress: PropTypes.object,
    runStatus: PropTypes.bool,
    jobId: PropTypes.string,
    showWiki: PropTypes.bool,
    activeScript: PropTypes.object,

    // provided by withDatasetChanges
    getDatasetChangeDetails: PropTypes.func,

    // actions
    transformHistoryCheck: PropTypes.func.isRequired,
    performNextAction: PropTypes.func.isRequired,
    performTransform: PropTypes.func.isRequired,
    performTransformAndRun: PropTypes.func.isRequired,
    runDatasetSql: PropTypes.func.isRequired,
    previewDatasetSql: PropTypes.func.isRequired,
    saveDataset: PropTypes.func.isRequired,
    saveAsDataset: PropTypes.func.isRequired,
    startDownloadDataset: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func,
    cancelJob: PropTypes.func,
    editOriginalSql: PropTypes.func,
    replaceUrlAction: PropTypes.func,
    showUnsavedChangesConfirmDialog: PropTypes.func,
    setCurrentSql: PropTypes.func,
    createScript: PropTypes.func,
    fetchScripts: PropTypes.func,
    updateScript: PropTypes.func,
    setActiveScript: PropTypes.func
  };

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  static getFullPathListForDisplay(dataset) {
    if (!dataset) {
      return;
    }
    const fullPath = dataset.get('displayFullPath');
    return modelUtils.isNamedDataset(dataset) ? fullPath : undefined;
  }

  static getNameForDisplay(dataset) {
    const defaultName = formatMessage('NewQuery.NewQuery');
    if (!dataset) {
      return defaultName;
    }
    const displayFullPath = dataset.get('displayFullPath');
    return modelUtils.isNamedDataset(dataset) && displayFullPath ? displayFullPath.get(-1) : defaultName;
  }

  constructor(props) {
    super(props);

    this.doButtonAction = this.doButtonAction.bind(this);
    this.downloadDataset = this.downloadDataset.bind(this);

    this.state = {
      actionState: null,
      isSaveAsModalOpen: false
    };
  }

  doButtonAction(actionType) {
    switch (actionType) {
    case 'run':
      this.setState({ actionState: 'run' });
      return this.handleRunClick();
    case 'preview':
      this.setState({ actionState: 'preview' });
      return this.handlePreviewClick();
    case 'discard':
      return this.handleDiscardConfirm();
    case 'saveView':
      return this.handleSaveView();
    case 'saveViewAs':
      return this.handleSaveViewAs();
    case 'saveScript':
      return this.handleSaveScript();
    case 'saveScriptAs':
      return this.handleSaveScriptAs();
    case 'cancel':
      return this.props.cancelJob(this.props.jobId);
    case DOWNLOAD_TYPES.json:
    case DOWNLOAD_TYPES.csv:
    case DOWNLOAD_TYPES.parquet:
      return this.downloadDataset(actionType);
    default:
      break;
    }
  }

  handleRunClick() {
    this.navigateToExploreTableIfNecessary();
    this.props.runDatasetSql();
  }

  handlePreviewClick() {
    this.navigateToExploreTableIfNecessary();
    this.props.previewDatasetSql();
  }

  //TODO: DX-14762 - refactor to use runDatasetSql and performTransform saga;
  // investigate replacing pathutils.navigateToExploreTableIfNecessary with pageTypeUtils methods

  isTransformNeeded() {
    const { dataset, queryContext, currentSql } = this.props;
    return needsTransform(dataset, queryContext, currentSql);
  }

  transformIfNecessary(callback, forceDataLoad) {
    const { dataset, currentSql, queryContext, exploreViewState } = this.props;

    const doPerformTransform = () => {
      return this.props.performTransform({
        dataset,
        currentSql,
        queryContext,
        viewId: exploreViewState.get('viewId'),
        callback,
        // forces preview to reload a data if nothing is changed. Primary use case is
        // when a user clicks a preview button
        forceDataLoad
      });
    };

    if (this.isTransformNeeded()) {
      // need to navigate before history check
      this.navigateToExploreTableIfNecessary();
      this.props.transformHistoryCheck(dataset, doPerformTransform);
    } else {
      doPerformTransform();
    }
  }

  navigateToExploreTableIfNecessary() {
    const { pageType, location } = this.props;
    navigateToExploreDefaultIfNecessary(pageType, location, this.context.router);
  }

  showErrorMsgAsModal = (errorTitle, errorMsg, retryCallback) => {
    this.setState({showErrorMsgAsModal: true, errorTitle, errorMsg, retryCallback});
  };

  hideErrorMsgAsModal = () => {
    this.setState({showErrorMsgAsModal: false});
  };

  downloadDataset(format) {
    this.transformIfNecessary(
      (didTransform, dataset) => {
        this.props.showConfirmationDialog({
          title: this.props.intl.formatMessage({ id: 'Download.DownloadLimit' }),
          confirmText: this.props.intl.formatMessage({ id: 'Download.Download' }),
          text: this.props.intl.formatMessage({ id: 'Download.DownloadLimitValue' }),
          doNotAskAgainKey: 'isDownloadWarningDisabled',
          doNotAskAgainText: this.props.intl.formatMessage({ id: 'Download.DownloadLimitWarn' }),
          confirm: () => this.props.startDownloadDataset(dataset, format)
        });
      }
    );
  }

  isNewDataset() {
    const { mode } = this.props.location.query;
    return modelUtils.isNewDataset(this.props.dataset, mode);
  }

  // Note: similar to but different from ExplorePageControllerComponent#shouldShowUnsavedChangesPopup
  isEditedDataset() {
    const { dataset, history, currentSql } = this.props;
    if (!dataset.get('datasetType')) {
      // not loaded yet
      return false;
    }

    if (PHYSICAL_DATASET_TYPES.has(dataset.get('datasetType'))) {
      return false;
    }

    // New Query can not show (edited)
    if (!modelUtils.isNamedDataset(dataset)) {
      return false;
    }

    if (isSqlChanged(dataset.get('sql'), currentSql)) {
      return true;
    }

    return history ? history.get('isEdited') : false;
  }

  handleSaveView = () => {
    const nextAction = this.state.nextAction;
    this.setState({ nextAction: undefined });
    this.transformIfNecessary(
      (didTransform, dataset) => {
        // transformIfNecessary does a transformHistoryCheck if a transform is necessary.
        // if not, here we need to another transformHistoryCheck because save will lose the future history.
        // No need to worry about doing it twice because if transformIfNecessary does a transform, the next
        // transformHistoryCheck will never trigger.
        return this.props.transformHistoryCheck(dataset, () => {
          return this.props.saveDataset(dataset, this.props.exploreViewState.get('viewId'), nextAction);
        });
      }
    );
  };

  handleSaveViewAs = () => {
    const nextAction = this.state.nextAction;
    this.setState({ nextAction: undefined });
    this.transformIfNecessary(
      () => this.props.saveAsDataset(nextAction)
    );
  };

  handleSaveScript = () => {
    const { activeScript, currentSql, queryContext, intl } = this.props;
    if (!activeScript.id) {
      this.handleSaveScriptAs();
    } else {
      const payload = { name: activeScript.name, content: currentSql, context: queryContext.toJS(), description: '' };
      this.props.updateScript(payload, activeScript.id).then((res) => {
        if (!res.error) {
          this.props.setActiveScript({ script: res.payload });
          openPopupNotification({ type: 'success', message: intl.formatMessage({ id: 'NewQuery.ScriptSaved' }) });
          this.props.fetchScripts();
        }
      });
    }
  };

  handleSaveScriptAs = () => {
    this.setState({ isSaveAsModalOpen: true });
  };

  handleDiscard = () => {
    if (!this.props.activeScript.id) {
      this.props.setCurrentSql({ sql: '' });
    } else {
      this.props.setActiveScript({ script: {} });
    }
    this.props.router.push({ pathname: '/new_query', state: { discard: true }});
  }

  handleDiscardConfirm = () => {
    const { intl } = this.props;
    this.props.showConfirmationDialog({
      title: intl.formatMessage({ id: 'Script.DiscardConfirm' }),
      confirmText: intl.formatMessage({ id: 'Common.Discard' }),
      text: intl.formatMessage({ id: 'Script.DiscardConfirmMessage' }),
      confirm: () => this.handleDiscard(),
      closeButtonType: 'XBig',
      className: 'discardConfirmDialog --newModalStyles',
      headerIcon: <Art title='Warning' src='CircleWarning.svg' alt='Warning' style={{ height: 24, width: 24, marginRight: 8 }} />
    });
  }

  handleShowBI = (nextAction) => {
    const {dataset} = this.props;
    if (!modelUtils.isNamedDataset(dataset)) {
      this.transformIfNecessary(() => this.props.saveAsDataset(nextAction));
    } else {
      this.props.performNextAction(this.props.dataset, nextAction);
    }
  };

  isCreatedAndNamedDataset() {
    const { dataset } = this.props;
    return dataset.get('datasetVersion') !== undefined && modelUtils.isNamedDataset(dataset);
  }

  // unlike acceleration button, settings button is always shown, but it is disabled when
  // show acceleration button is hidden or disabled.
  shouldEnableSettingsButton() {
    return this.isCreatedAndNamedDataset() && !this.isEditedDataset();
  }

  renderCopyToClipBoard(fullPath) {
    return fullPath
      ? <CopyButton text={fullPath} title={this.props.intl.formatMessage({ id: 'Path.Copy' })} style={{transform: 'translateY(1px)'}}/>
      : null;
  }

  renderDatasetLabel(dataset) {
    const nameForDisplay = ExploreHeader.getNameForDisplay(dataset);
    const isEditedDataset = this.isEditedDataset();
    const nameStyle = isEditedDataset ? { fontStyle: 'italic' } : {};
    const fullPath = ExploreHeader.getFullPathListForDisplay(dataset);
    const edited = this.props.intl.formatMessage({ id: 'Dataset.Edited' });
    return (
      <DatasetItemLabel
        customNode={ // todo: string replace loc
          <div className='flexbox-truncate-text-fix'>
            <div style={{...style.dbName}} data-qa={nameForDisplay}>
              <EllipsedText style={nameStyle} text={`${nameForDisplay}${isEditedDataset ? edited : ''}`} className='heading'>
                <span>{nameForDisplay}</span>
                <span data-qa='dataset-edited'>{isEditedDataset ? edited : ''}</span>
              </EllipsedText>
              {this.renderCopyToClipBoard(constructFullPath(fullPath))}
            </div>
            {fullPath && <BreadCrumbs
              hideLastItem
              fullPath={fullPath}
              pathname={this.props.location.pathname}/>
            }
            {<DocumentTitle title={
              fullPath
                ? formatFullPath(fullPath).join('.') + (isEditedDataset ? '*' : '')
                : nameForDisplay
            } /> }
          </div>
        }
        isNewQuery={dataset.get('isNewQuery')}
        showFullPath
        fullPath={fullPath}
        iconSize='LARGE'
        placement='right'
        typeIcon={getIconDataTypeFromDatasetType(dataset.get('datasetType'))}
        shouldShowOverlay={false}
      />
    );
  }

  renderHeader() {
    const isJobCancellable = this.props.jobProgress ? this.getCancellable(this.props.jobProgress.status) : null;
    const { disablePreviewButton, disableRunButton } = explorePageInfoHeaderConfig;

    return (
      <>
        <div className='ExploreHeader__left'>
          { isJobCancellable && this.props.jobId && this.state.actionState === 'run' ?
            <Button
              color='secondary'
              type={ButtonTypes.SECONDARY}
              data-qa='qa-cancel'
              title={this.props.intl.formatMessage({ id: 'Common.Cancel' })}
              onClick={() => this.doButtonAction('cancel')}
              style={{ width: 75 }}
              disableMargin
            >
              {`${this.props.intl.formatMessage({ id: 'Common.Cancel' })}`}
            </Button>
            :
            <Button
              color='primary'
              type={ButtonTypes.PRIMARY}
              data-qa='qa-run'
              title={`${this.props.intl.formatMessage({ id: 'Common.Run' })} ${this.props.keyboardShortcuts.run }`}
              onClick={() => this.doButtonAction('run')}
              style={{ width: 75 }}
              disabled={disableRunButton || this.props.disableButtons}
              disableMargin
            >
              <Art src='Play.svg' alt={`${this.props.intl.formatMessage({ id: 'Common.Run' })}`} title={`${this.props.intl.formatMessage({ id: 'Common.Run' })} Ctrl+Enter`} style={{ height: 24, width: 24 }}/>
              {`${this.props.intl.formatMessage({ id: 'Common.Run' })}`}
            </Button>
          }
          { isJobCancellable && this.props.jobId && this.state.actionState === 'preview' ?
            <Button
              color='secondary'
              type={ButtonTypes.SECONDARY}
              data-qa='qa-cancel'
              title={this.props.intl.formatMessage({ id: 'Common.Cancel' })}
              onClick={() => this.doButtonAction('cancel')}
              style={{ width: 98 }}
              disableMargin
            >
              {`${this.props.intl.formatMessage({ id: 'Common.Cancel' })}`}
            </Button>
            :
            <Button
              color='primary'
              variant={ButtonTypes.OUTLINED}
              data-qa='qa-preview'
              title={`${this.props.intl.formatMessage({ id: 'Common.Preview' })} ${ this.props.keyboardShortcuts.preview}`}
              onClick={() => this.doButtonAction('preview')}
              style={{ width: 98 }}
              disabled={disablePreviewButton || this.props.disableButtons}
              disableMargin
            >
              <Art src='Eye.svg' alt={`${this.props.keyboardShortcuts.preview}`} title={`${this.props.intl.formatMessage({ id: 'Common.Preview' })} ${this.props.keyboardShortcuts.preview}`} style={{ height: 24, width: 24 }}/>
              {`${this.props.intl.formatMessage({ id: 'Common.Preview' })}`}
            </Button>
          }

          {!modelUtils.isNamedDataset(this.props.dataset) &&
            <Button
              color='primary'
              variant={ButtonTypes.OUTLINED}
              data-qa='qa-discard'
              title={this.props.intl.formatMessage({ id: 'Common.Discard' })}
              onClick={() => this.doButtonAction('discard')}
              style={{ width: 98 }}
              disableMargin
              disabled={this.props.disableButtons}
            >
              <Art src='Discard.svg' alt={this.props.intl.formatMessage({ id: 'Common.Discard' })} title={this.props.intl.formatMessage({ id: 'Common.Discard' })} style={{ height: 20, width: 22 }}/>
              {this.props.intl.formatMessage({ id: 'Common.Discard' })}
            </Button>
          }

          {this.renderEditOriginalButton()}

          <ExploreActions
            dataset={this.props.dataset}
            pageType={this.props.pageType}
            exploreViewState={this.props.exploreViewState}
          />
          {
            showQuerySpinner() &&
            <ExploreTableJobStatusSpinner jobProgress={this.props.jobProgress} jobId={this.props.jobId} action={this.state.actionState} message='Running' />
          }

        </div>
        <div className='ExploreHeader__right'>
          { this.renderAnalyzeButtons() }
          { this.renderSaveButton() }
        </div>
      </>
    );
  }

  openTableau = () => {
    this.handleShowBI(NEXT_ACTIONS.openTableau);
  };
  openPowerBi = () => {
    this.handleShowBI(NEXT_ACTIONS.openPowerBI);
  };

  renderAnalyzeButton = (name, icon, onclick, iconSize, className) => {
    const { dataset } = this.props;
    return (
      <Button
        variant='outlined'
        color='primary'
        size='medium'
        onClick={onclick}
        className={className}
        disabled={this.getExtraSaveDisable(dataset)}
        disableRipple
        disableMargin>
        <Art src={icon} alt={name} title={name} style={{ height: iconSize, width: iconSize }}/>
      </Button>
    );
  };
  renderAnalyzeButtons = () => {
    const {
      settings,
      showWiki
    } = this.props;

    if (!showWiki) return;

    const analyzeToolsConfig = getAnalyzeToolsConfig(settings, config);
    const showTableau = analyzeToolsConfig.tableau.enabled;
    const showPowerBI = analyzeToolsConfig.powerbi.enabled;

    if (!showTableau && !showPowerBI) return null;

    return (
      <Fragment>
        {showTableau && this.renderAnalyzeButton(la('Tableau'), 'Tableau.svg', this.openTableau, 19, '-noImgHover -noMinWidth')}
        {showPowerBI && this.renderAnalyzeButton(la('Power BI'), 'PBI Logo.svg', this.openPowerBi, 24, '-noImgHover -noMinWidth')}
      </Fragment>
    );
  };

  renderSaveButton = () => {
    const { dataset, disableButtons, location } = this.props;
    const mustSaveAs = dataset.getIn(['fullPath', 0]) === 'tmp';
    const isExtraDisabled = this.getExtraSaveDisable(dataset);
    const isSqlEditorTab = exploreUtils.isSqlEditorTab(location);
    const groupDropdownProps = {
      text: isSqlEditorTab ? 'Save Script' : 'Save View',
      onClick: isSqlEditorTab ? this.handleSaveScript : this.handleSaveView
    };

    return (
      <>
        <DropdownMenu
          className='explore-save-button'
          disabled={disableButtons}
          isButton
          groupDropdownProps={groupDropdownProps}
          menu={
            <SaveMenu
              action={this.doButtonAction}
              mustSaveAs={mustSaveAs}
              isSqlEditorTab={isSqlEditorTab}
              disableBoth={isExtraDisabled}
            />
          }
        />
      </>
    );
  };

  // this feature disabled for now
  renderRightTreeToggler() {
    return !this.props.rightTreeVisible
      ? <button
        className='info-button toogler'
        style={[style.pullout]}
        onClick={this.props.toggleRightTree}>
        <FontIcon type='Expand' />
      </button>
      : null;
  }

  renderHeaders() {
    switch (this.props.pageType) {
    case PageTypes.graph:
    case PageTypes.details:
    case PageTypes.reflections:
    case PageTypes.wiki:
      return;
    case PageTypes.default:
      return <div className='ExploreHeader'>
        {this.renderHeader()}
      </div>;
    default:
      throw new Error(`not supported page type; '${this.props.pageType}'`);
    }
  }

  getCancellable = jobStatus => {
    return jobStatus === JOB_STATUS.running
      || jobStatus === JOB_STATUS.starting
      || jobStatus === JOB_STATUS.enqueued
      || jobStatus === JOB_STATUS.pending
      || jobStatus === JOB_STATUS.metadataRetrieval
      || jobStatus === JOB_STATUS.planning
      || jobStatus === JOB_STATUS.engineStart
      || jobStatus === JOB_STATUS.queued
      || jobStatus === JOB_STATUS.executionPlanning;
  };

  handleEditOriginal = () => {
    const {
      dataset,
      editOriginalSql: editSql,
      datasetSummary,
      replaceUrlAction,
      getDatasetChangeDetails,
      showUnsavedChangesConfirmDialog: showConfirm
    } = this.props;

    const reapply = () => {
      editSql(dataset.get('id'), dataset.getIn(['apiLinks', 'self']));
    };

    if (this.isDatasetReadyForReapply()) {
      const {
        sqlChanged,
        historyChanged
      } = getDatasetChangeDetails();

      if (sqlChanged || historyChanged) {
        showConfirm({
          confirm: reapply
        });
      } else {
        reapply();
      }
    } else {
      replaceUrlAction(datasetSummary.links.edit);
    }
  };

  isDatasetReadyForReapply = () => {
    const {dataset } = this.props;
    return dataset.get('canReapply');
  }

  renderEditOriginalButton() {
    const { datasetSummary } = this.props;
    if (this.isDatasetReadyForReapply() || datasetSummary) {
      return (
        <Button
          variant='outlined'
          color='primary'
          size='medium'
          onClick={this.handleEditOriginal}
          disableRipple
          disableMargin>
          <FormattedMessage id='SQL.EditOriginal'/>
        </Button>
      );
    }
  }

  render() {
    const { dataset } = this.props;
    const { isSaveAsModalOpen } = this.state;
    const isSavedDataset = modelUtils.isNamedDataset(dataset);
    return (
      <div className='ExploreHeader__container'>
        {this.renderHeaders()}
        {isSaveAsModalOpen &&
          <SQLScriptDialog
            title='Save Script as...'
            mustSaveAs={dataset.getIn(['fullPath', 0]) === 'tmp'}
            isOpen={isSaveAsModalOpen}
            onCancel={() => this.setState({ isSaveAsModalOpen: false })}
            // eslint-disable-next-line
            script={{ context: this.props.queryContext, content: this.props.currentSql != null ? this.props.currentSql : this.props.datasetSql}}
            onSubmit={this.props.createScript}
            postSubmit={(payload) => {
              this.props.fetchScripts();
              this.props.setActiveScript({ script: payload });
            }}
            {...(isSavedDataset && { push: () => this.props.router.push({ pathname: '/new_query', state: { renderScriptTab: true } })})}
          />
        }
      </div>
    );
  }
}

function mapStateToProps(state, props) {
  const {location = {}} = props;
  const version = location.query && location.query.version;
  const explorePageState = getExploreState(state);
  const jobProgress = getJobProgress(state, version);
  const runStatus = getRunStatus(state).isRun;
  const jobId = getExploreJobId(state);
  return {
    location: state.routing.locationBeforeTransitions || {},
    history: getHistory(state, props.dataset.get('tipVersion')),
    tableColumns: getTableColumns(state, props.dataset.get('datasetVersion')),
    settings: state.resources.entities.get('setting'),
    jobProgress,
    runStatus,
    jobId,
    datasetSummary: state.resources.entities.get('datasetSummary'),
    queryContext: explorePageState.view.queryContext,
    showWiki: isWikAvailable(state, location),
    activeScript: getActiveScript(state)
  };
}

export default withRouter(connect(mapStateToProps, {
  transformHistoryCheck,
  performTransform,
  performTransformAndRun,
  runDatasetSql,
  previewDatasetSql,
  saveDataset,
  saveAsDataset,
  startDownloadDataset,
  performNextAction,
  showConfirmationDialog,
  cancelJob: cancelJobAndShowNotification,
  editOriginalSql,
  replaceUrlAction: replace,
  showUnsavedChangesConfirmDialog,
  setCurrentSql,
  createScript,
  fetchScripts,
  updateScript,
  setActiveScript
})(withDatasetChanges(ExploreHeader)));

const style = {
  base: {
    display: 'flex',
    justifyContent: 'flex-end',
    height: 52,
    padding: 0,
    margin: 0,
    borderBottom: 'none',
    borderTop: 'none',
    borderLeft: 'none',
    borderRight: 'none'
  },
  disabledStyle: {
    pointerEvents: 'none',
    opacity: 0.7
  },
  query: {
    textDecoration: 'none',
    width: 100,
    height: 28,
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#43B8C9',
    borderBottom: '1px solid #3399A8',
    borderRadius: 2,
    color: '#fff',
    ':hover': {
      backgroundColor: 'rgb(104, 198, 211)'
    }
  },
  leftWrap: {
    display: 'flex',
    maxWidth: 250,
    flexWrap: 'wrap',
    userSelect: 'text',
    marginRight: 150 // distance between a title and navigation buttons
  },
  leftPart: {
    display: 'flex',
    alignContent: 'center',
    alignItems: 'center'
  },
  dbName: {
    maxWidth: 300,
    display: 'flex',
    alignItems: 'center',
    color: '#333',
    fontWeight: 500
  },
  pullout: {
    backgroundColor: 'transparent',
    borderColor: 'transparent',
    position: 'relative',
    width: 30
  },
  noTextButton: {
    minWidth: 50,
    paddingRight: 10,
    paddingLeft: 5
  },
  actionBtnWrap: {
    marginBottom: 0,
    marginLeft: 0,
    minWidth: 80
  },
  narwhal: {
    Icon: {
      width: 22,
      height: 22
    },
    Container: {
      width: 24,
      height: 24,
      marginRight: 10
    }
  },
  titleWrap: {
    display: 'flex',
    alignItems: 'center'
  },
  triangle: {
    width: 0,
    height: 0,
    borderStyle: 'solid',
    borderWidth: '0 4px 6px 4px',
    borderColor: 'transparent transparent #fff transparent',
    position: 'absolute',
    zIndex: 99999,
    right: 6,
    top: -6
  },
  popover: {
    padding: 0
  },
  iconButton: {
    minWidth: 40,
    outline: 0
  },
  icon: {
    width: 20,
    height: 20,
    display: 'flex',
    margin: '0 auto'
  }
};
