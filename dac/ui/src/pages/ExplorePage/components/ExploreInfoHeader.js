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
import classNames from 'classnames';
import Immutable from 'immutable';
import DocumentTitle from 'react-document-title';
import { injectIntl } from 'react-intl';

import CopyButton from '@app/components/Buttons/CopyButton';
import Button from '@app/components/Buttons/Button';
import * as ButtonTypes from '@app/components/Buttons/ButtonTypes';

import DropdownMenu from '@app/components/Menus/DropdownMenu';
import EllipsedText from 'components/EllipsedText';
import modelUtils from 'utils/modelUtils';
import { constructFullPath, navigateToExploreDefaultIfNecessary } from 'utils/pathUtils';
import { formatMessage } from 'utils/locale';
import { needsTransform, isSqlChanged } from 'sagas/utils';

import { PHYSICAL_DATASET_TYPES } from '@app/constants/datasetTypes';
//actions
import { saveDataset, saveAsDataset } from 'actions/explore/dataset/save';
import { performTransform, transformHistoryCheck } from 'actions/explore/dataset/transform';
import { performTransformAndRun, runDatasetSql, previewDatasetSql } from 'actions/explore/dataset/run';
import { showConfirmationDialog } from 'actions/confirmation';
import { PageTypeButtons } from '@app/pages/ExplorePage/components/PageTypeButtons';
import { pageTypesProp } from '@app/pages/ExplorePage/pageTypes';

import { startDownloadDataset } from 'actions/explore/download';
import { performNextAction, NEXT_ACTIONS } from 'actions/explore/nextAction';

import DatasetAccelerationButton from 'dyn-load/components/Acceleration/DatasetAccelerationButton';
import ExploreInfoHeaderMixin from 'dyn-load/pages/ExplorePage/components/ExploreInfoHeaderMixin';
import config from 'dyn-load/utils/config';
import { getAnalyzeToolsConfig } from '@app/utils/config';

import SaveMenu, { DOWNLOAD_TYPES } from 'components/Menus/ExplorePage/SaveMenu';
import CombinedActionMenu from '@app/components/Menus/ExplorePage/CombinedActionMenu';
import BreadCrumbs from 'components/BreadCrumbs';
import FontIcon from 'components/Icon/FontIcon';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import SimpleButton from '@app/components/Buttons/SimpleButton';
import Art from '@app/components/Art';

import { getIconDataTypeFromDatasetType } from 'utils/iconUtils';

import { PALE_NAVY } from 'uiTheme/radium/colors';
import { getHistory, getTableColumns, getExploreState } from 'selectors/explore';

import './ExploreInfoHeader.less';

export const TABLEAU_TOOL_NAME = 'Tableau';
export const QLIK_TOOL_NAME = 'Qlik Sense';

@injectIntl
@Radium
@ExploreInfoHeaderMixin
export class ExploreInfoHeader extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    pageType: pageTypesProp,
    toggleRightTree: PropTypes.func.isRequired,
    grid: PropTypes.object,
    space: PropTypes.object,
    rightTreeVisible: PropTypes.bool,
    location: PropTypes.object,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,

    // connected
    history: PropTypes.instanceOf(Immutable.Map),
    queryContext: PropTypes.instanceOf(Immutable.List),
    currentSql: PropTypes.string,
    tableColumns: PropTypes.instanceOf(Immutable.List),
    settings: PropTypes.instanceOf(Immutable.Map),

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
    showConfirmationDialog: PropTypes.func
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
  }

  doButtonAction(actionType) {
    switch (actionType) {
    case 'saveAs':
      return this.handleSaveAs();
    case 'run':
      return this.handleRunClick();
    case 'preview':
      return this.handlePreviewClick();
    case 'save':
      return this.handleSave();
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

  handleSave = () => {
    const nextAction = this.state.nextAction;
    this.setState({nextAction: undefined});
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

  handleSaveAs = () => {
    const nextAction = this.state.nextAction;
    this.setState({nextAction: undefined});
    this.transformIfNecessary(
      () => this.props.saveAsDataset(nextAction)
    );
  };

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
    const nameForDisplay = ExploreInfoHeader.getNameForDisplay(dataset);
    const isEditedDataset = this.isEditedDataset();
    const nameStyle = isEditedDataset ? { fontStyle: 'italic' } : {};
    const fullPath = ExploreInfoHeader.getFullPathListForDisplay(dataset);
    const edited = this.props.intl.formatMessage({ id: 'Dataset.Edited' });
    return (
      <DatasetItemLabel
        customNode={ // todo: string replace loc
          <div className='flexbox-truncate-text-fix'>
            <div style={{...style.dbName}} data-qa={nameForDisplay}>
              <EllipsedText style={nameStyle} text={`${nameForDisplay}${isEditedDataset ? edited : ''}`}>
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
                ? BreadCrumbs.formatFullPath(fullPath).join('.') + (isEditedDataset ? '*' : '')
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
      />
    );
  }

  renderLeftPartOfHeader(dataset) {
    if (!dataset.get('datasetType')) {
      return <div style={style.leftPart}/>;
    }
    return (
      <div style={style.leftPart}>
        <div style={style.leftWrap}>
          <div className='title-wrap' style={[style.titleWrap]}>
            {this.renderDatasetLabel(dataset)}
          </div>
        </div>
        <PageTypeButtons dataQa='page-type-buttons' selectedPageType={this.props.pageType} dataset={dataset} />
      </div>
    );
  }

  renderRightPartOfHeader() {
    return (
      <div className='right-part'>
        { this.renderAccelerationButton() }
        { this.renderAnalyzeButtons() }
        { this.renderEllipsisButton() }
        { this.renderSaveButton() }
        { this.renderRunButton('preview', this.doButtonAction) }
        { this.renderRunButton('run', this.doButtonAction) }
        { /* this feature disabled for now
            <div style={[style.divider]} />
            {this.renderRightTreeToggler()}
          */ }
      </div>
    );
  }

  renderAccelerationButton = () => {
    if (!this.showAccelerationButton()) {
      return null;
    }
    const fullPath = ExploreInfoHeader.getFullPathListForDisplay(this.props.dataset);
    return (
      <DatasetAccelerationButton
        style={{ marginLeft: 20 }}
        fullPath={fullPath}
        isEditedDataset={this.isEditedDataset()}/>
    );
  };

  openTableau = () => {
    this.handleShowBI(NEXT_ACTIONS.openTableau);
  };
  openPowerBi = () => {
    this.handleShowBI(NEXT_ACTIONS.openPowerBI);
  };

  renderAnalyzeButton = (name, icon, onclick, iconSize) => {
    return (<SimpleButton buttonStyle='secondary' onClick={onclick} data-qa={name} style={style.iconButton}>
      <Art src={icon} alt={name} title={name} style={{...style.icon, height: iconSize, width: iconSize}}/>
    </SimpleButton> );
  };
  renderAnalyzeButtons = () => {
    const { settings } = this.props;

    const analyzeToolsConfig = getAnalyzeToolsConfig(settings, config);
    const showTableau = analyzeToolsConfig.tableau.enabled;
    const showPowerBI = analyzeToolsConfig.powerbi.enabled;
    if (!showTableau && !showPowerBI) return null;
    return (
      <Fragment>
        {showPowerBI && this.renderAnalyzeButton(la('Power BI'), 'PowerBi.svg', this.openPowerBi, 24)}
        {showTableau && this.renderAnalyzeButton(la('Tableau'), 'Tableau.svg', this.openTableau, 19)}
      </Fragment>
    );
  };

  // ellipsis button with settings, download, and analyze options
  renderEllipsisButton = () => {
    const { dataset, tableColumns } = this.props;
    const isSettingsDisabled = !this.shouldEnableSettingsButton();
    const isActionDisabled = !dataset.get('isNewQuery') && !dataset.get('datasetType'); // not new query nor loaded
    const datasetColumns = tableColumns && tableColumns.map(column => column.get('type')).toJS() || [];
    return (
      <DropdownMenu
        className='explore-ellipsis-button'
        iconType='Ellipsis'
        disabled={isSettingsDisabled && isActionDisabled}
        style={style.noTextButton}
        isButton
        menu={<CombinedActionMenu
          dataset={dataset}
          datasetColumns={datasetColumns}
          downloadAction={this.downloadDataset}
          action={this.doButtonAction}
          isSettingsDisabled={isSettingsDisabled}
        />}
      />
    );
  };

  renderSaveButton = () => {
    const { dataset } = this.props;
    const shouldEnableButtons = dataset.get('isNewQuery') || dataset.get('datasetType'); // new query or loaded
    const mustSaveAs = dataset.getIn(['fullPath', 0]) === 'tmp';

    return (
      <DropdownMenu
        className='explore-save-button'
        iconType='Save'
        disabled={!shouldEnableButtons}
        style={style.noTextButton}
        isButton
        menu={<SaveMenu action={this.doButtonAction} mustSaveAs={mustSaveAs}/>}
      />
    );
  };

  renderRunButton = (type, actionFn)  => {
    const doAction = () => actionFn(type); //type should be "preview" or "run" for doButtonAction

    const isMac = navigator.platform && navigator.platform.toUpperCase().indexOf('MAC') >= 0;
    const runKbdShortTxt = (isMac) ? '⌘+Shift+Enter' : 'Ctrl+Shift+Enter';
    const previewKbdShortTxt = (isMac) ? '⌘+Enter' : 'Ctrl+Enter';
    const title = (type === 'preview') ?
      `${la('Preview')} ${previewKbdShortTxt}` : `${la('Run')} ${runKbdShortTxt}`;

    const className = (type === 'preview') ? 'preview-button' : 'run-button';
    const btnType = (type === 'preview') ? ButtonTypes.SECONDARY : ButtonTypes.PRIMARY;
    const btnText = (type === 'preview') ? la('Preview') : la('Run');
    const dataQa = (type === 'preview') ? 'qa-preview' : 'qa-run';
    const icon = (type === 'preview') ? null : 'NarwhalReversed';
    const iconStyle = (type === 'preview') ? null : style.narwhal;

    return (
      <Button
        type={btnType}
        data-qa={dataQa}
        className={className}
        title={title}
        onClick={doAction}
        text={btnText}
        icon={icon}
        iconStyle={iconStyle}
        style={style.actionBtnWrap}
      />
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

  render() {
    const { dataset } = this.props;
    const classes = classNames('explore-info-header', { 'move-right': this.props.rightTreeVisible });
    const isInProgress = this.props.exploreViewState.get('isInProgress');

    return (
      <div className={classes} style={[style.base, isInProgress && style.disabledStyle]}>
        {this.renderLeftPartOfHeader(dataset)}
        {this.renderRightPartOfHeader()}
      </div>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const explorePageState = getExploreState(state);
  return {
    location: state.routing.locationBeforeTransitions || {},
    history: getHistory(state, ownProps.dataset.get('tipVersion')),
    currentSql: explorePageState.view.currentSql,
    queryContext: explorePageState.view.queryContext,
    tableColumns: getTableColumns(state, ownProps.dataset.get('datasetVersion')),
    settings: state.resources.entities.get('setting')
  };
}

export default connect(mapStateToProps, {
  transformHistoryCheck,
  performTransform,
  performTransformAndRun,
  runDatasetSql,
  previewDatasetSql,
  saveDataset,
  saveAsDataset,
  startDownloadDataset,
  performNextAction,
  showConfirmationDialog
})(ExploreInfoHeader);

const style = {
  base: {
    display: 'flex',
    justifyContent: 'space-between',
    height: 52,
    padding: 0,
    margin: 0,
    borderBottom: 'none',
    borderTop: 'none',
    borderLeft: 'none',
    borderRight: 'none',
    backgroundColor: PALE_NAVY
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
    marginRight: 50 // distance between a title and navigation buttons
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
  divider: {
    height: 28,
    borderLeft: '2px solid rgba(0,0,0,0.1)'
  },
  noTextButton: {
    minWidth: 50
  },
  actionBtnWrap: {
    marginBottom: 0,
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
