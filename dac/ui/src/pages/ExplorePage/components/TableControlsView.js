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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';

import ExploreTableColumnFilter from 'pages/ExplorePage/components/ExploreTable/ExploreTableColumnFilter';

import { Button } from 'dremio-ui-lib';
import ExploreCopyTableButton from '@app/pages/ExplorePage/components/ExploreTable/ExploreCopyTableButton';

import { sqlEditorButton } from 'uiTheme/radium/buttons';

import modelUtils from '@app/utils/modelUtils';
import { isSqlChanged } from '@app/sagas/utils';
import { CombinedActionMenu } from '@app/components/Menus/ExplorePage/CombinedActionMenu';
import { navigateToExploreDefaultIfNecessary } from '@app/utils/pathUtils';
import DropdownMenu from '@app/components/Menus/DropdownMenu';
import { PHYSICAL_DATASET_TYPES } from '@app/constants/datasetTypes';
import Art from '@app/components/Art';

import './TableControls.less';
import { memoOne } from '@app/utils/memoUtils';

const datasetColumnsMemoize = memoOne((tableColumns) => {
  return tableColumns && tableColumns.map(column => column.get('type')).toJS() || [];
});
@injectIntl
@Radium
class TableControls extends PureComponent {

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    tableColumns: PropTypes.instanceOf(Immutable.List).isRequired,

    groupBy: PropTypes.func.isRequired,
    addField: PropTypes.func,
    join: PropTypes.func.isRequired,
    rightTreeVisible: PropTypes.bool,
    approximate: PropTypes.bool,
    intl: PropTypes.object.isRequired,
    columnCount: PropTypes.number,
    disableButtons: PropTypes.bool,

    saveDataset: PropTypes.func,
    saveAsDataset: PropTypes.func,
    performNextAction: PropTypes.func,
    runDatasetSql: PropTypes.func,
    previewDatasetSql: PropTypes.func,
    needsTransform: PropTypes.func,
    queryContext: PropTypes.func,
    performTransform: PropTypes.func,
    transformHistoryCheck: PropTypes.func,
    pageType: PropTypes.string,
    showConfirmationDialog: PropTypes.func,
    startDownloadDataset: PropTypes.func,
    currentSql: PropTypes.string,
    history: PropTypes.object,
    location: PropTypes.object
  };

  constructor(props) {
    super(props);

    this.state = {
      tooltipState: false,
      anchorOrigin: {
        horizontal: 'right',
        vertical: 'bottom'
      },
      targetOrigin: {
        horizontal: 'right',
        vertical: 'top'
      },
      isDownloading: false
    };
  }


  renderCopyToClipboard = () => {
    const { exploreViewState, dataset } = this.props;

    const isDataAvailable = !exploreViewState.get('invalidated')
      && !exploreViewState.get('isFailed')
      && !exploreViewState.get('isInProgress');
    const version = dataset && dataset.get('datasetVersion');

    return (isDataAvailable && version) ? <ExploreCopyTableButton version={version} style={styles.copy}/> : null;
  };

  isTransformNeeded() {
    const { dataset, queryContext, currentSql, needsTransform } = this.props;
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

  isCreatedAndNamedDataset() {
    const { dataset } = this.props;
    return dataset.get('datasetVersion') !== undefined && modelUtils.isNamedDataset(dataset);
  }

  // unlike acceleration button, settings button is always shown, but it is disabled when
  // show acceleration button is hidden or disabled.
  shouldEnableSettingsButton() {
    return this.isCreatedAndNamedDataset() && !this.isEditedDataset();
  }

  isNewDataset() {
    const { mode } = this.props.location.query;
    return modelUtils.isNewDataset(this.props.dataset, mode);
  }

  updateDownloading = () => {
    this.setState({
      isDownloading: !this.state.isDownloading
    });
  }

  // ellipsis button with settings, download, and analyze options
  renderSavedButton = () => {
    const { dataset, tableColumns } = this.props;
    const isSettingsDisabled = !this.shouldEnableSettingsButton();
    const isActionDisabled = dataset.get('isNewQuery') || !dataset.get('datasetType'); // not new query nor loaded
    const datasetColumns = datasetColumnsMemoize(tableColumns);

    return (
      <DropdownMenu
        className='explore-ellipsis-button'
        iconType='Ellipsis'
        disabled={(isSettingsDisabled && isActionDisabled) || this.state.isDownloading}
        isDownloading={this.state.isDownloading}
        isButton
        menu={<CombinedActionMenu
          dataset={dataset}
          datasetColumns={datasetColumns}
          downloadAction={this.downloadDataset}
          isSettingsDisabled={isSettingsDisabled}
          updateDownloading={this.updateDownloading}
        />}
      />
    );
  };

  render() {
    const {
      dataset,
      addField,
      groupBy,
      join,
      columnCount,
      intl,
      disableButtons
    } = this.props;

    return (
      <div className='table-controls'>
        <div className='left-controls'>
          <div className='controls' style={styles.controlsInner}>
            { columnCount }
            <Button
              variant='outlined'
              color='primary'
              size='medium'
              onClick={addField}
              disableRipple
              disabled={disableButtons}>
              <Art src='AddFields.svg' alt='addFields' title='AddFields' style={styles.blueIcon}/>
              { intl.formatMessage({ id: 'Dataset.AddField' }) }
            </Button>
            <Button
              variant='outlined'
              color='primary'
              size='medium'
              onClick={groupBy}
              disableRipple
              disabled={disableButtons}>
              <Art src='GroupBy.svg' alt='groupBy' title='GroupBy' style={styles.blueIcon}/>
              { intl.formatMessage({ id: 'Dataset.GroupBy' }) }
            </Button>
            <Button
              variant='outlined'
              color='primary'
              size='medium'
              onClick={join}
              disableRipple
              disabled={disableButtons}>
              <Art src='Join.svg' alt='join' title='Join' style={styles.blueIcon}/>
              { intl.formatMessage({ id: 'Dataset.Join' }) }
            </Button>
            <div className='table-controls__right' style={styles.right}>
              {this.renderCopyToClipboard()}
              <ExploreTableColumnFilter
                dataset={dataset}
              />
              { this.renderSavedButton() }
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export const styles = {
  disabledStyle: {
    pointerEvents: 'none',
    opacity: 0.7
  },
  innerTextStyle: {
    textAlign: 'left'
  },
  activeButton: {
    ...sqlEditorButton,

    color: 'rgb(0, 0, 0)',
    ':hover': {
      backgroundColor: 'rgb(229, 242, 247)'
    }
  },
  iconBox: {
    width: 24,
    height: 24
  },
  iconContainer: {
    marginRight: 1,
    width: 24,
    position: 'relative'
  },
  tableControls: {
    marginTop: 0,
    marginLeft: 0,
    paddingLeft: 8,
    paddingRight: 8,
    height: 42,
    display: 'flex',
    alignItems: 'center'
  },
  controlsInner: {
    height: 24
  },
  copy: {
    marginRight: 10,
    marginTop: 5
  },
  right: {
    display: 'flex',
    flex: 1,
    justifyContent: 'flex-end'
  },
  blueIcon: {
    height: 24,
    width: 24,
    filter: 'invert(64%) sepia(28%) saturate(773%) hue-rotate(139deg) brightness(93%) contrast(101%)'
  }
};

export default TableControls;
