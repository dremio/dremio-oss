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
import { Link } from 'react-router';
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import DocumentTitle from 'react-document-title';
import { injectIntl } from 'react-intl';

import MainInfoMixin from 'dyn-load/pages/HomePage/components/MainInfoMixin';

import DatasetMenu from 'components/Menus/HomePage/DatasetMenu';
import FolderMenu from 'components/Menus/HomePage/FolderMenu';
import { UnformattedEntityMenu } from 'components/Menus/HomePage/UnformattedEntityMenu';

import BreadCrumbs from 'components/BreadCrumbs';
import { getRootEntityType } from 'utils/pathUtils';
import SettingsBtn from 'components/Buttons/SettingsBtn';
import FontIcon from 'components/Icon/FontIcon';

import { tableStyles } from '../tableStyles';
import BrowseTable from './BrowseTable';
import { HeaderButtons } from './HeaderButtons';
import MainInfoItemName from './MainInfoItemName';

@injectIntl
@Radium
@MainInfoMixin
export default class MainInfo extends Component {

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    entityType: PropTypes.string,
    viewState: PropTypes.instanceOf(Immutable.Map),
    updateRightTreeVisibility: PropTypes.func,
    rightTreeVisible: PropTypes.bool,
    isInProgress: PropTypes.bool,
    intl: PropTypes.object.isRequired
  };

  static defaultProps = {
    viewState: Immutable.Map()
  };

  getButtonsDataCell(dataType, name, item) {
    const finalDataType = dataType || 'VIRTUAL_DATASET';
    switch (finalDataType) {
    case 'folder':
      return this.getFolderActions(item);
    case 'dataset':
      return <ActionWrap>
        {this.getSettingsBtnByType(<DatasetMenu entity={item} entityType='dataset'/>, item)}
        {this.renderQueryButton(item)}
      </ActionWrap>;
    case 'raw':
    case 'database':
    case 'table':
      return <ActionWrap>{this.renderQueryButton(item)}</ActionWrap>;
    case 'VIRTUAL_DATASET': // todo: DRY with `dataset`, and why all caps?
      return <ActionWrap>
        {this.getSettingsBtnByType(<DatasetMenu entity={item} entityType='dataset'/>, item)}
        {this.renderQueryButton(item)}
      </ActionWrap>;
    case 'file':
      return <ActionWrap>{this.getFileMenu(item, this.renderQueryButton(item))}</ActionWrap>;
    case 'physicalDatasets': // todo: why is only this plural?
      return <ActionWrap>
        {this.getSettingsBtnByType(<DatasetMenu entity={item} entityType='physicalDataset'/>, item)}
        {this.renderQueryButton(item)}
      </ActionWrap>;
    default:
      throw new Error('unknown dataType');
    }
  }

  getFolderActions(folder) {
    if (folder.get('fileSystemFolder')) {
      if (folder.get('queryable')) {
        return <ActionWrap>
          {this.getSettingsBtnByType(<DatasetMenu entity={folder} entityType='folder'/>, folder)}
          {this.renderQueryButton(folder)}
        </ActionWrap>;
      }
      return (
        <ActionWrap>
          {this.getSettingsBtnByType(<UnformattedEntityMenu entity={folder}/>, folder)}
          {this.renderConvertButton(folder, {
            icon: <FontIcon type='FolderConvert'/>,
            to: {...this.context.location, state: {
              modal: 'DatasetSettingsModal',
              tab: 'format',
              entityType: folder.get('entityType'),
              entityId: folder.get('id'),
              query: {then: 'query'}
            }}
          })}
        </ActionWrap>
      );
    }

    return <ActionWrap>{this.getSettingsBtnByType(<FolderMenu folder={folder}/>, folder)}</ActionWrap>;
  }

  getFileMenu(file, queryBtn) {
    if (file.get('queryable')) {
      return <ActionWrap>
        {this.getSettingsBtnByType(<DatasetMenu entity={file} entityType='file'/>, file)}
        {queryBtn}
      </ActionWrap>;
    }
    return (
      <div style={{display: 'flex'}}>
        {this.getSettingsBtnByType(<UnformattedEntityMenu entity={file}/>, file)}
        {this.renderConvertButton(file, {
          icon: <FontIcon type='FileConvert'/>,
          to: {...this.context.location, state: {
            modal: 'DatasetSettingsModal',
            tab: 'format',
            entityType: file.get('entityType'),
            entityId: file.get('id'),
            queryable: file.get('queryable'),
            fullPath: file.get('filePath'),
            query: {then: 'query'}
          }}
        })}
      </div>
    );
  }

  getSettingsBtnByType(menu, item) {
    return (
      <SettingsBtn
        handleSettingsClose={this.handleSettingsClose.bind(this)}
        handleSettingsOpen={this.handleSettingsOpen.bind(this)}
        dataQa={item.get('name')}
        menu={menu}
      />
    );
  }

  getRow(item) {
    //@TODO API for folders not correct, need to change
    const [name, datasets, jobs, descendant, action] = this.getTableColumns();
    const isFolder = item.get('entityType') === 'folder';
    const datasetCount = isFolder
      ? item.getIn(['extendedConfig', 'datasetCount'])
      : item.get('datasetCount');
    const jobsCount = item.get('jobCount') || item.getIn(['extendedConfig', 'jobCount']) || 0;
    const descendantsCount = isFolder ? item.getIn(['extendedConfig', 'descendants']) : item.get('descendants');
    const descendantsNode = isFolder
      ? item.getIn(['extendedConfig', 'descendants'])
      : this.getDescendantLink(item);
    return {
      rowClassName: item.get('name'),
      data: {
        [name.key]: {
          node: () => <MainInfoItemName item={item}/>,
          value: item.get('name')
        },
        [datasets.key]: {
          node: () => datasetCount || datasetCount === 0 ? datasetCount : '—',
          value: datasetCount === undefined ? -1 : datasetCount
        },
        [jobs.key]: {
          node: () => <Link to={item.getIn(['links', 'jobs'])}>{jobsCount}</Link>,
          value: jobsCount
        },
        [descendant.key]: {
          node: () => descendantsNode || descendantsNode === 0 ? descendantsNode : '—',
          value: descendantsCount === undefined ? -1 : descendantsCount
        },
        // todo: fileType vs entityType?
        [action.key]: {
          node: () => this.getButtonsDataCell(item.get('fileType'), item.get('name'), item)
        }
      }
    };
  }

  getTableColumns() {
    const { intl } = this.props;
    return [
      { key: 'name', title: intl.formatMessage({id: 'Common.Name'}), flexGrow: 1 },
      { key: 'datasets', title: intl.formatMessage({id: 'Dataset.Datasets'}), style: tableStyles.datasetsColumn },
      { key: 'jobs', title: intl.formatMessage({id: 'Job.Jobs'}), style: tableStyles.digitColumn, width: 50 },
      { key: 'descendants', title: intl.formatMessage({id: 'Dataset.Descendants'}), style: tableStyles.digitColumn },
      {
        key: 'action',
        title: intl.formatMessage({id: 'Common.Action'}),
        style: tableStyles.actionColumn,
        width: 150,
        disableSort: true
      }
    ];
  }

  getTableData() {
    const contents = this.props.entity && this.props.entity.get('contents');
    let rows = Immutable.List();
    if (contents && !contents.isEmpty()) {
      const appendRow = dataset => {
        rows = rows.push(this.getRow(dataset));
      };
      contents.get('datasets').forEach(appendRow);
      contents.get('folders').forEach(appendRow);
      contents.get('files').forEach(appendRow);
      contents.get('physicalDatasets').forEach(appendRow);
    }
    return rows;
  }

  handleSettingsClose(settingsWrap) {
    $(settingsWrap).parents('tr').removeClass('hovered');
  }

  handleSettingsOpen(settingsWrap) {
    $(settingsWrap).parents('tr').addClass('hovered');
  }

  isReadonly(spaceList) {
    const { pathname } = this.context.location;
    if (spaceList !== undefined) {
      return !!spaceList.find((item) => {
        if (item.href === pathname) {
          return item.readonly;
        }
      });
    }
    return false;
  }

  toggleRightTree = () => {
    this.props.updateRightTreeVisibility(!this.props.rightTreeVisible);
  }

  renderQueryButton(item) {
    const href = item.getIn(['links', 'query']);
    return <Link to={href} className='main-settings-btn min-btn'>
      <button className='settings-button'>
        <FontIcon type='Query'/>
      </button>
    </Link>;
  }

  render() {
    const { entity, viewState } = this.props;
    const { pathname } = this.context.location;

    const buttons = entity && <HeaderButtons
      entity={entity}
      rootEntityType={getRootEntityType(entity.getIn(['links', 'self']))}
      rightTreeVisible={this.props.rightTreeVisible}
      toggleVisibility={this.toggleRightTree}
    />;

    return (
      <BrowseTable
        title={entity && <BreadCrumbs fullPath={entity.get('fullPathList')} pathname={pathname} />}
        buttons={buttons}
        key={pathname} /* trick to clear out the searchbox on navigation */
        columns={this.getTableColumns()}
        tableData={this.getTableData()}
        viewState={viewState}
      >
        <DocumentTitle title={entity && BreadCrumbs.formatFullPath(entity.get('fullPathList')).join('.') || ''} />
      </BrowseTable>
    );
  }
}

export const styles = {
  height: {
    height: '100%'
  },
  loader: {
    display: 'flex',
    justifyContent: 'center',
    color: 'gray',
    marginTop: 10,
    fontSize: 22
  },
  viewerHeader: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between'
  },
  button: {
    borderRadius: '2px',
    marginRight: '6px',
    height: 23,
    width: 68,
    boxShadow: '0 1px 1px #b2bec7',
    cursor: 'pointer',
    display: 'flex',
    paddingTop: 1
  },
  searchField: {
    width: 200,
    height: 30
  }
};


function ActionWrap({children}) {
  return <span className='action-wrap'>{children}</span>;
}
ActionWrap.propTypes = {
  children: PropTypes.node
};
