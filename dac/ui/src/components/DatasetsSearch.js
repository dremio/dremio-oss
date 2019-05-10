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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { Link } from 'react-router';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { injectIntl } from 'react-intl';
import Art from 'components/Art';
import { getIconDataTypeFromDatasetType } from 'utils/iconUtils';
import { TagList } from '@app/pages/HomePage/components/TagList';

import { bodySmall } from 'uiTheme/radium/typography';


import { PALE_NAVY, PALE_ORANGE } from 'uiTheme/radium/colors';
import DatasetItemLabel from './Dataset/DatasetItemLabel';
import './DatasetsSearch.less';

const emptyList = new Immutable.List();

@injectIntl
@Radium
@pureRender
export default class DatasetsSearch extends Component {
  static propTypes = {
    searchData: PropTypes.instanceOf(Immutable.List).isRequired,
    globalSearch: PropTypes.bool,
    handleSearchHide: PropTypes.func.isRequired,
    inputValue: PropTypes.string,
    searchViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired
  };

  getDatasetsList(searchData, inputValue) {
    const { globalSearch } = this.props;
    return searchData.map((value, key) => {
      const name = value.getIn(['fullPath', -1]);
      const datasetItem = (
        <div key={key} style={[styles.datasetItem, bodySmall]}
          data-qa={`ds-search-row-${name}`}
          className='search-result-row'>
          <div style={[styles.datasetData]}>
            <DatasetItemLabel
              name={name}
              showFullPath
              inputValue={inputValue}
              fullPath={value.get('displayFullPath')}
              typeIcon={getIconDataTypeFromDatasetType(value.get('datasetType'))}
            />
          </div>
          {/* DX-11249 <div style={styles.parentDatasetsHolder} data-qa='ds-parent'>
            {this.getParentItems(value, inputValue)}
          </div> */}
          <TagList tags={value.get('tags', emptyList)} style={{flex: 1, minWidth: 0}} />
          {this.getActionButtons(value)}
        </div>
      );
      return globalSearch
        ? <Link key={key} className='dataset' style={{textDecoration: 'none'}}
          to={value.getIn(['links', 'self'])}>{datasetItem}</Link>
        : datasetItem;
    });
  }

  getActionButtons(dataset) {
    return (
      <span className='main-settings-btn min-btn'
        style={[styles.actionButtons]}>
        {
          dataset.getIn(['links', 'edit']) &&
          <Link to={dataset.getIn(['links', 'edit'])}>
            <button className='settings-button' data-qa='edit'>
              <Art
                src={'Edit.svg'}
                alt={this.props.intl.formatMessage({ id: 'Common.Edit' })}
                style={styles.icon} />
            </button>
          </Link>
        }
        <Link to={dataset.getIn(['links', 'self'])}>
          <button className='settings-button' data-qa='query'>
            <Art
              src={'Query.svg'}
              alt={this.props.intl.formatMessage({ id: 'Common.DoQuery' })}
              style={styles.icon} />
          </button>
        </Link>
      </span>
    );
  }

  getParentItems(dataset, inputValue) {
    if (dataset && dataset.get('parents')) {
      return dataset.get('parents').map((value, key) => {
        if (!value.has('type')) return; // https://dremio.atlassian.net/browse/DX-7233

        const lastParent = value.get('datasetPathList').size < 1
          ? value.get('datasetPathList').size
          : value.get('datasetPathList').size - 1;
        return (
          <div key={`parent_${key}`} style={styles.parentDatasets}>
            <DatasetItemLabel
              name={value.get('datasetPathList').get(lastParent)}
              inputValue={inputValue}
              fullPath={value.get('datasetPathList')}
              showFullPath
              typeIcon={getIconDataTypeFromDatasetType(value.get('type'))}
            />
          </div>
        );
      }).filter(Boolean);
    }
  }

  getHeader(inputValue) {
    return (
      <h3 style={styles.header}>
        {la('Search Results for')} "{inputValue}"
        <Art
          src={'XBig.svg'}
          alt={this.props.intl.formatMessage({ id: 'Common.Close' })}
          style={styles.closeIcon}
          onClick={this.props.handleSearchHide.bind(this)} />
      </h3>
    );
  }

  render() {
    const { searchData, inputValue, searchViewState } = this.props;
    const searchBlock = searchData && searchData.size && searchData.size > 0
      ? <div>{this.getDatasetsList(searchData, inputValue)}</div>
      : <div style={styles.notFound}>{la('Not found')}</div>;
    return <section className='datasets-search' style={styles.main}>
      {this.getHeader(inputValue)}
      <div className='dataset-wrapper' style={styles.datasetWrapper}>
        <ViewStateWrapper viewState={searchViewState}>
          {searchBlock}
        </ViewStateWrapper>
      </div>
    </section>;
  }
}

const styles = {
  main: {
    background: '#fff',
    zIndex: 999,
    color: '#000',
    boxShadow: '-1px 1px 1px #ccc'
  },
  datasetItem: {
    padding: '10px',
    borderBottom: '1px solid rgba(0,0,0,.1)',
    height: 45,
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    ':hover': {
      background: PALE_ORANGE
    }
  },
  datasetData: {
    margin: '0 0 0 5px',
    minWidth: 300
  },
  header: {
    height: 38,
    width: '100%',
    background: PALE_NAVY,
    display: 'flex',
    alignItems: 'center',
    padding: '0 10px'
  },
  datasetWrapper: {
    maxHeight: 360,
    overflow: 'auto'
  },
  icon: {
    width: 24,
    height: 24
  },
  closeIcon: {
    margin: '0 0 0 auto',
    height: 24,
    cursor: 'pointer'
  },
  actionButtons: {
    margin: '0 0 0 auto'
  },
  parentDatasetsHolder: {
    display: 'flex'
  },
  parentDatasets: {
    display: 'flex',
    alignItems: 'center',
    margin: '0 10px 0 0'
  },
  hover: {
    ':hover': {}
  },
  notFound: {
    padding: '10px 10px'
  }
};
