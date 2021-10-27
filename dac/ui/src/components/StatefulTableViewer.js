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
import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable, { List } from 'immutable';

import ViewStateWrapper from 'components/ViewStateWrapper';
import TableViewer from 'components/TableViewer';
import EmptyTableMessage from 'components/EmptyTableMessage';
import VirtualizedTableViewer from 'components/VirtualizedTableViewer';
import browserUtils from 'utils/browserUtils';

export default class StatefulTableViewer extends Component {

  static propTypes = {
    virtualized: PropTypes.bool,
    viewState: PropTypes.instanceOf(Immutable.Map),
    tableData: PropTypes.oneOfType([
      PropTypes.instanceOf(Immutable.List),
      PropTypes.array
    ]),
    noDataText: PropTypes.string,
    rowHeight: PropTypes.number,
    onClick: PropTypes.func,
    resizableColumn: PropTypes.bool,
    loadNextRecords: PropTypes.func,
    disableSort: PropTypes.bool,
    disableZebraStripes: PropTypes.any
    // extra props passed along to underlying Table impl
    // columns: PropTypes.array.isRequired,
    // className: PropTypes.string,
  };

  static defaultProps = { // todo: loc
    virtualized: false,
    className: '',
    noDataText: 'No Items'
  };

  renderTableContent() {
    const {
      viewState, tableData, virtualized, noDataText,
      onClick, resizableColumn, loadNextRecords, disableSort, disableZebraStripes, ...passAlongProps
    } = this.props;
    const data = viewState && viewState.get('isInProgress') ? Immutable.List() : tableData;
    const tableProps = {
      tableData: data,
      resizableColumn,
      loadNextRecords,
      onClick,
      ...passAlongProps
    };
    const tableSize = List.isList(tableData) ? tableData.size : tableData.length;
    const tableViewer = virtualized
      ? <VirtualizedTableViewer {...tableProps} disableSort={disableSort} disableZebraStripes={disableZebraStripes}/>
      : <TableViewer {...tableProps} />;
    if (!(viewState && viewState.get('isInProgress')) && tableSize === 0) {
      // here we skip showing empty virtualized table header because of IE11 issues with flex box
      // in this particular case grid for react-virtualized computed with wrong offsetWidth
      const emptyTableViewer = (browserUtils.getPlatform().name === 'IE' && virtualized) ? null : tableViewer;
      return (
        <EmptyTableMessage noDataText={noDataText} tableViewer={emptyTableViewer} />
      );
    }
    return tableViewer;
  }

  render() {
    const { viewState } = this.props;
    return (
      <div style={styles.base}>
        {viewState && !viewState.get('isInProgress')
          && <ViewStateWrapper style={{ height: 'auto' }} viewState={viewState} />
        }
        {this.renderTableContent()}
        { //position: relative needed to fit spinner and overlay under the table header.
          viewState && viewState.get('isInProgress')
          && <ViewStateWrapper style={{ position: 'relative' }} viewState={viewState} />
        }
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    width: '100%',
    overflow: 'hidden', // avoid flickering: since the child of the div with this prop is always AutoSizer, we do not expect overflows
    position: 'relative' // needed for correct positioning of .empty-message
  },
  emptyTable: {
    width: '100%'
  }
};
