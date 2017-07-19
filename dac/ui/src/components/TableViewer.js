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
import { Table, Tr } from 'reactable';
import Immutable from 'immutable';

export default class TableViewer extends Component {
  static propTypes = {
    tableData: PropTypes.object,
    className: PropTypes.string
    // other props passed to reactable Table
    // columns: PropTypes.array
  }

  static defaultProps = {
    tableData: Immutable.List()
  }

  render() {
    const { tableData, className, ...tableProps } = this.props;
    return (
      <Table
        className={`table table-striped ${className || ''}`}
        {...tableProps}>
        {tableData.toJS().map((row, index) => {
          const rowClassName = row.rowClassName;
          return <Tr className={rowClassName} key={index} data={row.data} />;
        })}
      </Table>
    );
  }
}
