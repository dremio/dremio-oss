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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import ImmutablePropTypes from 'react-immutable-proptypes';
import classNames from 'classnames';
import { formatMessage } from '@app/utils/locale';
import { getTableColumns } from '@app/selectors/explore';
import { columnPropTypes, DataColumn } from '@app/pages/ExplorePage/components/DataColumns/DataColumn';
import { SectionTitle } from '@app/pages/ExplorePage/components/Wiki/SectionTitle';
import {
  wrapper,
  list as listCls,
  title,
  column as columnCls
} from './DataColumnList.less';

const mapStateToProps = state => {
  const location = state.routing.locationBeforeTransitions || {};
  return {
    columns: getTableColumns(state, location.query && location.query.version, location)
  };
};

export class DataColumnListView extends PureComponent {
  static propTypes = {
    columns: ImmutablePropTypes.listOf(ImmutablePropTypes.contains(columnPropTypes)),
    className: PropTypes.string,
    titleClass: PropTypes.string
  };

  render() {
    const {
      columns,
      titleClass,
      className
    } = this.props;

    return (<div className={classNames(wrapper, className)}>
      <SectionTitle
        title={`${formatMessage('Dataset.Fields')} ${columns.size}`}
        titleClass={titleClass}
        className={title}
        />
      <div className={listCls}>
        {
          // call [toJS] right here, as it is a pure component and column is an only property
          columns.toJS().map((column, index) => {
            return <DataColumn key={index} className={columnCls} {...column} />;
          })
        }
      </div>
    </div>);
  }
}


export const DataColumnList = connect(mapStateToProps)(DataColumnListView);
