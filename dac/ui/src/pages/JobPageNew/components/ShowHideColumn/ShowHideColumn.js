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
import { useState } from 'react';
import { injectIntl } from 'react-intl';
import PropTypes from 'prop-types';
import { TableColumns } from '@app/constants/Constants';
import Immutable from 'immutable';
import jobsUtils from 'utils/jobsUtils';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import FilterSelectMenu from 'components/Fields/FilterSelectMenu';
import './ShowHideColumn.less';

const ShowHideColumn = ({
  intl: { formatMessage },
  defaultValue,
  updateColumnsState
}) => {
  const [selectClass, setSelectClass] = useState('');

  const handleColumnPositionChange = (type, index) => {
    let arrayToStore = [];
    index = index + 1;
    const jobColumns = localStorageUtils.getJobColumns();
    const columnsLength = jobColumns.length;
    if (type === 'down' && index >= 0 && index < columnsLength - 1) {
      arrayToStore = jobsUtils.moveArrayElement(jobColumns, index, index + 1);
    } else if (type === 'up' && index > 1 && index < columnsLength) {
      arrayToStore = jobsUtils.moveArrayElement(jobColumns, index, index - 1);
    }
    localStorageUtils.setJobColumns(
      arrayToStore.length > 0
        ?
        arrayToStore
        :
        jobColumns
    );
    const swappedColumns = localStorageUtils.getJobColumns()
      .filter(item => item.isSelected);
    updateColumnsState(swappedColumns);
  };
  const filterColumnUnSelect = (item) => {
    const unSelectedColumn = localStorageUtils.getJobColumns()
      .map(column => {
        if (column.key === item && item !== 'reflection') {
          column.isSelected = false;
          return column;
        }
        return column;
      });
    localStorageUtils.setJobColumns(unSelectedColumn);
    const columnsUnSelected = localStorageUtils.getJobColumns()
      .filter(data => data.isSelected);
    updateColumnsState(columnsUnSelected);
  };

  const filterColumnSelect = (item) => {
    const selectedColumn = localStorageUtils.getJobColumns()
      .map(column => {
        if (column.key === item) {
          column.isSelected = true;
          return column;
        }
        return column;
      });
    localStorageUtils.setJobColumns(selectedColumn);
    const columnsSelected = localStorageUtils.getJobColumns()
      .filter(data => data.isSelected);
    updateColumnsState(columnsSelected);
  };

  const getColumnFilterData = () => {
    //for 18.1.0 release only need to be changed in the next update Ticket Number DX-37189
    const filteredColumnsData = localStorageUtils.getJobColumns()
      ?
      localStorageUtils.getJobColumns().filter(item => item.key !== 'jobStatus')
      :
      TableColumns.filter(item => item.key !== 'jobStatus');
    return filteredColumnsData;
  };

  return (
    <FilterSelectMenu
      selectViewBeforeOpen={() => setSelectClass('showHideBackgroundColor')}
      selectViewBeforeClose={() => setSelectClass('')}
      selectedToTop={false}
      noSearch
      selectedValues={defaultValue}
      onItemSelect={filterColumnSelect}
      onItemUnselect={filterColumnUnSelect}
      items={getColumnFilterData()}
      label={formatMessage({ id: 'Common.Columns' })}
      name='col'
      showSelectedLabel={false}
      icon='Gear.svg'
      isArtIcon
      iconStyle={styles.gear}
      iconClass='showHideColumn__settingIcon'
      onClick={handleColumnPositionChange}
      checkBoxClass='showHideColumn__checkBox'
      className='showHideColumn__label margin-top--none'
      popoverFilters='margin-top--half'
      selectClass={selectClass}
      showCheckIcon
    />
  );
};

ShowHideColumn.propTypes = {
  intl: PropTypes.object.isRequired,
  defaultValue: PropTypes.instanceOf(Immutable.List),
  updateColumnsState: PropTypes.func
};
const styles = {
  gear: {
    color: '#393D41',
    fontSize: '14px',
    paddingLeft: '7px',
    paddingTop: '1px'
  }
};
export default injectIntl(ShowHideColumn);
