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
import { useRef } from "react";
import { injectIntl } from "react-intl";
import PropTypes from "prop-types";
import { TableColumns } from "@app/constants/Constants";
import Immutable from "immutable";
import localStorageUtils from "utils/storageUtils/localStorageUtils";
import FilterSelectMenu from "components/Fields/FilterSelectMenu";
import "./ShowHideColumn.less";

const disabledColumns = ["acceleration", "usr", "job", "st", "qt"];

const ShowHideColumn = ({
  intl: { formatMessage },
  defaultValue,
  updateColumnsState,
}) => {
  const indexRef = useRef({});

  const handleDragEnd = (itemIndex) => {
    const toIndex = indexRef.current.value;
    const allColumns = localStorageUtils.getJobColumns();
    const firstColumn = allColumns[0];

    const dropDownItems = getColumnFilterData();

    const [removed] = dropDownItems.splice(itemIndex, 1);
    dropDownItems.splice(toIndex, 0, removed);

    const updatedColumns = [firstColumn, ...dropDownItems];
    localStorageUtils.setJobColumns(updatedColumns);

    const swappedColumns = localStorageUtils
      .getJobColumns()
      .filter((item) => item.isSelected);
    updateColumnsState(swappedColumns);
  };

  const handleColumnPositionChange = (dragIndex, hoverIndex) => {
    indexRef.current.value = hoverIndex;
  };

  const filterColumnUnSelect = (item) => {
    const unSelectedColumn = localStorageUtils.getJobColumns().map((column) => {
      if (column.key === item && !disabledColumns.includes(item)) {
        column.isSelected = false;
        return column;
      }
      return column;
    });
    localStorageUtils.setJobColumns(unSelectedColumn);
    const columnsUnSelected = localStorageUtils
      .getJobColumns()
      .filter((data) => data.isSelected);
    updateColumnsState(columnsUnSelected);
  };

  const filterColumnSelect = (item) => {
    const selectedColumn = localStorageUtils.getJobColumns().map((column) => {
      if (column.key === item) {
        column.isSelected = true;
        return column;
      }
      return column;
    });
    localStorageUtils.setJobColumns(selectedColumn);
    const columnsSelected = localStorageUtils
      .getJobColumns()
      .filter((data) => data.isSelected);
    updateColumnsState(columnsSelected);
  };

  const getColumnFilterData = () => {
    //for 18.1.0 release only need to be changed in the next update Ticket Number DX-37189
    const filteredColumnsData = localStorageUtils.getJobColumns()
      ? localStorageUtils
          .getJobColumns()
          .filter((item) => item.key !== "jobStatus")
      : TableColumns.filter((item) => item.key !== "jobStatus");
    return filteredColumnsData;
  };

  return (
    <FilterSelectMenu
      selectedToTop={false}
      noSearch
      selectedValues={defaultValue}
      onItemSelect={filterColumnSelect}
      onItemUnselect={filterColumnUnSelect}
      items={getColumnFilterData()}
      label={formatMessage({ id: "Common.Columns" })}
      name="col"
      showSelectedLabel={false}
      icon="ManageColumn.svg"
      isArtIcon
      iconStyle={styles.gear}
      iconClass="showHideColumn__settingIcon"
      checkBoxClass="showHideColumn__checkBox"
      selectClass='showHideColumn'
      popoverFilters="margin-top--half"
      onDragMove={handleColumnPositionChange}
      onDragEnd={handleDragEnd}
      isDraggable
      hasIconFirst
      selectType='button'
    />
  );
};

ShowHideColumn.propTypes = {
  intl: PropTypes.object.isRequired,
  defaultValue: PropTypes.instanceOf(Immutable.List),
  updateColumnsState: PropTypes.func,
};
const styles = {
  gear: {
    color: "#393D41",
    fontSize: "14px",
    paddingLeft: "7px",
    paddingTop: "1px",
  },
};
export default injectIntl(ShowHideColumn);
