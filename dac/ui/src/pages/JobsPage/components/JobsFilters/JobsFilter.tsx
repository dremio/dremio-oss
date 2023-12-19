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

import { useCallback, useEffect, useRef, useState } from "react";
import { intl } from "@app/utils/intl";
import { cloneDeep, debounce } from "lodash";
import Immutable from "immutable";
import { WithRouterProps, withRouter } from "react-router";
import { useResourceSnapshot } from "smart-resource/react";
import { CellMeasurerCache } from "react-virtualized";

import ContainsText from "@app/pages/JobPage/components/JobsFilters/ContainsText";
import StartTimeSelect from "@app/pages/JobPage/components/JobsFilters/StartTimeSelect/StartTimeSelect";
import * as IntervalTypes from "@app/pages/JobPage/components/JobsFilters/StartTimeSelect/IntervalTypes";
import FilterSelectMenu from "@app/components/Fields/FilterSelectMenu";

import { UsersForJobsResource } from "@app/exports/resources/UsersForJobsResource";
import { JobsQueryParams } from "dremio-ui-common/types/Jobs.types";
import { formatQueryState } from "dremio-ui-common/utilities/jobs.js";
import {
  FILTER_LABEL_IDS,
  GenericFilters,
  itemsForQueryTypeFilter,
  itemsForStateFilter,
  transformToItems,
  transformToSelectedItems,
} from "./utils";
import additionalJobsControls from "@inject/shared/AdditionalJobsControls";
import { QueuesResource } from "@app/resources/QueuesResource";

import * as classes from "./JobsFilters.module.less";

const cellCache = new CellMeasurerCache({
  fixedWidth: true,
  defaultHeight: 32,
});

type JobsFilterProps = {
  query: JobsQueryParams | undefined;
  manageColumns: any[];
  setManageColumns: React.Dispatch<any>;
} & WithRouterProps;

const JobsFilters = withRouter(
  ({
    query,
    router,
    location,
    manageColumns,
    setManageColumns,
  }: JobsFilterProps) => {
    const defaultStartTime = new Date(2015, 0).getTime();
    const startTime = query?.filters?.st?.[0] || defaultStartTime; // start from 2015
    const endTime = query?.filters?.st?.[1] || new Date().getTime(); // current time
    const ellipsedTextClass = "ellipsedTextClass";
    const [users] = useResourceSnapshot(UsersForJobsResource);
    const [queues] = useResourceSnapshot(QueuesResource);
    const [usersSearch, setUsersSearch] = useState("");
    const dragToRef = useRef(-1);

    // Update filters by pushing to URL
    const updateQuery = useCallback(
      (query: JobsQueryParams) => {
        router.replace({
          ...location,
          pathname: location.pathname,
          query: formatQueryState(query),
        });
      },
      [location, router]
    );

    useEffect(() => {
      UsersForJobsResource.fetch({
        filter: usersSearch,
      });
      additionalJobsControls?.().fetchQueue?.() && QueuesResource.fetch();
    }, [usersSearch]);

    const debounceUpdateQuery = debounce((text) => {
      if (query) {
        if (text) {
          updateQuery({
            ...query,
            filters: { ...query.filters, contains: [text] },
          });
        } else {
          delete query.filters.contains;
          updateQuery(query);
        }
      }
    }, 300);

    const handleEnterText = (text: string) => {
      debounceUpdateQuery(text);
    };

    const handleUpdateStartTime = (
      type: string,
      rangeObj: Immutable.List<any>
    ) => {
      const range = rangeObj && rangeObj.toJS && rangeObj.toJS();
      const fromDate = range && range[0];
      const toDate = range && range[1];
      const fromDateTimestamp = fromDate && fromDate.toDate().getTime();
      const toDateTimestamp = toDate && toDate.toDate().getTime();

      if (query) {
        if (type === IntervalTypes.ALL_TIME_INTERVAL) {
          // if we are showing all time, clear out the time filter
          delete query.filters[GenericFilters.st];
          updateQuery(query);
        } else {
          updateQuery({
            ...query,
            filters: {
              ...query.filters,
              [GenericFilters.st]: [fromDateTimestamp, toDateTimestamp],
            },
          });
        }
      }
    };

    const addValueToFilter = (type: string, value: string) => {
      const values = query?.filters?.[type] || [];
      if (!values.includes(value) && query) {
        values.push(value);
        updateQuery({
          ...query,
          filters: { ...query.filters, [type]: values },
        } as JobsQueryParams);
      }
    };

    const removeValueFromFilter = (type: string, value: string) => {
      const values = query?.filters?.[type] || [];
      const index = values.indexOf(value);
      if (index !== -1 && query) {
        if (values.length > 1) {
          values.splice(index, 1);
          updateQuery({
            ...query,
            filters: { ...query.filters, [type]: values },
          } as JobsQueryParams);
        } else {
          delete query.filters[type];
          updateQuery(query);
        }
      }
    };

    const handleDragMove = (_: number, hoverIndex: number) => {
      dragToRef.current = hoverIndex;
    };

    const handleDragEnd = (itemIdx: number) => {
      const toIdx = dragToRef.current;
      if (itemIdx === toIdx || toIdx === -1) return;
      const cols = cloneDeep(manageColumns);

      const range =
        toIdx > itemIdx
          ? { start: itemIdx, stop: toIdx }
          : { start: toIdx, stop: itemIdx };
      for (let i = range.start; i <= range.stop; i++) {
        if (i === range.start && toIdx > itemIdx) {
          cols[i].sort = toIdx;
        } else if (i === range.stop && itemIdx > toIdx) {
          cols[i].sort = toIdx;
        } else {
          cols[i].sort = cols[i].sort + (itemIdx > toIdx ? 1 : -1);
        }
      }
      setManageColumns(cols.sort((a, b) => (a.sort > b.sort ? 1 : -1)));
    };

    const handleSelectColumn = (item: string) => {
      const cols = cloneDeep(manageColumns);
      const col = cols.find((col) => col.id === item);
      col.selected = !col.selected;
      setManageColumns(cols);
    };

    const renderFilterMenu = (filterKey: keyof typeof GenericFilters) => {
      const items =
        filterKey === GenericFilters.usr
          ? users || []
          : filterKey === GenericFilters.jst
          ? itemsForStateFilter
          : filterKey === GenericFilters.qt
          ? itemsForQueryTypeFilter
          : queues || [];
      return (
        <FilterSelectMenu
          iconStyle={styles.arrow}
          popoverFilters="popoverFilters"
          ellipsedTextClass={ellipsedTextClass}
          selectedToTop={false}
          noSearch={filterKey !== GenericFilters.usr}
          onItemSelect={(id: string) => addValueToFilter(filterKey, id)}
          onItemUnselect={(id: string) => removeValueFromFilter(filterKey, id)}
          selectedValues={Immutable.fromJS(query?.filters?.[filterKey] || [])}
          items={items}
          label={intl.formatMessage({ id: FILTER_LABEL_IDS[filterKey] })}
          menuHeader={intl.formatMessage({ id: FILTER_LABEL_IDS[filterKey] })}
          name={filterKey}
          checkBoxClass="jobsFilters__checkBox"
          isJobStatus={filterKey === GenericFilters.jst}
          {...(filterKey === GenericFilters.usr && {
            cellCache: cellCache,
            loadItemsForFilter: (value: string) => setUsersSearch(value),
            wrapWithCellMeasurer: true,
            searchPlaceholder: "Search users",
          })}
        />
      );
    };

    return (
      <div className={classes["jobs-filters"]}>
        <div className={classes["jobs-filters__left"]}>
          <ContainsText
            className="containsTextClass"
            searchIconClass="containsTextClass__searchIconClass"
            defaultValue={query?.filters?.contains?.[0] || ""}
            id="containsText"
            onEnterText={handleEnterText}
          />
          <StartTimeSelect
            iconStyle={styles.arrow}
            popoverFilters="popoverFilters startTimeFilterPopover"
            selectedToTop={false}
            onChange={handleUpdateStartTime}
            id="startTimeFilter"
            defaultType={IntervalTypes.ALL_TIME_INTERVAL}
            startTime={startTime}
            endTime={endTime}
          />
          {renderFilterMenu(GenericFilters.jst)}
          {renderFilterMenu(GenericFilters.qt)}
          {renderFilterMenu(GenericFilters.usr)}
          {additionalJobsControls?.().renderFilterMenu?.() &&
            renderFilterMenu(GenericFilters.qn)}
        </div>
        <FilterSelectMenu
          selectedToTop={false}
          noSearch
          selectedValues={Immutable.fromJS(
            transformToSelectedItems(manageColumns)
          )}
          onItemSelect={handleSelectColumn}
          onItemUnselect={handleSelectColumn}
          items={transformToItems(manageColumns)}
          label={intl.formatMessage({ id: "Common.Manage.Columns" })}
          name="col"
          showSelectedLabel={false}
          iconId="interface/manage-column"
          hasSpecialIcon
          iconClass="showHideColumn__settingIcon"
          checkBoxClass="showHideColumn__checkBox"
          selectClass="showHideColumn"
          popoverFilters="margin-top--half"
          onDragMove={handleDragMove}
          onDragEnd={handleDragEnd}
          isDraggable
          hasIconFirst
          selectType="button"
        />
      </div>
    );
  }
);

const styles = {
  arrow: {
    color: "#505862",
    fontSize: 12,
    height: 24,
    width: 24,
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
  },
};

export default JobsFilters;
