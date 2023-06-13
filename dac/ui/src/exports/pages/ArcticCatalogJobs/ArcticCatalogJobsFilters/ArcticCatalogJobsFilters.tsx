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

import React, { useEffect } from "react";
import Immutable from "immutable";
import { useDispatch, useSelector } from "react-redux";
import { intl } from "@app/utils/intl";

// import StartTimeSelect from "@app/pages/JobPage/components/JobsFilters/StartTimeSelect/StartTimeSelect";
// import * as IntervalTypes from "@app/pages/JobPage/components/JobsFilters/StartTimeSelect/IntervalTypes";
import { ArcticCatalogJobsQueryParams } from "../ArcticCatalogJobs";
import FilterSelectMenu from "@app/components/Fields/FilterSelectMenu";
import { CellMeasurerCache } from "react-virtualized";
import { loadItemsForFilter } from "@app/actions/joblist/jobList";
import { getDataWithItemsForFilters } from "selectors/jobs";
import ContainsText from "@app/pages/JobPage/components/JobsFilters/ContainsText";
import { debounce } from "lodash";

import * as classes from "../ArcticCatalogJobs.module.less";

const stateFilterItems = [
  // todo: `la` loc not building correctly here
  { id: "SETUP", label: "Setup", icon: "job-state/setup" },
  { id: "QUEUED", label: "Queued", icon: "job-state/queued" },
  { id: "STARTING", label: "Starting", icon: "job-state/starting" },
  { id: "RUNNING", label: "Running", icon: "job-state/running" },
  { id: "COMPLETED", label: "Completed", icon: "job-state/completed" },
  { id: "CANCELLED", label: "Cancelled", icon: "job-state/canceled" },
  { id: "FAILED", label: "Failed", icon: "job-state/failed" },
];

const typeFilterItems = [
  { id: "OPTIMIZE", label: "Optimize", default: false },
  { id: "VACUUM", label: "Vacuum", default: false },
];

const cellCache = new CellMeasurerCache({
  fixedWidth: true,
  defaultHeight: 32,
});

type ArcticCatalogJobsFiltersProps = {
  curQuery: ArcticCatalogJobsQueryParams;
  updateQuery: (query: ArcticCatalogJobsQueryParams) => void;
  // targetFilterItems: Array<{
  //   label: string;
  //   id: string;
  //   icon?: React.ReactElement;
  // }>;
};

// DX-61156: use input filter to search on table/reference
// LATER: listJobs endpoint doesn't support startTime as a filter
const ArcticCatalogJobsFilters = ({
  curQuery,
  updateQuery,
}: // targetFilterItems = [],
ArcticCatalogJobsFiltersProps) => {
  const dispatch = useDispatch();

  useEffect(() => {
    // @ts-ignore
    dispatch(loadItemsForFilter("users"));
  }, [dispatch]);

  const users = useSelector((state: any) =>
    getDataWithItemsForFilters(state).get("users")
  );
  // const startTime =
  //   curQuery?.filters?.startTime?.[0] ?? new Date(2015, 0).getTime(); // start from 2015
  // const endTime = curQuery?.filters?.startTime?.[1] ?? new Date().getTime(); // current time

  // const handleUpdateStartTime = (
  //   type: string,
  //   rangeObj: Immutable.List<any>
  // ) => {
  //   const range = rangeObj && rangeObj.toJS && rangeObj.toJS();
  //   const fromDate = range && range[0];
  //   const toDate = range && range[1];
  //   const fromDateTimestamp = fromDate && fromDate.toDate().getTime();
  //   const toDateTimestamp = toDate && toDate.toDate().getTime();

  //   if (type === IntervalTypes.ALL_TIME_INTERVAL) {
  //     // if we are showing all time, clear out the time filter
  //     updateQuery({
  //       ...curQuery,
  //       filters: { ...curQuery.filters, startTime: [] },
  //     });
  //   } else {
  //     updateQuery({
  //       ...curQuery,
  //       filters: {
  //         ...curQuery.filters,
  //         startTime: [fromDateTimestamp, toDateTimestamp],
  //       },
  //     });
  //   }
  // };

  const addValueToFilter = (type: string, value: string) => {
    const values = curQuery?.filters?.[type] ?? [];
    if (!values.includes(value)) {
      values.push(value);
      updateQuery({
        ...curQuery,
        filters: { ...curQuery.filters, [type]: values },
      });
    }
  };

  const removeValueFromFilter = (type: string, value: string) => {
    const values = curQuery?.filters?.[type] ?? [];
    const index = values.indexOf(value);
    if (index !== -1) {
      if (values.length > 1) {
        values.splice(index, 1);
        updateQuery({
          ...curQuery,
          filters: { ...curQuery.filters, [type]: values },
        });
      } else {
        delete curQuery.filters[type];
        updateQuery(curQuery);
      }
    }
  };

  const debounceUpdateQuery = debounce((text) => {
    if (text) {
      updateQuery({
        ...curQuery,
        filters: { ...curQuery.filters, contains: [text] },
      });
    } else {
      delete curQuery?.filters?.contains;
      updateQuery(curQuery);
    }
  }, 300);

  const handleEnterText = (text: string) => {
    debounceUpdateQuery(text);
  };

  return (
    <div className={classes["arctic-catalog-jobs__filters"]}>
      <ContainsText
        className="containsTextClass"
        searchIconClass="containsTextClass__searchIconClass"
        defaultValue={curQuery?.filters?.contains?.[0]}
        id="containsText"
        placeholderId="ArcticCatalog.Jobs.FilterPlaceholder"
        onEnterText={handleEnterText}
      />
      {/* <FilterSelectMenu
        iconStyle={styles.arrow}
        popoverFilters="popoverFilters"
        selectedToTop={false}
        noSearch
        onItemSelect={(id: string) => addValueToFilter("target", id)}
        onItemUnselect={(id: string) => removeValueFromFilter("target", id)}
        selectedValues={Immutable.fromJS(curQuery.filters?.target)}
        items={targetFilterItems}
        label={intl.formatMessage({ id: "Common.Target" })}
        menuHeader={intl.formatMessage({ id: "Common.Target" })}
        name="target"
        checkBoxClass="jobsFilters__checkBox"
        isJobStatus
      /> */}
      {/* <StartTimeSelect
        iconStyle={styles.arrow}
        popoverFilters="popoverFilters startTimeFilterPopover"
        selectedToTop={false}
        onChange={handleUpdateStartTime}
        id="startTimeFilter"
        defaultType={IntervalTypes.ALL_TIME_INTERVAL}
        startTime={startTime}
        endTime={endTime}
      /> */}
      <FilterSelectMenu
        iconStyle={styles.arrow}
        popoverFilters="popoverFilters"
        selectedToTop={false}
        noSearch
        onItemSelect={(id: string) => addValueToFilter("state", id)}
        onItemUnselect={(id: string) => removeValueFromFilter("state", id)}
        selectedValues={Immutable.fromJS(curQuery.filters?.state)}
        items={stateFilterItems}
        label={intl.formatMessage({ id: "Common.Status" })}
        menuHeader={intl.formatMessage({ id: "Common.Status" })}
        name="state"
        checkBoxClass="jobsFilters__checkBox"
        isJobStatus={true}
      />
      <FilterSelectMenu
        iconStyle={styles.arrow}
        popoverFilters="popoverFilters"
        selectedToTop={false}
        noSearch
        onItemSelect={(id: string) => addValueToFilter("type", id)}
        onItemUnselect={(id: string) => removeValueFromFilter("type", id)}
        selectedValues={Immutable.fromJS(curQuery.filters?.type)}
        items={typeFilterItems}
        label={intl.formatMessage({ id: "Common.Type" })}
        menuHeader={intl.formatMessage({ id: "Common.Type" })}
        name="type"
        checkBoxClass="jobsFilters__checkBox margin-right"
      />
      <FilterSelectMenu
        iconStyle={styles.arrow}
        popoverFilters="popoverFilters"
        selectedToTop
        searchPlaceholder="Search users"
        onItemSelect={(id: string) => addValueToFilter("usr", id)}
        onItemUnselect={(id: string) => removeValueFromFilter("usr", id)}
        selectedValues={Immutable.fromJS(curQuery.filters?.usr)}
        items={users ?? []}
        loadItemsForFilter={(value: string, limit: string) =>
          // @ts-ignore
          dispatch(loadItemsForFilter("users", value, limit))
        }
        label={intl.formatMessage({ id: "Common.User" })}
        menuHeader={intl.formatMessage({ id: "Common.User" })}
        name="usr"
        checkBoxClass="jobsFilters__checkBox margin-right"
        cellCache={cellCache}
        wrapWithCellMeasurer
      />
    </div>
  );
};

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

export default ArcticCatalogJobsFilters;
