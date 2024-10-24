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
import { cloneElement, Component } from "react";
import Immutable from "immutable";
import PropTypes from "prop-types";
import { injectIntl } from "react-intl";
import { noop, debounce } from "lodash";
import { CellMeasurerCache } from "react-virtualized";
import FilterSelectMenu, {
  getDataQaForFilterItem,
} from "components/Fields/FilterSelectMenu";
import Select from "#oss/components/Fields/Select";
import JobsFiltersMixin, {
  getSortItems,
} from "dyn-load/pages/JobPage/components/JobsFilters/JobsFiltersMixin";
import ShowHideColumn from "../../../JobPageNew/components/ShowHideColumn/ShowHideColumn";
import ContainsText from "./ContainsText";
import * as IntervalTypes from "./StartTimeSelect/IntervalTypes";
import StartTimeSelect from "./StartTimeSelect/StartTimeSelect";
import { ddSort, ddSortList } from "./JobsFilters.less";
import "./JobsFilters.less";

const itemsForStateFilter = [
  // todo: `la` loc not building correctly here
  { id: "SETUP", label: "Setup", icon: "job-state/setup" },
  { id: "QUEUED", label: "Queued", icon: "job-state/queued" },
  { id: "ENGINE_START", label: "Engine Start", icon: "job-state/engine-start" },
  { id: "RUNNING", label: "Running", icon: "job-state/running" },
  { id: "COMPLETED", label: "Completed", icon: "job-state/completed" },
  { id: "CANCELED", label: "Canceled", icon: "job-state/canceled" },
  { id: "FAILED", label: "Failed", icon: "job-state/failed" },
];

const itemsForQueryTypeFilter = [
  // todo: `la` loc not building correctly here
  { id: "UI", label: "UI", default: true },
  { id: "EXTERNAL", label: "External Tools", default: true },
  { id: "ACCELERATION", label: "Accelerator", default: false },
  { id: "INTERNAL", label: "Internal", default: false },
  { id: "DOWNLOAD", label: "Downloads", default: false },
];

const cellCache = new CellMeasurerCache({
  fixedWidth: true,
  defaultHeight: 32,
});

const sortItems = getSortItems().map((item) => ({
  ...item,
  dataQa: getDataQaForFilterItem(item.id),
}));

@JobsFiltersMixin
export class JobsFilters extends Component {
  static contextTypes = {
    loggedInUser: PropTypes.object.isRequired,
  };

  static propTypes = {
    queryState: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object,
    dataFromUserFilter: PropTypes.any,
    dataWithItemsForFilters: PropTypes.object,
    loadItemsForFilter: PropTypes.func.isRequired,
    onUpdateQueryState: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired,
    columnFilterSelect: PropTypes.func,
    columnFilterUnSelect: PropTypes.func,
    updateColumnsState: PropTypes.func,
    checkedItems: PropTypes.instanceOf(Immutable.List),
    isQVJobs: PropTypes.bool,
  };

  static defaultProps = {
    queryState: Immutable.fromJS({ filters: {} }),
    dataWithItemsForFilters: Immutable.Map(),
  };

  constructor(props) {
    super(props);
    this.handleStartTimeChange = this.handleStartTimeChange.bind(this);
    this.addInfoToFilter = this.addInfoToFilter.bind(this);
    this.removeInfoFromFilter = this.removeInfoFromFilter.bind(this);
    this.toggleSortDirection = this.toggleSortDirection.bind(this);
    this.changeSortItem = this.changeSortItem.bind(this);
    this.state = {
      //may be used in a mixin
      queues: [],
    };
    this.handleEnterText = this.handleEnterText.bind(this);
  }

  UNSAFE_componentWillMount() {
    this.props.loadItemsForFilter("spaces");
    const { admin: isAdmin, permissions: { canViewAllJobs } = {} } =
      this.context.loggedInUser;
    if (isAdmin || canViewAllJobs) {
      this.props.loadItemsForFilter("users");
    }
    this.prepareQueuesFilter();
  }

  getAllFilters() {
    const {
      queryState,
      intl,
      isQVJobs,
      dataWithItemsForFilters,
      dataFromUserFilter,
    } = this.props;
    const { admin: isAdmin, permissions: { canViewAllJobs } = {} } =
      this.context.loggedInUser;
    const defaultStartTime = new Date(2015, 0).getTime();
    const startTime =
      queryState.getIn(["filters", "st", 0]) || defaultStartTime; // start from 2015
    const endTime =
      queryState.getIn(["filters", "st", 1]) || new Date().getTime(); // current time
    const selectedJst = queryState.getIn(["filters", "jst"]);
    const selectedQt = queryState.getIn(["filters", "qt"]);
    const selectedUsr = queryState.getIn(["filters", "usr"]);
    const userList = dataFromUserFilter
      ? dataFromUserFilter
      : dataWithItemsForFilters.get("users");

    const ellipsedTextClass = isQVJobs ? "ellipsedTextClass" : "";

    return [
      {
        value: "st",
        isVisible: true,
        node: (
          <StartTimeSelect
            iconStyle={styles.arrow}
            popoverFilters="popoverFilters startTimeFilterPopover"
            className={ellipsedTextClass}
            selectedToTop={false}
            onChange={this.handleStartTimeChange}
            id="startTimeFilter"
            defaultType={
              startTime === defaultStartTime // start from 2015
                ? IntervalTypes.ALL_TIME_INTERVAL
                : IntervalTypes.CUSTOM_INTERVAL
            }
            startTime={startTime}
            endTime={endTime}
          />
        ),
      },
      {
        value: "jst",
        isVisible: true,
        node: (
          <FilterSelectMenu
            iconStyle={styles.arrow}
            popoverFilters="popoverFilters"
            ellipsedTextClass={ellipsedTextClass}
            selectedToTop={false}
            noSearch
            onItemSelect={this.addInfoToFilter.bind(this, "jst")}
            onItemUnselect={this.removeInfoFromFilter.bind(this, "jst")}
            selectedValues={selectedJst}
            items={itemsForStateFilter}
            label={intl.formatMessage({ id: "Common.Status" })}
            menuHeader={intl.formatMessage({ id: "Common.Status" })}
            name="jst"
            checkBoxClass="jobsFilters__checkBox"
          />
        ),
      },
      {
        value: "qt",
        isVisible: true,
        node: (
          <FilterSelectMenu
            iconStyle={styles.arrow}
            popoverFilters="popoverFilters"
            ellipsedTextClass={ellipsedTextClass}
            selectedToTop={false}
            noSearch
            onItemSelect={this.addInfoToFilter.bind(this, "qt")}
            onItemUnselect={this.removeInfoFromFilter.bind(this, "qt")}
            selectedValues={selectedQt}
            items={itemsForQueryTypeFilter}
            label={intl.formatMessage({ id: "Common.Type" })}
            menuHeader={intl.formatMessage({ id: "Common.Type" })}
            name="qt"
            checkBoxClass="jobsFilters__checkBox margin-right"
          />
        ),
      },
      {
        value: "usr",
        isVisible: true,
        node: (
          <FilterSelectMenu
            iconStyle={styles.arrow}
            popoverFilters="popoverFilters"
            ellipsedTextClass={ellipsedTextClass}
            selectedToTop
            searchPlaceholder="Search users"
            onItemSelect={this.addInfoToFilter.bind(this, "usr")}
            onItemUnselect={this.removeInfoFromFilter.bind(this, "usr")}
            selectedValues={selectedUsr}
            items={userList}
            loadItemsForFilter={this.props.loadItemsForFilter.bind(
              this,
              "users",
            )}
            label={intl.formatMessage({ id: "Common.User" })}
            menuHeader={intl.formatMessage({ id: "Common.User" })}
            name="usr"
            checkBoxClass="jobsFilters__checkBox margin-right"
            cellCache={cellCache}
            wrapWithCellMeasurer
          />
        ),
      },
    ].filter(
      (filter) =>
        (filter.value !== "usr" && !isAdmin) || isAdmin || canViewAllJobs,
    );
  }

  handleStartTimeChange(type, rangeObj) {
    const { queryState } = this.props;
    const range = rangeObj && rangeObj.toJS && rangeObj.toJS();
    const fromDate = range && range[0];
    const toDate = range && range[1];
    const fromDateTimestamp = fromDate && fromDate.toDate().getTime();
    const toDateTimestamp = toDate && toDate.toDate().getTime();

    if (type === IntervalTypes.ALL_TIME_INTERVAL) {
      // if we are showing all time, clear out the time filter
      this.props.onUpdateQueryState(queryState.deleteIn(["filters", "st"]));
    } else {
      this.props.onUpdateQueryState(
        queryState.setIn(
          ["filters", "st"],
          [fromDateTimestamp, toDateTimestamp],
        ),
      );
    }
  }

  delayedHandleChange = debounce(
    (newState) => this.props.onUpdateQueryState(newState),
    300,
  );
  handleEnterText(text) {
    const { queryState, isQVJobs } = this.props;
    const newState = text
      ? queryState.setIn(["filters", "contains"], Immutable.List([text]))
      : queryState.deleteIn(["filters", "contains"]);
    isQVJobs
      ? this.delayedHandleChange(newState)
      : this.props.onUpdateQueryState(newState);
  }

  addInfoToFilter(type, value) {
    const { queryState } = this.props;
    const values = queryState.getIn(["filters", type]) || Immutable.List();
    if (!values.includes(value)) {
      this.props.onUpdateQueryState(
        queryState.setIn(["filters", type], values.push(value)),
      );
    }
  }

  removeInfoFromFilter(type, value) {
    const { queryState } = this.props;
    const values = queryState.getIn(["filters", type]) || Immutable.List();

    const index = values.indexOf(value);
    if (index !== -1) {
      const newState =
        values.size > 1
          ? queryState.setIn(["filters", type], values.remove(index))
          : queryState.deleteIn(["filters", type]);
      this.props.onUpdateQueryState(newState);
    }
  }

  toggleSortDirection() {
    const { queryState } = this.props;
    const oldDirection = queryState.get("order");
    const newDirection =
      oldDirection === "ASCENDING" ? "DESCENDING" : "ASCENDING";
    this.props.onUpdateQueryState(queryState.set("order", newDirection));
  }

  changeSortItem(id) {
    const { queryState } = this.props;
    const order = queryState.get("order");
    this.props.onUpdateQueryState(
      queryState.set("sort", id).set("order", order || "DESCENDING"),
    );
  }

  renderAllFilters(items, isCustom) {
    return items
      .filter((item) => item.isVisible)
      .map((filter) => (
        <div style={styles.filterBlock} key={filter.value}>
          <div>{cloneElement(filter.node, { isCustom })}</div>
        </div>
      ));
  }

  renderDefaultFilters() {
    return this.renderAllFilters(this.getAllFilters());
  }

  renderSortLabel() {
    const sortId = this.props.queryState.get("sort");
    const selectedItem = sortItems.find((item) => {
      return item.id === sortId;
    });
    const label = selectedItem ? selectedItem.label : "";
    return (
      <div
        data-qa="order-filter"
        style={{ ...styles.filterBlock, cursor: "pointer" }}
        onClick={this.toggleSortDirection}
      >
        <label style={{ cursor: "pointer" }}>
          {this.props.intl.formatMessage({ id: "Job.OrderBy" }, { label })}
          {this.renderSortDirectionIcon()}
        </label>
        <div style={styles.sortDivider} />
      </div>
    );
  }

  renderSortDirectionIcon() {
    const direction = this.props.queryState.get("order");
    if (direction) {
      const type =
        direction === "DESCENDING"
          ? "interface/caretDown"
          : "interface/caretUp";
      return <dremio-icon name={type}></dremio-icon>;
    }
  }

  renderFilters() {
    const {
      queryState,
      columnFilterSelect,
      columnFilterUnSelect,
      checkedItems,
      updateColumnsState,
      isQVJobs,
    } = this.props;

    const className = isQVJobs ? "containsTextClass" : "";
    const searchIconClass = isQVJobs
      ? "containsTextClass__searchIconClass"
      : "";

    return (
      <div style={styles.base}>
        <ContainsText
          className={className}
          searchIconClass={searchIconClass}
          defaultValue={queryState.getIn(["filters", "contains", 0])}
          id="containsText"
          onEnterText={this.handleEnterText}
        />
        {this.renderDefaultFilters()}
        {isQVJobs && (
          <div className="filters-header__showHideWrapper">
            <ShowHideColumn
              columnFilterSelect={columnFilterSelect}
              columnFilterUnSelect={columnFilterUnSelect}
              updateColumnsState={updateColumnsState}
              defaultValue={checkedItems}
            />
          </div>
        )}
        {!isQVJobs && (
          <div style={styles.order}>
            {this.renderSortLabel()}
            <Select
              dataQa="sort-filter"
              items={sortItems}
              valueField="id"
              selectedValueRenderer={noop}
              className={ddSort}
              listClass={ddSortList}
              value={queryState.get("sort")}
              onChange={this.changeSortItem}
            />
          </div>
        )}
      </div>
    );
  }

  render() {
    const { isQVJobs } = this.props;
    return (
      <div
        className={isQVJobs && "filters-header"}
        style={{ ...styles.base, ...styles.filtersHeader }}
      >
        {this.renderFilters()}
      </div>
    );
  }
}

const styles = {
  IconTheme: {
    Container: {
      float: "right",
      margin: "0",
    },
  },
  base: {
    width: "100%",
    display: "flex",
    alignItems: "center",
    paddingLeft: 10,
    gap: 20,
  },
  orderBy: {
    margin: "0 0 0 auto",
  },
  filtersHeader: {
    backgroundColor: "rgb(245, 252, 255)",
    paddingLeft: 0,
  },
  divider: {
    width: "25px",
  },
  sortDivider: {
    background: "rgba(0,0,0,0.10)",
    height: 16,
    width: 1,
    marginLeft: 10,
  },
  filterBlock: {
    display: "flex",
    alignItems: "center",
  },
  order: {
    display: "flex",
    justifyContent: "center",
    marginLeft: "auto",
  },
  gutterTop: {
    paddingTop: 6,
  },
  arrow: {
    color: "var(--icon--primary)",
    fontSize: "12px",
    height: "24px",
    width: "24px",
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
  },
};
export default injectIntl(JobsFilters);
