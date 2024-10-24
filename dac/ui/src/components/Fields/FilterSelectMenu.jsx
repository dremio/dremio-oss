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
import { Fragment, Component, useState } from "react";
import clsx from "clsx";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { injectIntl } from "react-intl";
import { Tooltip } from "dremio-ui-lib";
import { AutoSizer, List, CellMeasurer } from "react-virtualized";

import EllipsedText from "components/EllipsedText";
import { memoOne } from "utils/memoUtils";

import DragTarget from "components/DragComponents/DragTarget";
import DragSource from "components/DragComponents/DragSource";

import Checkbox from "components/Fields/Checkbox";
import { SearchField } from "components/Fields";
import { SelectView } from "./SelectView";
import { getIconPath } from "#oss/utils/getIconPath";

import "./FilterSelectMenu.less";

const FILTER_ITEM_HEIGHT = 32;
const VIRTUAL_LIST_MAX = 10;

// Limit height of dropdown
const getPopoverHeight = memoOne((numItems, hasSearch, filterItemHeight) => {
  const num = numItems;
  const { paddingBottom, paddingTop } = styles.popoverContent;
  const searchHeight = hasSearch ? styles.searchInput.height + 16 : 0;

  const itemsHeight = Math.min(num, VIRTUAL_LIST_MAX) * filterItemHeight;
  return paddingBottom + paddingTop + searchHeight + itemsHeight;
});

FilterSelectMenuItem.propTypes = {
  item: PropTypes.object.isRequired,
  onChange: PropTypes.func.isRequired,
  checked: PropTypes.bool,
  name: PropTypes.string,
  itemIndex: PropTypes.number,
  onClick: PropTypes.func,
  className: PropTypes.string,
  checkBoxClass: PropTypes.string,
  showCheckIcon: PropTypes.bool,
  disabled: PropTypes.bool,
  ellipsedTextClass: PropTypes.string,

  isDraggable: PropTypes.bool,
  onDragMove: PropTypes.func,
  onDragEnd: PropTypes.func,
  onDragStart: PropTypes.func,
};

export const SUBHEADER_ITEM_TYPE = "SUBHEADER_ITEM_TYPE";

export function FilterSelectMenuItem({
  item,
  itemIndex,
  onChange,
  checked,
  className,
  checkBoxClass,
  disabled,

  //drag & drop properties
  isDraggable = false,
  onDragStart,
  onDragEnd,
  onDragMove,
}) {
  const [draggingIndex, settingDragIndex] = useState(-1);

  function handleDragStart(dragConfig) {
    settingDragIndex(dragConfig.index);
    onDragStart && onDragStart(dragConfig);
  }
  const handleDragMove = (dragIndex, hoverIndex) => {
    onDragMove && onDragMove(dragIndex, hoverIndex);
  };

  function handleDragEnd(itemIndexId) {
    settingDragIndex(-1);
    onDragEnd && onDragEnd(itemIndexId);
  }
  const menuClass = clsx("filterSelectMenu", className, {
    "--transparent": itemIndex === draggingIndex,
  });

  const menuItemCls = clsx("filterSelectMenu__checkbox", {
    "gutter--none": isDraggable,
  });

  function renderSubheaderItem() {
    return (
      <span
        key={item.id}
        className="gutter-top gutter-bottom gutter-left--double gutter-right--double"
      >
        {item.label}
      </span>
    );
  }

  function renderContent() {
    if (item.type === SUBHEADER_ITEM_TYPE) {
      return renderSubheaderItem();
    }

    return (
      <span className={menuItemCls} key={item.id}>
        <Checkbox
          onChange={() => onChange(checked, item.id)}
          label={[
            item.icon && (
              <dremio-icon
                name={item.icon}
                style={{
                  ...styles.checkboxLabelContainer,
                  ...(item?.iconStyles ?? {}),
                }}
              />
            ),
            item.iconId && (
              <img
                src={getIconPath(item.iconId)}
                data-qa={item.iconId}
                alt={item.label}
                style={{ height: 24, width: 24, marginRight: "4px" }}
              />
            ),
            item.label.length > 24 || item.tooltip ? (
              <Tooltip title={item.tooltip ?? item.label}>
                <EllipsedText text={item.label} className="ellipseLabel" />
              </Tooltip>
            ) : (
              item.label
            ),
          ]}
          key={`CheckBox-${item.id}`}
          checked={checked}
          dataQa={getDataQaForFilterItem(item.id)}
          checkBoxClass={checkBoxClass}
          disabled={disabled}
          style={isDraggable ? styles.draggableContainer : {}}
          inputStyles={{
            position: "unset",
            left: "unset",
            display: "none",
          }}
          tooltipEnabledLabelProps={{
            tabIndex: 0,
            onKeyDown: (e) => {
              if (e.code === "Enter" || e.code === "Space") {
                onChange(checked, item.id);
              }
            },
          }}
        />
      </span>
    );
  }

  function draggableComponent() {
    return (
      <DragTarget
        dragType="sortColumns"
        moveColumn={(dragIndex, currentHoverIndex) =>
          handleDragMove(dragIndex, currentHoverIndex)
        }
        index={itemIndex}
        itemIndex={itemIndex}
        key={item.label}
        className="filterSelectMenu__dragTargetCls"
        dragTargetHoverCls="filterSelectMenu__dragTargetHoverCls"
      >
        <DragSource
          dragType="sortColumns"
          className="filterSelectMenu__dragSourceCls"
          index={itemIndex}
          onDragStart={handleDragStart}
          onDragEnd={() => handleDragEnd(itemIndex)}
          id={item.label}
          key={item.label}
          preventDrag={isDraggable ? undefined : true}
          dragStyles={{
            opacity: 0,
          }}
        >
          <div className="flex --alignCenter margin-right">
            <dremio-icon
              name="interface/drag-handle"
              alt="Drag handle"
              class="filterSelectMenu__dragHandle"
            />
            {renderContent()}
          </div>
        </DragSource>
      </DragTarget>
    );
  }
  return (
    <div className={menuClass}>
      {isDraggable ? draggableComponent() : renderContent()}
    </div>
  );
}

@injectIntl
export default class FilterSelectMenu extends Component {
  static propTypes = {
    wrapWithCellMeasurer: PropTypes.bool,
    cellCache: PropTypes.object,
    items: PropTypes.array, // [{label: string, id: string, icon: component}]
    hideOnSelect: PropTypes.bool,
    selectedValues: PropTypes.instanceOf(Immutable.List),
    selectClass: PropTypes.string,
    name: PropTypes.string,
    label: PropTypes.string,
    searchPlaceholder: PropTypes.string,
    className: PropTypes.string,
    checkBoxClass: PropTypes.string,
    iconClass: PropTypes.string,
    showCheckIcon: PropTypes.bool,
    filterItemHeight: PropTypes.number,
    listClass: PropTypes.string,
    popoverContentClass: PropTypes.string,
    popoverContentHeight: PropTypes.number,

    alwaysShowLabel: PropTypes.bool,
    preventSelectedLabel: PropTypes.bool,
    noSearch: PropTypes.bool,
    selectedToTop: PropTypes.bool,
    selectType: PropTypes.string,

    // callbacks
    loadItemsForFilter: PropTypes.func,
    onItemSelect: PropTypes.func,
    onItemUnselect: PropTypes.func,

    intl: PropTypes.object.isRequired,
    showSelectedLabel: PropTypes.bool,
    iconId: PropTypes.string,
    iconStyle: PropTypes.object,
    onClick: PropTypes.func,
    popoverFilters: PropTypes.string,
    selectViewBeforeOpen: PropTypes.func,
    selectViewBeforeClose: PropTypes.func,
    setBackGroundColorForLabel: PropTypes.bool,
    ellipsedTextClass: PropTypes.string,
    menuHeader: PropTypes.string,
    disableReorderOnSelect: PropTypes.bool,
    hasIconFirst: PropTypes.bool,
    iconTooltip: PropTypes.string,

    //drag and drop properties;
    isDraggable: PropTypes.bool,
    onDragMove: PropTypes.func,
    onDragEnd: PropTypes.func,
    onDragStart: PropTypes.func,
  };

  static defaultProps = {
    // todo: `la` loc not building correctly here
    items: [],
    selectedValues: Immutable.List(),
    searchPlaceholder: "Search",
    showSelectedLabel: true,
    wrapWithCellMeasurer: false,
  };

  state = {
    pattern: "",
    itemsList: this.props.items || [],
  };

  componentDidUpdate(prevProps) {
    const { items: prevItems } = prevProps;
    const { items: newItems } = this.props;
    const shouldUpdateState = !newItems.every(({ id: newId }) =>
      prevItems.find(({ id: prevId }) => newId && prevId && prevId === newId),
    );
    if (shouldUpdateState) {
      this.setState({ itemsList: newItems });
    }
  }

  getSelectedItems = memoOne((items, selectedValues) => {
    const selectedMap = this.getSelectedMap(items, selectedValues);
    return items.filter((item) => !!selectedMap[item.id]);
  });

  getUnselectedItems = memoOne((items, selectedValues, pattern) => {
    const selectedMap = this.getSelectedMap(items, selectedValues);
    return items
      .filter(
        (item) =>
          !selectedMap[item.id] &&
          (!pattern ||
            item.label.toLowerCase().includes(pattern.trim().toLowerCase())),
      )
      .sort((a, b) => a.label.localeCompare(b.label));
  });

  getSelectedMap = memoOne((items, selectedValues) => {
    return items.reduce((prev, item) => {
      prev[item.id] = selectedValues.includes(item.id);
      return prev;
    }, {});
  });

  getOrderedItems = memoOne((items, selectedValues, pattern) => {
    return pattern
      ? items.filter((item) =>
          item.label.toLowerCase().includes(pattern.trim().toLowerCase()),
        )
      : items;
  });

  handleSearchForItem = (value) => {
    const { name, loadItemsForFilter } = this.props;
    this.setState({ pattern: value });
    if (name === "usr") {
      loadItemsForFilter(value, "1000");
    }
  };

  handleItemChange = (checked, id) => {
    if (checked) {
      if (this.props.onItemUnselect) {
        this.props.onItemUnselect(id);
      }
    } else if (this.props.onItemSelect) {
      this.props.onItemSelect(id);
    }
    if (this.props.hideOnSelect) {
      this.handleRequestClose();
    }
  };

  beforeDDClose = () => {
    const { selectViewBeforeClose } = this.props;
    if (selectViewBeforeClose && typeof selectViewBeforeClose === "function") {
      selectViewBeforeClose();
    }
    this.setState({ pattern: "" });
  };
  beforeDDOpen = () => {
    const { selectViewBeforeOpen, selectedValues } = this.props;
    const { itemsList } = this.state;
    if (selectViewBeforeOpen && typeof selectViewBeforeOpen === "function") {
      selectViewBeforeOpen();
    }
    if (!this.props.disableReorderOnSelect) {
      this.setState({
        itemsList: [
          ...this.getSelectedItems(itemsList, selectedValues),
          ...this.getUnselectedItems(itemsList, selectedValues, ""),
        ],
      });
    }
  };

  renderItems = ({ index, key, style, parent }) => {
    const {
      items,
      selectedValues,
      name,
      className,
      checkBoxClass,
      onClick,
      cellCache,
      wrapWithCellMeasurer,
      ellipsedTextClass,
      onDragMove,
      onDragStart,
      onDragEnd,
      isDraggable = false,
      selectType,
    } = this.props;

    const { pattern, itemsList } = this.state;

    if (!items.length && pattern) return null;

    const orderedItems =
      name === "col"
        ? items
        : this.getOrderedItems(itemsList, selectedValues, pattern);
    const selectedMap = this.getSelectedMap(itemsList, selectedValues);

    const item = orderedItems[index];
    const isSubHeaderItem = items[index]?.type === SUBHEADER_ITEM_TYPE;

    const itemClassName = clsx(
      className,
      isSubHeaderItem
        ? "filterSelectMenu__menuSubHeader"
        : "filterSelectMenu__menuItem",
      { "--disabled": item.disabled && selectType !== "button" },
      { "--chosen": selectedMap[item.id] && selectType !== "button" },
    );
    const itemStyle = isSubHeaderItem ? { ...style, height: 32 } : style;

    const renderItemCell = () => (
      <div style={itemStyle} key={key}>
        <FilterSelectMenuItem
          key={index}
          item={item}
          onChange={this.handleItemChange}
          checked={selectedMap[item.id]}
          name={name}
          itemIndex={index}
          onClick={onClick}
          className={itemClassName}
          checkBoxClass={checkBoxClass}
          disabled={item.disabled}
          ellipsedTextClass={ellipsedTextClass}
          isDraggable={item.isDraggable ?? isDraggable}
          onDragMove={onDragMove}
          onDragEnd={onDragEnd}
          onDragStart={onDragStart}
        />
      </div>
    );

    if (wrapWithCellMeasurer) {
      return (
        <CellMeasurer
          key={key}
          cache={cellCache}
          parent={parent}
          rowIndex={index}
          columnIndex={0}
        >
          {renderItemCell()}
        </CellMeasurer>
      );
    }
    return renderItemCell();
  };

  renderSelectedLabel() {
    // todo: better loc
    const {
      alwaysShowLabel,
      ellipsedTextClass,
      intl: { formatMessage },
      preventSelectedLabel,
      selectedValues,
    } = this.props;

    const { itemsList } = this.state;

    const getSelectedItemsLabel = (allItems, values) => {
      const selectedItems = this.getSelectedItems(allItems, values);
      if (selectedItems && selectedItems.length === 0) {
        return this.props.label;
      }

      const { label, label: { props: { label: propsLabel } = {} } = {} } =
        selectedItems[0] || {};
      const selectedItemsLable =
        propsLabel && typeof propsLabel === "string"
          ? formatMessage({ id: `${propsLabel}.name` })
          : label;

      return selectedItemsLable;
    };

    const labelText = selectedValues.size
      ? (alwaysShowLabel ? ": " : "") +
        getSelectedItemsLabel(itemsList, selectedValues)
      : "";
    const additionalItemCount =
      this.getSelectedItems(itemsList, selectedValues).length - 1;

    return (
      !preventSelectedLabel && (
        <div
          className={clsx(
            ellipsedTextClass,
            "filter-select-label",
            "filterSelectMenu__label__text",
            { "--filtered": selectedValues.size },
          )}
        >
          <EllipsedText
            text={labelText}
            style={
              additionalItemCount >= 1
                ? styles.infoLabelShort
                : styles.infoLabel
            }
          />
          {additionalItemCount >= 1 && (
            <span>{`, +${additionalItemCount}`}</span>
          )}
        </div>
      )
    );
  }

  getLabelIcon() {
    const { iconId, selectedValues } = this.props;
    if (iconId) {
      return iconId;
    }
    return selectedValues.size ? "ArrowDownBlue.svg" : "ArrowDown.svg";
  }

  getSelectViewContent() {
    const {
      selectType,
      preventSelectedLabel,
      selectedValues,
      alwaysShowLabel,
      label,
      showSelectedLabel,
    } = this.props;
    if (selectType === "button") {
      return (
        <span title={label} className="pl-05 pr-1">
          {label}
        </span>
      );
    } else {
      return (
        <Fragment>
          {(preventSelectedLabel ||
            !selectedValues.size ||
            alwaysShowLabel) && <span>{label}</span>}
          {showSelectedLabel ? this.renderSelectedLabel() : label}
        </Fragment>
      );
    }
  }

  render() {
    const {
      name,
      iconStyle,
      popoverFilters,
      selectClass,
      iconClass,
      selectedValues,
      noSearch,
      searchPlaceholder,
      filterItemHeight,
      listClass,
      popoverContentClass,
      popoverContentHeight,
      cellCache,
      items,
      iconId,
      menuHeader,
      hasIconFirst,
      iconTooltip,
    } = this.props;

    const { pattern, itemsList } = this.state;

    const getRowHeight = ({ index }) => {
      if (this.props.items?.[index]?.type === SUBHEADER_ITEM_TYPE) {
        return 32;
      } else if (typeof cellCache?.rowHeight === "function") {
        return cellCache.rowHeight({ index });
      } else {
        return filterItemHeight || FILTER_ITEM_HEIGHT;
      }
    };

    const orderedItems =
      name === "col"
        ? items
        : this.getOrderedItems(itemsList, selectedValues, pattern);
    const selectedMap = this.getSelectedMap(itemsList, selectedValues);

    const className = clsx(
      "filter-select-menu field filterSelectMenu__labelWrapper",
      { "--filtered": selectedValues.size },
      selectClass,
    );
    const iconClassName = clsx(iconClass, "filterSelectMenu__downIcon");
    const hasSearch = !noSearch || !!pattern;
    const pHeight =
      popoverContentHeight ||
      getPopoverHeight(
        orderedItems.length,
        hasSearch,
        filterItemHeight || FILTER_ITEM_HEIGHT,
      );

    return (
      <SelectView
        content={this.getSelectViewContent()}
        beforeClose={this.beforeDDClose}
        beforeOpen={this.beforeDDOpen}
        className={className}
        dataQa={name + "-filter"}
        iconId={this.getLabelIcon()}
        iconStyle={iconStyle}
        popoverFilters={popoverFilters}
        iconClass={iconClassName}
        hasIconFirst={hasIconFirst}
        menuHeader={menuHeader}
        hasSpecialIcon={!!iconId}
        iconTooltip={iconTooltip}
      >
        <div
          style={{ ...styles.popoverContent, height: pHeight }}
          className={popoverContentClass}
        >
          {hasSearch && (
            <SearchField
              style={styles.searchStyle}
              inputStyle={styles.searchInput}
              placeholder={searchPlaceholder}
              value={pattern}
              onChange={this.handleSearchForItem}
            />
          )}
          <div style={styles.virtualContainer}>
            <AutoSizer>
              {({ width, height }) => (
                <List
                  selectedMap={selectedMap} //Rerender when selection changes
                  ref={(ref) => (this.virtualList = ref)}
                  rowCount={orderedItems.length}
                  rowHeight={getRowHeight}
                  rowRenderer={this.renderItems}
                  width={width}
                  height={height}
                  className={listClass}
                />
              )}
            </AutoSizer>
          </div>
        </div>
      </SelectView>
    );
  }
}

/**
 * Generates data-qa attribute for filters.
 *
 * E2e test are build in assumption that FilterSelectMenu and {@see JobsFilters} have the same shape
 * of data-qa attributes. This method is created to explicitly define that relation
 * @param {string|number} id - filter id
 */
export const getDataQaForFilterItem = (id) => id + "-filter-item";

const styles = {
  base: {
    display: "flex",
    alignItems: "center",
    cursor: "pointer",
  },
  popoverContent: {
    display: "flex",
    flexWrap: "wrap",
    flexDirection: "column",
    minWidth: 240,
    paddingTop: 4,
    paddingBottom: 4,
  },
  virtualContainer: {
    flex: 1,
  },
  arrow: {
    Container: {
      position: "relative",
      top: 1,
    },
  },
  infoLabel: {
    maxWidth: 120,
  },
  infoLabelShort: {
    maxWidth: 90,
  },
  searchInput: {
    height: FILTER_ITEM_HEIGHT,
    padding: "4px 10px",
    margin: "8px 16px",
  },
  checkboxLabelContainer: {
    overflow: "hidden",
    height: 24,
    width: 24,
  },
  draggableContainer: {
    color: "var(--text--faded)",
    opacity: 1,
  },
};
