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
import { Fragment, Component } from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';
import Art from '@app/components/Art';
import EllipsedText from 'components/EllipsedText';
import { AutoSizer, List, CellMeasurer } from 'react-virtualized';
import { memoOne } from 'utils/memoUtils';

import FontIcon from 'components/Icon/FontIcon';
import Checkbox from 'components/Fields/Checkbox';
import { SearchField } from 'components/Fields';
import './FilterSelectMenu.less';
import { Tooltip } from 'dremio-ui-lib';
import { SelectView } from './SelectView';

const FILTER_ITEM_HEIGHT = 32;
const VIRTUAL_LIST_MAX = 10;

// Limit height of dropdown
const getPopoverHeight = memoOne((numItems, hasSearch, filterItemHeight) => {
  const num = numItems;
  const { paddingBottom, paddingTop } = styles.popoverContent;
  const searchHeight = hasSearch ? styles.searchInput.height : 0;

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
  ellipsedTextClass: PropTypes.string
};

export function FilterSelectMenuItem({
  item,
  itemIndex,
  onChange,
  checked,
  name,
  onClick,
  className,
  checkBoxClass,
  showCheckIcon,
  disabled,
  ellipsedTextClass
}) {
  const menuClass = clsx('filterSelectMenu', className);
  return (
    <div className={menuClass}>
      <span className='filterSelectMenu__checkbox' key={item.id}>
        <Checkbox
          onChange={() => onChange(checked, item.id)}
          label={[
            item.icon && (
              <FontIcon
                type={item.icon}
                key={'fi_' + item.id}
                theme={{ Container: styles.checkboxLabelContainer }}
              />
            ),
            item.label.length > 24 ? (
              <Tooltip title={item.label}>
                <EllipsedText text={item.label} className='ellipseLabel' />
              </Tooltip>
            ) : (
              item.label
            )
          ]}
          key={`CheckBox-${item.id}`}
          checked={checked}
          dataQa={getDataQaForFilterItem(item.id)}
          checkBoxClass={checkBoxClass}
          showCheckIcon={showCheckIcon}
          disabled={disabled}
        />
      </span>
      <span className='filterSelectMenu__vectorColumns'>
        {name === 'col' && (
          <>
            <Art
              src='UpVector.svg'
              alt='icon'
              title='icon'
              className='filterSelectMenu__upVector'
              onClick={() => onClick('up', itemIndex)}
              data-qa='UpVector'
            />
            <Art
              src='DownVector.svg'
              alt='icon'
              title='icon'
              className='filterSelectMenu__downVector'
              onClick={() => onClick('down', itemIndex)}
              data-qa='DownVector'
            />
          </>
        )}
      </span>
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
    isArtIcon: PropTypes.bool,

    // callbacks
    loadItemsForFilter: PropTypes.func,
    onItemSelect: PropTypes.func,
    onItemUnselect: PropTypes.func,

    intl: PropTypes.object.isRequired,
    showSelectedLabel: PropTypes.bool,
    icon: PropTypes.string,
    iconStyle: PropTypes.object,
    onClick: PropTypes.func,
    popoverFilters: PropTypes.string,
    selectViewBeforeOpen: PropTypes.func,
    selectViewBeforeClose: PropTypes.func,
    setBackGroundColorForLabel: PropTypes.bool,
    disableReorderOnSelect: PropTypes.bool,
    ellipsedTextClass: PropTypes.string
  };

  static defaultProps = {
    // todo: `la` loc not building correctly here
    items: [],
    selectedValues: Immutable.List(),
    searchPlaceholder: 'Search',
    showSelectedLabel: true,
    wrapWithCellMeasurer: false
  };

  state = {
    pattern: '',
    itemsList: this.props.items || []
  };

  componentDidUpdate(prevProps) {
    const { items: prevItems } = prevProps;
    const { items: newItems } = this.props;
    const shouldUpdateState = !newItems.every(({ id: newId }) =>
      prevItems.find(({ id: prevId }) => newId && prevId && prevId === newId)
    );
    if (shouldUpdateState) {
      this.setState({ itemsList: newItems }); // eslint-disable-line
    }
  }

  getSelectedItems = memoOne((items, selectedValues) => {
    const selectedMap = this.getSelectedMap(items, selectedValues);
    return items.filter((item) => !!selectedMap[item.id]);
  });

  getUnselectedItems = memoOne((items, selectedValues, pattern) => {
    const selectedMap = this.getSelectedMap(items, selectedValues);
    return items.filter(
      (item) =>
        !selectedMap[item.id] &&
        (!pattern ||
          item.label.toLowerCase().includes(pattern.trim().toLowerCase()))
    );
  });

  getSelectedMap = memoOne((items, selectedValues) => {
    return items.reduce((prev, item) => {
      prev[item.id] = selectedValues.includes(item.id);
      return prev;
    }, {});
  });

  getOrderedItems = memoOne((items, selectedValues, pattern) => {
    return pattern
      ? items.filter(
        (item) =>
          item.label.toLowerCase().includes(pattern.trim().toLowerCase())
      )
      : items;
  });

  handleSearchForItem = (value) => {
    const { name, loadItemsForFilter } = this.props;
    this.setState({ pattern: value });
    if (name === 'usr') {
      loadItemsForFilter(value, '1000');
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
    if (selectViewBeforeClose && typeof selectViewBeforeClose === 'function') {
      selectViewBeforeClose();
    }
    this.setState({ pattern: '' });
  };
  beforeDDOpen = () => {
    const { selectViewBeforeOpen, selectedValues } = this.props;
    const { itemsList } = this.state;
    if (selectViewBeforeOpen && typeof selectViewBeforeOpen === 'function') {
      selectViewBeforeOpen();
    }
    this.setState({
      itemsList: [
        ...this.getSelectedItems(itemsList, selectedValues),
        ...this.getUnselectedItems(itemsList, selectedValues, '')
      ]
    });
  };

  renderItems = ({ index, key, style, parent }) => {
    const {
      items,
      selectedValues,
      name,
      className,
      checkBoxClass,
      showCheckIcon,
      onClick,
      cellCache,
      wrapWithCellMeasurer,
      ellipsedTextClass
    } = this.props;

    if (!items.length) return null;

    const { pattern, itemsList } = this.state;

    const orderedItems = this.getOrderedItems(
      itemsList,
      selectedValues,
      pattern
    );
    const selectedMap = this.getSelectedMap(itemsList, selectedValues);

    const item = orderedItems[index];

    const renderItemCell = () => (
      <div style={style} key={key}>
        <FilterSelectMenuItem
          key={index}
          item={item}
          onChange={this.handleItemChange}
          checked={selectedMap[item.id]}
          name={name}
          itemIndex={index}
          onClick={onClick}
          className={className}
          checkBoxClass={checkBoxClass}
          showCheckIcon={showCheckIcon}
          disabled={item.disabled}
          ellipsedTextClass={ellipsedTextClass}
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
      selectedValues
    } = this.props;

    const { itemsList } = this.state;

    const getSelectedItemsLabel = (allItems, values) => {
      const selectedItems = this.getSelectedItems(allItems, values);
      const { label, label: { props: { label: propsLabel } = {} } = {} } =
        selectedItems[0] || {};
      const selectedItemsLable =
        propsLabel && typeof propsLabel === 'string'
          ? formatMessage({ id: `${propsLabel}.name` })
          : label;
      return selectedItems.length > 1
        ? `${selectedItemsLable} +${selectedItems.length - 1}`
        : selectedItemsLable;
    };

    const selectedItems = !this.props.selectedValues.size
      ? `: ${formatMessage({ id: 'Common.All' })}`
      : (alwaysShowLabel ? ': ' : '') +
        getSelectedItemsLabel(itemsList, selectedValues);

    return (
      !preventSelectedLabel && (
        <EllipsedText
          className={clsx('filter-select-label', ellipsedTextClass)}
          style={styles.infoLabel}
          text={selectedItems}
        />
      )
    );
  }

  render() {
    const {
      label,
      name,
      showSelectedLabel,
      icon,
      iconStyle,
      popoverFilters,
      selectClass,
      isArtIcon,
      iconClass,
      selectedValues,
      preventSelectedLabel,
      alwaysShowLabel,
      noSearch,
      searchPlaceholder,
      filterItemHeight,
      listClass,
      popoverContentClass,
      popoverContentHeight,
      cellCache
    } = this.props;

    const { pattern, itemsList } = this.state;

    const orderedItems = this.getOrderedItems(
      itemsList,
      selectedValues,
      pattern
    );
    const unSelectedItems = this.getUnselectedItems(itemsList, selectedValues);
    const selectedMap = this.getSelectedMap(itemsList, selectedValues);

    const className = clsx('filter-select-menu field', selectClass);
    const hasSearch = !noSearch && (!!unSelectedItems.length || !!pattern);
    const pHeight =
      popoverContentHeight ||
      getPopoverHeight(
        orderedItems.length,
        hasSearch,
        filterItemHeight || FILTER_ITEM_HEIGHT
      );

    return (
      <SelectView
        content={
          <Fragment>
            {(preventSelectedLabel ||
              !selectedValues.size ||
              alwaysShowLabel) && <span>{label}</span>}
            {showSelectedLabel ? this.renderSelectedLabel() : label}
          </Fragment>
        }
        beforeClose={this.beforeDDClose}
        beforeOpen={this.beforeDDOpen}
        className={className}
        dataQa={name + '-filter'}
        icon={icon}
        iconStyle={iconStyle}
        popoverFilters={popoverFilters}
        isArtIcon={isArtIcon}
        iconClass={iconClass}
      >
        <div
          style={{ ...styles.popoverContent, height: pHeight }}
          className={popoverContentClass}
        >
          {hasSearch && (
            <SearchField
              style={styles.searchStyle}
              searchIconTheme={styles.searchIcon}
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
                  rowHeight={
                    (cellCache ? cellCache.rowHeight : null) ||
                    filterItemHeight ||
                    FILTER_ITEM_HEIGHT
                  }
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
export const getDataQaForFilterItem = (id) => id + '-filter-item';

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    cursor: 'pointer'
  },
  popoverContent: {
    display: 'flex',
    flexWrap: 'wrap',
    flexDirection: 'column',
    minWidth: 240,
    paddingLeft: 16,
    paddingRight: 16,
    paddingTop: 8,
    paddingBottom: 8
  },
  virtualContainer: {
    flex: 1
  },
  arrow: {
    Container: {
      position: 'relative',
      top: 1
    }
  },
  infoLabel: {
    maxWidth: 130
  },
  searchInput: {
    height: FILTER_ITEM_HEIGHT,
    padding: '4px 10px'
  },
  searchIcon: {
    Container: {
      position: 'absolute',
      right: 6,
      top: 3
    },
    Icon: {
      width: 22,
      height: 22,
      marginTop: '3px'
    }
  },
  checkboxLabelContainer: {
    overflow: 'hidden',
    height: 24,
    width: 24
  }
};
