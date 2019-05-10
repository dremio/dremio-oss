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
import rand from 'csprng';
import { sortedIndex } from 'lodash/array';
import transformModelMapper from 'utils/mappers/ExplorePage/Transform/transformModelMapper';
import simpleTransformMappers from 'utils/mappers/ExplorePage/simpleTransformMappers';
import mapConvertDataType from 'utils/mappers/ExplorePage/convertDataType';
import dataStoreUtils from 'utils/dataStoreUtils';
import { TEXT, INTEGER, FLOAT, DECIMAL, BOOLEAN, DATE, DATETIME, TIME, LIST, MAP } from 'constants/DataTypes';

const DROP_WIDTH = 150;
const ARROW_WIDTH = 20;
const SHADOW_WIDTH = 1;
const WIDTH_WITH_PADDING = 6;
const HEIGHT_WITH_LINEHEIGHT = 9;
const HALF_LINEHEIGHT = HEIGHT_WITH_LINEHEIGHT / 2;
const ROWS_LIMIT = 150;
const TRANSFORM_TYPE_WITHOUT_DATA = [
  'EXTRACT_ELEMENT', 'EXTRACT_ELEMENTS',
  'EXTRACT_TEXT', 'REPLACE_TEXT', 'SPLIT',
  'KEEP_ONLY', 'EXCLUDE', 'REPLACE'
];

const detailTypeToTransformType = {
  EXTRACT_ELEMENT: 'extract',
  EXTRACT_ELEMENTS: 'extract',
  EXTRACT_TEXT: 'extract',
  REPLACE_TEXT: 'replace',
  REPLACE: 'replace',
  KEEP_ONLY: 'keeponly',
  SPLIT: 'split',
  EXCLUDE: 'exclude'
};

const notNeedSelectionMethod = ['Values', 'Range', 'Custom Condition', 'Exact'];

const transformationMapperHash = {
  ASC: simpleTransformMappers.mapSortItem,
  DESC: simpleTransformMappers.mapSortItem,
  UNNEST: simpleTransformMappers.mapUnnest,
  RENAME: simpleTransformMappers.mapRename,
  MULTIPLE: simpleTransformMappers.mapMultySortItem,
  CONVERT_CASE: simpleTransformMappers.mapCovertCase,
  TRIM_WHITE_SPACES: simpleTransformMappers.mapTrim,
  transform: transformModelMapper.transformApplyOrPreviewMapper,
  CALCULATED_FIELD: simpleTransformMappers.mapCalculated,
  DROP: simpleTransformMappers.mapDrop,
  CONVERT_DATA_TYPE: mapConvertDataType,
  SPLIT_BY_DATA_TYPE: simpleTransformMappers.mapCleanData,
  SINGLE_DATA_TYPE: simpleTransformMappers.mapCleanData,
  GROUP_BY: simpleTransformMappers.mapGroupBy,
  JOIN: simpleTransformMappers.mapJoin
};

class ExploreUtils {

  getTransformState(location) {
    const state = location.state || {};
    return Immutable.fromJS({
      transformType: state.transformType,
      columnName: state.columnName,
      columnType: state.columnType,
      method: state.method || this.getInitialTransformMethod(state.columnType, state.transformType, state.selection),
      selection: state.selection || {}
    });
  }

  getLocationToGoToTransformWizard({detailType, column, toType, location, joinTab, props}) {
    const { pathname: oldPathname, query: oldQuery } = location;
    const pathname = `${oldPathname}/details`;
    const query = {
      ...oldQuery,
      type: detailType
    };
    const state = {
      columnName: column.name,
      columnType: column.type,
      toType,
      previewVersion: '',
      props
    };
    if (joinTab) {
      query.joinTab = joinTab;
    }
    if (TRANSFORM_TYPE_WITHOUT_DATA.indexOf(detailType) !== -1) {
      query.type = 'transform';
      state.transformType = detailTypeToTransformType[detailType] || detailType;
      state.method = this.getInitialTransformMethod(state.columnType, state.transformType, state.selection);
    }
    return { pathname, state, query };
  }

  getInitialTransformMethod(columnType, transformType, selection) {
    const hasSelection = selection && selection.cellText !== '';
    switch (columnType) {
    case TEXT:
      switch (transformType) {
      case 'replace':
      case 'keeponly':
      case 'exclude':
        return hasSelection ? 'Pattern' : 'Values';
      default:
        return 'Pattern';
      }
    case DATETIME:
    case DATE:
    case TIME:
    case INTEGER:
    case FLOAT:
    case DECIMAL:
      switch (transformType) {
      case 'replace':
      case 'keeponly':
      case 'exclude':
        return hasSelection ? 'Exact' : 'Range';
      default:
        break;
      }
      break;
    case BOOLEAN:
      return 'Values';
    default:
      break;
    }

    const firstMethodForColumnType = dataStoreUtils.getSubtypeForTransformTab().find((subtitle) => {
      return subtitle.types.find((type) => type === columnType);
    });
    if (firstMethodForColumnType) {
      return firstMethodForColumnType.name;
    }
  }

  transformHasSelection(transform) {
    return Boolean(transform.getIn(['selection', 'cellText'])) ||
      Boolean(transform.getIn(['selection', 'mapPathList']));
  }

  /**
   * Trims empty cells out of selection. Returns null if selection spans multiple cells.
   * @param selection
   * @returns selection with modified range
   */
  updateSelectionToSingleCell(selection) {
    if (!selection || selection.rangeCount !== 1) {
      return null;
    }
    const selRange = selection.getRangeAt(0);
    let startCell = this.getTableCellElement(selRange.startContainer);
    let endCell = this.getTableCellElement(selRange.endContainer);

    // already single cell?
    if (startCell === endCell) {
      return selection;
    }

    // try removing outside cells
    let rangeStart = selRange.startOffset;
    if (startCell && startCell.textContent.length === rangeStart) {
      startCell = startCell.nextSibling;
      rangeStart = 0;
    }
    let rangeEnd = selRange.endOffset;
    if (endCell && rangeEnd === 0) {
      endCell = endCell.previousSibling;
      if (endCell) {
        rangeEnd = endCell.textContent.length;
      }
    }

    if (startCell !== endCell) {
      return null;
    }

    const textNode = this.getTableCellTextNode(startCell);
    this.replaceSelectionRange(selection, textNode, rangeStart, rangeEnd);

    return selection;
  }

  getTableCellElement(node) {
    const result = node.nodeType === window.Node.TEXT_NODE ? node.parentNode : node;
    return result.closest('.public_fixedDataTableCell_main');
  }

  replaceSelectionRange(selection, node, start, end) {
    selection.removeAllRanges();
    const range = document.createRange();
    range.setStart(node, start);
    range.setEnd(node, end);
    selection.addRange(range);
  }

  getTableCellTextNode(cell) {
    return $('.cell-wrap', cell).contents()[0];
  }

  getSelectionData(selection) {
    if (!selection || selection.rangeCount !== 1) {
      return null;
    }

    const selectionText = selection.toString(); //DX-11088 we would like to enable spliting data by space. So we should not trim selection value.
    const oRange = selection.getRangeAt(0);
    if (oRange.endContainer === oRange.startContainer) {
      return {
        selection,
        text: selectionText,
        oRange,
        oRect: oRange.getBoundingClientRect(),
        startOffset: oRange.startOffset,
        endOffset: oRange.endOffset
      };
    }

    return {
      selection,
      text: selectionText,
      oRange,
      oRect: oRange.getBoundingClientRect(),
      startOffset: oRange.startOffset,
      endOffset: oRange.endOffset
    };
  }

  calculateListSelectionOffsets(startOffset, endOffset, text) {
    const items = JSON.parse(text);
    if (!items || !items.length) {
      return null;
    }

    const itemsText = items.map(JSON.stringify);
    // calcluate indices of first and last chars of an item
    const itemExtents = itemsText.slice(1).reduce(
      (prev, itemText) => {
        const start = prev[prev.length - 1][1] + 2; // + 1 for comma, +1 to advance to next item.
        return prev.concat([[start, start + itemText.length - 1]]);
      }, [[1, 1 + itemsText[0].length  - 1]]
    );

    const startItemIndex = sortedIndex(itemExtents.map(x => x[1]), startOffset);
    const endItemIndex = sortedIndex(itemExtents.map(x => x[0]), endOffset); // This end index is exclusive

    if (startItemIndex === items.length || endItemIndex === 0 || startItemIndex === endItemIndex) {
      return null;
    }

    return {
      itemsText,
      startIndex: startItemIndex,
      endIndex: endItemIndex,
      startOffset: itemExtents[startItemIndex][0],
      endOffset: itemExtents[endItemIndex - 1][1] + 1 // +1 to change inclusive to exclusive
    };
  }

  _findItemsForList(startOffset, endOffset, text, sel) {
    const newOffsets = this.calculateListSelectionOffsets(startOffset, endOffset, text);
    if (newOffsets === null) {
      return null;
    }
    const {
      itemsText,
      startIndex,
      endIndex,
      startOffset: newStartOffset,
      endOffset: newEndOffset
    } = newOffsets;

    this.replaceSelectionRange(sel, sel.anchorNode, newStartOffset, newEndOffset);
    const selectedText = text.slice(newStartOffset, newEndOffset);
    return {
      oRect: sel.getRangeAt(0).getBoundingClientRect(),
      selectedText,
      startIndex,
      endIndex,
      listOfItems: itemsText
    };
  }

  getSelectionForList(columnText, columnName, sel) {
    const {startOffset, endOffset, selection} = sel;
    const itemInfo = this._findItemsForList(startOffset, endOffset, columnText, selection);
    if (!itemInfo) {
      return null;
    }
    const {selectedText, listOfItems, startIndex, endIndex, oRect} = itemInfo;
    return {
      listOfItems,
      model: {
        columnName,
        cellText: columnText,
        startIndex,
        endIndex, // exclusive
        listLength: listOfItems.length
      },
      position: {
        display: true,
        left: oRect.left + oRect.width,
        top: oRect.top + oRect.height,
        textWrap: {
          width: 'auto',
          height: oRect.height + HEIGHT_WITH_LINEHEIGHT,
          left: oRect.left - SHADOW_WIDTH,
          top: oRect.top - HALF_LINEHEIGHT,
          text: selectedText.toString().trim()
        }
      }
    };
  }

  getSelection(columnText, columnName, selection) {
    const { oRect, text, startOffset, endOffset } = selection;

    return {
      model: {
        columnName,
        cellText: columnText,
        length: text.length,
        offset: startOffset > endOffset ? endOffset : startOffset,
        fullCellSelection: columnText === text.trim()
      },
      position: {
        display: true,
        left: oRect.left + oRect.width - DROP_WIDTH + ARROW_WIDTH + SHADOW_WIDTH,
        top: oRect.top + oRect.height,
        textWrap: {
          width: oRect.width + WIDTH_WITH_PADDING,
          height: oRect.height + HEIGHT_WITH_LINEHEIGHT,
          left: oRect.left - SHADOW_WIDTH,
          top: oRect.top - HALF_LINEHEIGHT,
          text: text.trim()
        }
      }
    };
  }

  selectAll(elem) {
    const position = elem.getBoundingClientRect();
    this.selectElementContents(elem);
    return {
      display: true,
      left: position.left + position.width - DROP_WIDTH + ARROW_WIDTH + SHADOW_WIDTH,
      top: position.top + position.height,
      textWrap: {
        width: position.width  + WIDTH_WITH_PADDING,
        height: position.height + HEIGHT_WITH_LINEHEIGHT,
        left: position.left - SHADOW_WIDTH,
        top: position.top  - HALF_LINEHEIGHT,
        text: elem.textContent.trim()
      }
    };
  }

  selectElementContents(elem) {
    const range = document.createRange();
    range.selectNodeContents(elem);
    const sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
  }

  getHrefForUntitledDatasetConfig = (dottedFullPath, newVersion, doNotWaitJobCompletion) => {
    const pathParam = encodeURIComponent(dottedFullPath);
    return `/datasets/new_untitled?parentDataset=${pathParam}&newVersion=${newVersion}&limit=${doNotWaitJobCompletion ? 0 : ROWS_LIMIT}`;
  }

  getHrefForDatasetConfig = (resourcePath) => `${resourcePath}?view=explore&limit=50`;
  getPreviewLink = (dataset, tipVersion) => {
    return `${dataset.getIn(['apiLinks', 'self'])}/preview?view=explore` +
        `${tipVersion ? `&tipVersion=${tipVersion}` : ''}&limit=0`;
  };

  getReviewLink = (dataset, tipVersion) =>
    `${dataset.getIn(['apiLinks', 'self'])}/review?jobId=${dataset.get('jobId')}&view=explore` +
        `${tipVersion ? `&tipVersion=${tipVersion}` : ''}&limit=0`;

  getHrefForTransform({ resourceId, tableId }, location) {
    // TODO this should be moved into dataset decorator
    const { version } = location.query;
    const fullPath = location.query.mode === 'edit' ? `${encodeURIComponent(resourceId)}.${encodeURIComponent(tableId)}` : 'tmp.UNTITLED';
    const resourcePath = `dataset/${fullPath}`;
    return `/${resourcePath}/version/${version}`;
  }

  getNewDatasetVersion = () => '000' + rand(40, 10);

  getFullPath = ({ resourceId, tableId, mode }) => mode === 'edit'
    ? `${resourceId}.${tableId}`
    : 'tmp.UNTITLED';

  getVersionedResoursePath = ({ resourceId, tableId, version, mode }) => {
    const fullPath = this.getFullPath({resourceId, tableId, mode});
    const resourcePath = `/dataset/${fullPath}`;
    return `${resourcePath}/version/${version}`;
  };

  getPreviewTransformationLink(dataset, newVersion) {
    const end = `transformAndPreview?newVersion=${newVersion}&limit=0`;
    return `${dataset.getIn(['apiLinks', 'self'])}/${end}`;
  }

  getTransformPeekHref(dataset) {
    const newVersion = this.getNewDatasetVersion();
    const end = `transformPeek?newVersion=${newVersion}&limit=${ROWS_LIMIT}`;
    return `${dataset.getIn(['apiLinks', 'self'])}/${end}`;
  }

  getRunTransformationLink({fullPath, version, newVersion, type}) {
    return `/dataset/${fullPath.join('.')}/version/${version}/${type}?newVersion=${newVersion}&limit=${ROWS_LIMIT}`;
  }

  getUntitledSqlHref({newVersion}) {
    return `/datasets/new_untitled_sql?newVersion=${newVersion}&limit=0`;
  }

  getUntitledSqlAndRunHref({newVersion}) {
    return `/datasets/new_untitled_sql_and_run?newVersion=${newVersion}`; // does not need limit=0 as does not return data in all cases
  }

  getMappedDataForTransform(item, detailsType) {
    if (detailsType === 'transform') {
      return item;
    }
    return transformationMapperHash[detailsType || item.type] && transformationMapperHash[detailsType || item.type](
        item, item.transformType
    ) || item;
  }

  hasOrigin(dataset, location) {
    if (dataset.get('isNewQuery')) {
      return true;
    }
    const {query} = location;
    return query.mode === 'edit' || dataset.get('canReapply') || query.hasOrigin === 'true';
  }

  needSelection(method, transformType) {
    // in props.transform we can get transformType = 'extract' and method = "Values"
    // it happens when we switch between "Keep only" and "Extract"
    if (transformType === 'extract') {
      return true;
    }

    return notNeedSelectionMethod.indexOf(method) === -1;
  }

  needsToLoadCardFormValuesFromServer(transform) {
    if (!transform) {
      return true;
    }
    const columnType = transform.get('columnType');
    return !(transform.get('transformType') === 'extract' && (columnType === LIST || columnType === MAP));
  }

  getDefaultSelection = (columnName) => ({
    cellText: '',
    length: 0,
    offset: 0,
    columnName
  });

  getDocumentWidth = () => $(document).width();

  _createFakeElement = (text) => {
    const fakeElement = document.createElement('textarea');
    fakeElement.value = text;
    document.body.appendChild(fakeElement);
    return fakeElement;
  }
  copySelection = (elementOrText) => {
    if (elementOrText === null) return;

    const isText = typeof elementOrText === 'string';
    const selectionElement = isText ? this._createFakeElement(elementOrText) : elementOrText;

    if (isText) {
      selectionElement.select();
      selectionElement.setSelectionRange(0, selectionElement.value.length);
    } else {
      const sel = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(selectionElement);
      sel.removeAllRanges();
      sel.addRange(range);
    }

    document.execCommand('copy');

    window.getSelection().removeAllRanges();

    if (isText) {
      document.body.removeChild(selectionElement);
    }
  }
  escapeFieldNameForSQL(value) {
    return `"${value.replace(/"/g, '""')}"`;
  }

  isFilterIncludedInColumnName(column, filter) {
    if (!column) return false;
    const name = column.get('name');
    // empty string is treated as included, null or undefined filter - not.
    return name && name.toUpperCase().includes(filter.toUpperCase());
  }

  getFilteredColumns(columns, columnFilter) {
    if (!columns || !columns.size || !columnFilter) return columns;

    return columns.filter((column) => {
      return this.isFilterIncludedInColumnName(column, columnFilter);
    });
  }

  getFilteredColumnCount(columns, columnFilter) {
    if (!columns) return 0;
    if (!columnFilter) return columns.size;

    return columns.reduce((count, column) => {
      if (this.isFilterIncludedInColumnName(column, columnFilter)) {
        count += 1;
      }
      return count;
    }, 0);
  }
}

export default (new ExploreUtils());
