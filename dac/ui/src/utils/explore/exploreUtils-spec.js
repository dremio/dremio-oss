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
import Immutable from 'immutable';
import tableMockedDataExpected from '../mappers/mocks/gridMapper/expected.json';
import exploreUtils from './exploreUtils';

describe('exploreUtils', () => {

  let commonData;
  beforeEach(() => {
    commonData = {
      location: {
        query: {
          mode: 'edit',
          version: '00dfd88fccaf990e',
          type: 'TRIM_WHITE_SPACES'
        },
        pathname: '/space/"Prod-Sample"/ds1'
      },
      column: {name: 'A', type: 'TEXT'},
      detailType: 'TRIM_WHITE_SPACES',
      toType: 'INTEGER',
      response: {
        payload: Immutable.fromJS({
          result: '/dataset/tmp.UNTITLED/version/00dfe54f85d4adc9',
          entities: {
            dataset: {
              '/dataset/tmp.UNTITLED/version/00dfe54f85d4adc9': {
                datasetConfig: {
                  version: '00dfe54f85d4adc9'
                },
                resourcePath: '/dataset/tmp.UNTITLED',
                datasetName: 'UNTITLED'
              }
            }
          }
        })
      }
    };
  });

  describe('updateSelectionToSingleCell', () => {
    let range;
    let selection;
    let centerCell;
    let leftCell;
    let rightCell;
    beforeEach(() => {
      range = {
        startOffset: 0,
        endOffset: 5
      };
      selection = {
        rangeCount: 1,
        getRangeAt: () => range
      };
      centerCell = { textContent: 'center' };
      leftCell = { textContent: 'left', nextSibling: centerCell };
      rightCell = { textContent: 'right', previousSibling: centerCell};

      sinon.stub(exploreUtils, 'getTableCellTextNode').returns('textNode');
      sinon.stub(exploreUtils, 'replaceSelectionRange');
    });
    afterEach(() => {
      if (exploreUtils.getTableCellElement.restore) {
        exploreUtils.getTableCellElement.restore();
      }
      exploreUtils.getTableCellTextNode.restore();
      exploreUtils.replaceSelectionRange.restore();
    });

    it('should return null if no selection or ranges or more than one range', () => {
      expect(exploreUtils.updateSelectionToSingleCell(null)).to.be.null;
      expect(exploreUtils.updateSelectionToSingleCell({rangeCount: 0})).to.be.null;
      expect(exploreUtils.updateSelectionToSingleCell({rangeCount: 2})).to.be.null;
    });

    it('returns selection if start and end cells are the same ', () => {
      sinon.stub(exploreUtils, 'getTableCellElement').returns(centerCell).onSecondCall().returns(centerCell);
      expect(exploreUtils.updateSelectionToSingleCell(selection)).to.equal(selection);
    });

    it('returns selection if start and end cells are the same ', () => {
      range.startOffset = leftCell.textContent.length;
      range.endOffset = 0;
      sinon.stub(exploreUtils, 'getTableCellElement').returns(leftCell).onSecondCall().returns(rightCell);
      expect(exploreUtils.updateSelectionToSingleCell(selection)).to.equal(selection);
      expect(
        exploreUtils.replaceSelectionRange
      ).to.be.calledWith(selection, 'textNode', 0, centerCell.textContent.length);
    });

    it('returns null if start and end cells are not same ', () => {
      range.startOffset = leftCell.textContent.length;
      range.endOffset = 0;
      leftCell.nextSibling = null;
      sinon.stub(exploreUtils, 'getTableCellElement').returns(leftCell).onSecondCall().returns(rightCell);
      expect(exploreUtils.updateSelectionToSingleCell(selection)).to.be.null;
    });

  });

  describe('getSelectionData', () => {
    let selection;
    let oRange;
    beforeEach(() => {
      oRange = {
        getBoundingClientRect: sinon.stub().returns('getBoundingClientRect'),
        startOffset: 0,
        endOffset: 3,
        startContainer: {
          nodeType: 3,
          textContent: 'start',
          length: 5
        },
        endContainer: {
          nodeType: 3,
          textContent: 'end',
          length: 3
        }
      };
      selection = {
        rangeCount: 1,
        getRangeAt: sinon.stub().returns(oRange),
        toString: sinon.stub().returns('text')
      };
    });

    it('handle return null if no selection or rangeCount = 0', () => {
      expect(exploreUtils.getSelectionData()).to.be.null;
      expect(exploreUtils.getSelectionData({rangeCount: 0})).to.be.null;
    });

    it('should return selection range data if selection within a single node', () => {
      oRange.endContainer = oRange.startContainer;

      expect(exploreUtils.getSelectionData(selection)).to.eql({
        selection,
        text: 'text',
        oRange,
        oRect: 'getBoundingClientRect',
        startOffset: oRange.startOffset,
        endOffset: oRange.endOffset
      });
    });
  });

  describe('getSelection', () => {
    it('should not trim selection', () => {
      const selection = {
        oRect: {width: 6, height: 12, left: 5, top: 10},
        text: ' ',
        startOffset: 3,
        endOffset: 4
      };
      const result = exploreUtils.getSelection('foo bar', 'col1', selection);
      expect(result.model.length).to.equal(1);
      expect(result.model.offset).to.equal(3);
    });
  });

  describe('calculateListSelectionOffsets', () => {
    const listText = '["abc","def","ghi"]';

    it('should return null for empty list text', () => {
      expect(exploreUtils.calculateListSelectionOffsets(0, 1, '[]')).to.be.null;
    });

    it('should return null if no item is selected', () => {
      expect(exploreUtils.calculateListSelectionOffsets(0, 1, '["a"]')).to.be.null;
      expect(exploreUtils.calculateListSelectionOffsets(4, 5, '["a"]')).to.be.null; // ["a"|]|
      expect(exploreUtils.calculateListSelectionOffsets(4, 5, '["a","b"]')).to.be.null; // ["a"|,|"b"]
    });

    function selectionResult(selectionText, text) {
      const startOffset = selectionText.indexOf('|');
      const endOffset = selectionText.indexOf('|', startOffset + 1) - 1;
      const {startOffset: newStartOffset, endOffset: newEndOffset, startIndex, endIndex } =
        exploreUtils.calculateListSelectionOffsets(startOffset, endOffset, text);
      return [text.slice(newStartOffset, newEndOffset), startIndex, endIndex];
    }

    it('should complete a partial item', () => {
      expect(selectionResult('["a|b|c","def","ghi"]', listText)).to.eql(['"abc"', 0, 1]);
    });

    it('should complete multiple items', () => {
      expect(selectionResult('["a|bc","def","ghi|"]', listText)).to.eql(['"abc","def","ghi"', 0, 3]);
    });

    it('should select the included but not excluded item when selecting at a boundary', () => {
      expect(selectionResult('["abc"|,"def",|"ghi"]', listText)).to.eql(['"def"', 1, 2]);
    });

    it('should not include square brackets', () => {
      expect(selectionResult('|["ab|c","def","ghi"]', listText)).to.eql(['"abc"', 0, 1]);
      expect(selectionResult('["abc","def",|"ghi"]|', listText)).to.eql(['"ghi"', 2, 3]);
    });

    it('should handle nested objects and lists', () => {
      const complexListText = '["abc",["def","ghi"],5,{"a":2}]';
      expect(selectionResult('["abc",["|d|ef","ghi"],5,{"a":2}]', complexListText)).to.eql(['["def","ghi"]', 1, 2]);
      expect(selectionResult('["abc",["def","ghi"],|5|,{"a":2}]', complexListText)).to.eql(['5', 2, 3]);
      expect(selectionResult('["abc",["def","ghi"],5,|{"a":|2}]', complexListText)).to.eql(['{"a":2}', 3, 4]);
    });
  });

  describe('method getLocationToGoToTransformWizard', () => {
    it('should return pathname/details', () => {
      const {detailType, column, toType, location} = commonData;
      const result = exploreUtils.getLocationToGoToTransformWizard({detailType, column, toType, location});
      expect(result.pathname).to.eql(`${location.pathname}/details`);
    });

    it('should return state with column and columnType from column', () => {
      const {detailType, column, toType, location} = commonData;
      const result = exploreUtils.getLocationToGoToTransformWizard({detailType, column, toType, location, props: 1});
      expect(result.state).to.eql({
        ...location.state,
        columnName: column.name,
        columnType: column.type,
        toType,
        props: 1,
        previewVersion: ''
      });
    });

    it('should add type to query', () => {
      const {detailType, column, toType, location} = commonData;
      const result = exploreUtils.getLocationToGoToTransformWizard({detailType, column, toType, location});
      expect(result.query).to.eql({...location.query, type: detailType});
    });

    it('should set transformType for Extract, Replace, Split, Keep Only, Exclude', () => {
      const {column, toType, location} = commonData;
      const result = exploreUtils.getLocationToGoToTransformWizard({detailType: 'SPLIT', column, toType, location});
      expect(result.state.transformType).to.eql('split');
    });
  });

  describe('method getMappedDataForTransform', () => {
    it('should return map drop data to server format', () => {
      const data = {
        type: 'DROP',
        columnName: 'user'
      };
      expect(exploreUtils.getMappedDataForTransform(data)).to.be.deep.equal({type: 'drop', droppedColumnName: 'user'});
    });

    it('should return map drop data to server format', () => {
      const data = {
        type: 'RENAME',
        columnName: 'age',
        newColumnName: 'age123'
      };
      expect(exploreUtils.getMappedDataForTransform(data)).to.be.deep.equal({
        type: 'rename',
        oldColumnName: 'age',
        newColumnName: 'age123'
      });
    });
  });

  describe('method getPreviewTransformationLink', () => {
    const dataset = Immutable.fromJS({
      apiLinks: {
        self: '/dataset/12/version/123'
      }
    });
    it('should return link for preview', () => {
      //limit should be 0, as we decouple data loading from metadata loading. 0 means do not load
      // a data with initial response.
      expect(
        exploreUtils.getPreviewTransformationLink(dataset, '345')
      ).to.eql(`${dataset.getIn(['apiLinks', 'self'])}/transformAndPreview?newVersion=345&limit=0`);
    });
  });

  describe('method getVersionedResoursePath', () => {
    it('should return link for transform', () => {
      const resourceId = '123';
      const tableId = '123';
      const version = '123';
      const mode = '123';
      expect(exploreUtils.getVersionedResoursePath({
        resourceId, tableId, version, mode
      })).to.eql('/dataset/tmp.UNTITLED/version/123');
    });

    it('should return link for transform', () => {
      const resourceId = '123';
      const tableId = '123';
      const version = '123';
      const mode = 'edit';
      expect(exploreUtils.getVersionedResoursePath({
        resourceId, tableId, version, mode
      })).to.eql('/dataset/123.123/version/123');
    });
  });

  describe('method getFullPath', () => {
    it('should return tmp path', () => {
      const resourceId = '123';
      const tableId = '123';
      const mode = '123';
      expect(exploreUtils.getFullPath({
        resourceId, tableId, mode
      })).to.eql('tmp.UNTITLED');
    });

    it('should return full path based on dataset data', () => {
      const resourceId = '123';
      const tableId = '123';
      const mode = 'edit';
      expect(exploreUtils.getFullPath({
        resourceId, tableId, mode
      })).to.eql('123.123');
    });
  });

  describe('method getNewDatasetVersion', () => {
    it('should return unique versions in special format 000 + timastamp', () => {
      expect(exploreUtils.getNewDatasetVersion().length).to.eql(16);
      expect(exploreUtils.getNewDatasetVersion().slice(0, 3)).to.eql('000');
    });
  });

  describe('method getHrefForUntitledDatasetConfig', () => {
    it('should return link for untitled dataset', () => {
      expect(exploreUtils.getHrefForUntitledDatasetConfig('path', 'vvv')).
        to.eql('/datasets/new_untitled?parentDataset=path&newVersion=vvv&limit=150');
    });
    it('should return link for untitled dataset', () => {
      expect(exploreUtils.getHrefForUntitledDatasetConfig('path', '')).
        to.eql('/datasets/new_untitled?parentDataset=path&newVersion=&limit=150');
    });
  });

  describe('method getHrefForDatasetConfig', () => {
    it('should return link for dataset config', () => {
      expect(exploreUtils.getHrefForDatasetConfig('path')).to.eql('path?view=explore&limit=50');
    });
    it('should return link for dataset config', () => {
      expect(exploreUtils.getHrefForDatasetConfig('path1', '')).to.eql('path1?view=explore&limit=50');
    });
  });

  describe('method escapeFieldNameForSQL', () => {
    it('should escape quote with another quote and wrap with quotes', () => {
      expect(exploreUtils.escapeFieldNameForSQL('foo"bar')).to.eql('"foo""bar"');
    });
  });

  describe('getFilteredColumns', () => {
    const columnsList = Immutable.fromJS(tableMockedDataExpected.columns);
    it('should preserve columns with no or empty filter', () => {
      expect(columnsList.size).to.equal(4);
      expect(exploreUtils.getFilteredColumns(columnsList, '').size).to.equal(4);
    });
    it('should keep only columns with name including filter value', () => {
      const filteredColumns = exploreUtils.getFilteredColumns(columnsList, 'o_');
      expect(filteredColumns.size).to.equal(2);
      expect(filteredColumns.get(0).get('name').includes('o_')).to.equal(true);
    });
  });

  describe('getFilteredColumnCount', () => {
    const columnsList = Immutable.fromJS(tableMockedDataExpected.columns);
    it('should return zero when columns is not provided', () => {
      expect(exploreUtils.getFilteredColumnCount(null)).to.equal(0);
    });
    it('should return full array size w/o filter', () => {
      expect(exploreUtils.getFilteredColumnCount(columnsList)).to.equal(columnsList.size);
      expect(exploreUtils.getFilteredColumnCount(columnsList, '')).to.equal(columnsList.size);
    });
    it('should return filtered array size', () => {
      expect(exploreUtils.getFilteredColumnCount(columnsList, 'o_')).to.equal(2);
      expect(exploreUtils.getFilteredColumnCount(columnsList, 'l_')).to.equal(1);
      // filter should be case insensitive
      expect(exploreUtils.getFilteredColumnCount(columnsList, 'O_')).to.equal(2);
      expect(exploreUtils.getFilteredColumnCount(columnsList, 'L_')).to.equal(1);
    });
  });
});
