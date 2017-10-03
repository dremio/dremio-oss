/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Component, PropTypes } from 'react';
import ReactDOM from 'react-dom';
import Radium from 'radium';
import { Overlay } from 'react-overlays';
import Immutable from 'immutable';
import $ from 'jquery';

import { FLEX_COL_START, LINE_NOWRAP_ROW_BETWEEN_CENTER } from 'uiTheme/radium/flexStyle';
import { CELL_EXPANSION_HEADER, WHITE, BLUE } from 'uiTheme/radium/colors';
import { body, fixedWidthSmall } from 'uiTheme/radium/typography';
import EllipsedText from 'components/EllipsedText';
import { MAP, TEXT, LIST } from 'constants/DataTypes';
import exploreUtils from 'utils/explore/exploreUtils';
import FontIcon from 'components/Icon/FontIcon';
import dataFormatUtils from 'utils/dataFormatUtils';
import ViewStateWrapper from 'components/ViewStateWrapper';
import CellPopover from './CellPopover';
import './ExploreCellLargeOverlay.less';

@Radium
export default class ExploreCellLargeOverlay extends Component {
  static propTypes = {
    // If type of column is List or map, cellValue should be an object.
    // In other cases it should be a string
    cellValue: PropTypes.any,
    anchor: PropTypes.instanceOf(Element).isRequired,
    columnType: PropTypes.string,
    columnName: PropTypes.string,
    valueUrl: PropTypes.string,
    fullCell: PropTypes.instanceOf(Immutable.Map),
    fullCellViewState: PropTypes.instanceOf(Immutable.Map),
    hide: PropTypes.func.isRequired,
    loadFullCellValue: PropTypes.func,
    clearFullCallValue: PropTypes.func,
    onSelect: PropTypes.func,
    openPopover: PropTypes.bool,
    selectAll: PropTypes.func,
    isDumbTable: PropTypes.bool,
    location: PropTypes.object
  };

  static contextTypes = {
    router: PropTypes.object
  };

  constructor(props) {
    super(props);

    this.state = {
      maxHeight: 120,
      placement: 'top',
      placementLeft: 0
    };
  }

  componentWillMount() {
    const href = this.props.valueUrl;
    if (href) {
      this.props.loadFullCellValue({ href });
    }
  }

  componentDidMount() {
    this.calculateMaxHeight();
    this.onOverlayShow();
    // we require this line to prevent hover on + icon in tree, because we moveout this icon from tree block
    $('.Object-node').on('mouseenter', 'div', this.onNodeMouseEnter);
    $('li').on('mouseleave', '.Object-node', this.clearCurrentPath);
  }

  componentDidUpdate(prevProps) {
    const showOverlay = this.showOverlay(this.props);
    if (showOverlay !== this.showOverlay(prevProps)) {
      this.onOverlayShow();
    }
  }

  componentWillUnmount() {
    if (this.props.clearFullCallValue) {
      this.props.clearFullCallValue();
    }
    this.props.hide();
    $('.Object-node').off('mouseenter', 'div', this.onNodeMouseEnter);
    $('li').off('mouseleave', '.Object-node', this.clearCurrentPath);
  }

  onOverlayShow() {
    if (this.showOverlay(this.props)) {
      this.changePlacementIfNecessary();
      this.calculatePlacementLeft();
    }
  }

  onNodeMouseEnter = (e) => {
    e.stopPropagation();
  }

  onMouseUp = () => {
    if (!this.props.onSelect) {
      return;
    }
    const { columnType, columnName, location, isDumbTable } = this.props;
    const selection = exploreUtils.getSelectionData(window.getSelection());
    if (selection && selection.text && !isDumbTable) {
      const columnText = selection.oRange.startContainer.data;
      if (columnType !== TEXT) {
        this.handleSelectAll();
      } else {
        const data = exploreUtils.getSelection(columnText, columnName, selection);
        this.context.router.push({
          ...location,
          state: {
            ...location.state,
            columnName,
            columnType,
            selection: Immutable.fromJS({...data.model, columnName})
          }
        });
        this.props.onSelect({...data.position, columnType});
      }
      window.getSelection().removeAllRanges();
    }
  }

  onCurrentPathChange = (currentPath) => {
    this.setState({
      currentPath: this.getBeautyPath(currentPath)
    });
  }

  getBeautyPath(path) {
    const parts = path.split('.');
    return parts.reduce((prevPart, part) =>
      $.isNumeric(part) ? `${prevPart}[${part}]` : `${prevPart}.${part}`
    );
  }

  getCellValue({ cellValue, fullCell, valueUrl, columnType }) {
    return valueUrl ? fullCell.get('value') : dataFormatUtils.formatValue(cellValue, columnType);
  }

  showOverlay(props) {
    const { anchor } = props;
    const cellValue = this.getCellValue(props);
    return Boolean(anchor && cellValue);
  }

  changePlacementIfNecessary() {
    const largeOverlayNode = ReactDOM.findDOMNode(this.refs.largeOverlay);
    if (this.props.anchor && largeOverlayNode) {
      const anchorRect = this.props.anchor.getBoundingClientRect();
      const largeOverlayRect = largeOverlayNode.getBoundingClientRect();
      if (largeOverlayRect.height > anchorRect.top) {
        this.setState({
          placement: 'bottom'
        });
      }
    }
  }

  handleSelectMenuVisibleChange = (state) => {
    this.setState({
      preventHovers: state
    });
  }

  clearCurrentPath = (e) => {
    if (this.state.preventHovers) {
      e.stopPropagation();
    } else {
      this.setState({currentPath: ''});
    }
  }

  calculateMaxHeight() {
    setTimeout(() => {
      const domElement = this.refs.largeOverlay && $(ReactDOM.findDOMNode(this.refs.largeOverlay))[0];
      const top = domElement && domElement.offsetTop;

      if (top < 0) {
        $(domElement).css({top: 4});
      }
    });
  }

  calculatePlacementLeft() {
    setTimeout(() => {
      const largeOverlayNode = ReactDOM.findDOMNode(this.refs.largeOverlay);
      if (this.props.anchor && largeOverlayNode) {
        const anchorRect = this.props.anchor.getBoundingClientRect();
        const largeOverlayRect = largeOverlayNode.getBoundingClientRect();
        const OFFSET = 3;
        const placementLeft = anchorRect.left - largeOverlayRect.left - OFFSET;
        this.setState({
          placementLeft
        });
      }
    });
  }

  handleSelectAll = () => {
    const { columnType, columnName } = this.props;
    const cellValue = this.getCellValue(this.props);
    this.props.selectAll(this.refs.content, columnType, columnName, cellValue);
  }

  renderContent() {
    const { columnType, columnName, location, hide } = this.props;
    const cellValue = this.getCellValue(this.props);

    if (columnType === MAP || columnType === LIST) {
      return (
        <CellPopover
          availibleActions={['extract']}
          ref='cellPopover'
          hide={hide}
          onCurrentPathChange={this.onCurrentPathChange}
          cellPopover={Immutable.fromJS({ columnName, columnType })}
          data={dataFormatUtils.formatValue(cellValue, columnType)}
          location={location}
          hideCellPopover={hide}
          openPopover={this.state.openPopover}
          isDumbTable={this.props.isDumbTable}
          onSelectMenuVisibleChange={this.handleSelectMenuVisibleChange}
        />
      );
    }
    const style = { minHeight: 20 };
    if (columnType === 'TEXT') {
      style.whiteSpace = 'pre-wrap'; // todo: have to make copy also preserve
    }
    return (
      <div style={[styles.content, style]} onMouseUp={this.onMouseUp} data-qa='cell-content'>
        <span ref='content'>{cellValue}</span>
      </div>
    );
  }

  render() {
    const { placement, placementLeft } = this.state;
    const { columnType, fullCell, anchor } = this.props;
    const pointerStyle = placement === 'top' ? { bottom: 0 } : { top: 0 };
    const overlayStyle = placement === 'top' ? styles.top : styles.bottom;
    return (
      <Overlay
        ref='overlay'
        show={this.showOverlay(this.props)}
        target={anchor}
        onHide={this.props.hide}
        container={this}
        placement={placement}
        rootClose={columnType !== MAP && columnType !== LIST}>
        <div style={[styles.overlay, overlayStyle]} className={`large-overlay ${placement}`} ref='largeOverlay'>
          <div style={[styles.pointer, pointerStyle, { left: placementLeft }]}>
            <div className='pointer-bottom'></div>
            <div className='pointer-top'></div>
          </div>
          <div style={[styles.header]}>
            <div style={styles.path}>
              {
                this.props.onSelect && columnType !== MAP && columnType !== LIST
                  ? <span onClick={this.handleSelectAll} style={{cursor: 'pointer'}}>
                    {la('Select all')}
                  </span>
                  : <EllipsedText text={this.state.currentPath} />
              }
            </div>
            <FontIcon type='XSmall' onClick={this.props.hide} theme={styles.close}/>
          </div>
          <ViewStateWrapper viewState={fullCell}>
            {this.renderContent()}
          </ViewStateWrapper>
        </div>
      </Overlay>
    );
  }
}

const styles = {
  pointer: {
    position: 'absolute'
  },
  overlay: {
    maxHeight: 450,
    width: 400,
    marginLeft: -15,
    border: '1px solid #E9E9E9',
    boxShadow: '0px 0px 5px 0px rgba(0,0,0,0.05)',
    borderRadius: 2,
    position: 'absolute',
    zIndex: 1000,
    backgroundColor: WHITE,
    ...FLEX_COL_START
  },
  top: {
    marginTop: -4
  },
  bottom: {
    marginTop: 4
  },
  path: {
    display: 'inline-block',
    flexGrow: 1,
    minWidth: 0
  },
  header: { // todo: fix styling/element type for consistent UX
    width: '100%',
    ...LINE_NOWRAP_ROW_BETWEEN_CENTER,
    ...body,
    backgroundColor: CELL_EXPANSION_HEADER,
    color: BLUE,
    paddingLeft: 5,
    height: 24
  },
  close: {
    Container: {
      cursor: 'pointer'
    }
  },
  content: {
    maxHeight: 250,
    ...fixedWidthSmall,
    backgroundColor: WHITE,
    padding: '10px 5px',
    overflowX: 'auto'
  }
};
