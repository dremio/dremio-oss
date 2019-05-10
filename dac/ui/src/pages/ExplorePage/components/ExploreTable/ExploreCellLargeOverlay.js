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
import { Component, createRef, Fragment } from 'react';
import { compose } from 'redux';
import { connect } from 'react-redux';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { Overlay } from 'react-overlays';
import Immutable from 'immutable';
import $ from 'jquery';

import { FLEX_COL_START, LINE_NOWRAP_ROW_BETWEEN_CENTER } from 'uiTheme/radium/flexStyle';
import { CELL_EXPANSION_HEADER, WHITE, BLUE } from 'uiTheme/radium/colors';
import { fixedWidthSmall } from 'uiTheme/radium/typography';
import EllipsedText from 'components/EllipsedText';
import { MAP, TEXT, LIST } from 'constants/DataTypes';
import exploreUtils from 'utils/explore/exploreUtils';
import FontIcon from 'components/Icon/FontIcon';
import dataFormatUtils from 'utils/dataFormatUtils';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { withLocation } from 'containers/dremioLocation';
import { moduleStateHOC } from '@app/containers/ModuleStateContainer';
import exploreFullCell, { moduleKey, getCell } from '@app/reducers/modules/exploreFullCell';
import { KeyChangeTrigger } from '@app/components/KeyChangeTrigger';

import {
  loadFullCellValue,
  clearFullCellValue
} from 'actions/explore/dataset/data';
import CellPopover from './CellPopover';
import './ExploreCellLargeOverlay.less';


const mapStateToProps = state => ({
  fullCell: getCell(state)
});

const mapDispatchToProps = {
  loadFullCellValue,
  clearFullCellValue
};

@Radium
export class ExploreCellLargeOverlayView extends Component {
  static propTypes = {
    // If type of column is List or map, cellValue should be an object.
    // In other cases it should be a string
    cellValue: PropTypes.any,
    anchor: PropTypes.instanceOf(Element).isRequired,
    isTruncatedValue: PropTypes.bool,
    columnType: PropTypes.string,
    columnName: PropTypes.string,
    valueUrl: PropTypes.string,
    fullCell: PropTypes.instanceOf(Immutable.Map),
    fullCellViewState: PropTypes.instanceOf(Immutable.Map),
    hide: PropTypes.func.isRequired,
    loadFullCellValue: PropTypes.func,
    clearFullCellValue: PropTypes.func,
    onSelect: PropTypes.func,
    openPopover: PropTypes.bool,
    selectAll: PropTypes.func,
    isDumbTable: PropTypes.bool,
    location: PropTypes.object
  };

  static contextTypes = {
    router: PropTypes.object
  };

  contentRef = createRef();

  loadCellData = (href) => {
    if (href) {
      this.props.loadFullCellValue({ href });
    }
  }

  componentDidMount() {
    // we require this line to prevent hover on + icon in tree, because we moveout this icon from tree block
    $('.Object-node').on('mouseenter', 'div', this.onNodeMouseEnter);
    $('li').on('mouseleave', '.Object-node', this.clearCurrentPath);
  }

  componentWillUnmount() {
    if (this.props.clearFullCellValue) {
      this.props.clearFullCellValue();
    }
    this.props.hide();
    $('.Object-node').off('mouseenter', 'div', this.onNodeMouseEnter);
    $('li').off('mouseleave', '.Object-node', this.clearCurrentPath);
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

  handleSelectAll = () => {
    const { columnType, columnName } = this.props;
    const cellValue = this.getCellValue(this.props);
    this.props.selectAll(this.contentRef.current, columnType, columnName, cellValue);
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
        <span ref={this.contentRef}>{cellValue}</span>
      </div>
    );
  }

  render() {
    const { columnType, fullCell, anchor, valueUrl } = this.props;
    return (
      <Fragment>
        <KeyChangeTrigger keyValue={valueUrl} onChange={this.loadCellData} />
        <Overlay
          ref='overlay'
          show={this.showOverlay(this.props)}
          target={anchor}
          flip
          onHide={this.props.hide}
          container={document.body}
          placement='top'
          rootClose={columnType !== MAP && columnType !== LIST}
        >
          {
            ({ props: overlayProps, arrowProps, placement }) => {
              const pointerStyle = placement === 'top' ? { bottom: 0 } : { top: 0 };
              const overlayStyle = placement === 'top' ? styles.top : styles.bottom;

              return (
                <div style={[styles.overlay, overlayProps.style, overlayStyle]} className={`large-overlay ${placement}`} ref={overlayProps.ref}>
                  <div style={[styles.pointer, pointerStyle, arrowProps.style]} ref={arrowProps.ref}>
                    <div className='pointer-bottom'></div>
                    <div className='pointer-top'></div>
                  </div>
                  {this.renderHeader()}
                  <ViewStateWrapper viewState={fullCell}>
                    {this.renderContent()}
                  </ViewStateWrapper>
                </div>
              );
            }
          }
        </Overlay>
      </Fragment>
    );
  }

  renderHeader() {
    const { columnType, onSelect, hide, isTruncatedValue } = this.props;

    return (
      <div style={[styles.header]}>
        {isTruncatedValue && <span style={styles.infoMessage}>
          Values are truncated for preview
        </span>}
        <div style={styles.path}>
          {
            onSelect && columnType !== MAP && columnType !== LIST
              ? <span onClick={this.handleSelectAll} style={{cursor: 'pointer'}}>
                {la('Select all')}
              </span>
              : <EllipsedText text={this.state.currentPath} />
          }
        </div>
        <FontIcon type='XSmall' onClick={hide} theme={styles.close}/>
      </div>
    );
  }
}

export default compose(
  moduleStateHOC(moduleKey, exploreFullCell),
  withLocation,
  connect(mapStateToProps, mapDispatchToProps)
)(ExploreCellLargeOverlayView);

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
    minWidth: 0,
    color: BLUE
  },
  header: { // todo: fix styling/element type for consistent UX
    width: '100%',
    ...LINE_NOWRAP_ROW_BETWEEN_CENTER,
    backgroundColor: CELL_EXPANSION_HEADER,
    paddingLeft: 5,
    height: 24
  },
  infoMessage: {
    fontStyle: 'italic'
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
