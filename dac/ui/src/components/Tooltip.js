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
import { PureComponent } from 'react';
import { Overlay } from 'react-overlays';

import PropTypes from 'prop-types';

import getTooltipStyles from 'uiTheme/radium/tooltips';

export class Tooltip extends PureComponent {
  static propTypes = {
    children: PropTypes.node,
    type: PropTypes.string,
    /** identifies */
    placement: PropTypes.string,
    id: PropTypes.string,
    className: PropTypes.string,
    style: PropTypes.object,
    target: PropTypes.func.isRequired,
    container: PropTypes.object,
    tooltipInnerStyle: PropTypes.object,
    tooltipArrowStyle: PropTypes.object,
    dataQa: PropTypes.string
  };

  static defaultProps = {
    type: 'status'
  };

  render() {
    const {
      children,
      className,
      id,
      style,
      placement,
      target,
      tooltipInnerStyle,
      container,
      dataQa
    } = this.props;
    const index = placement.indexOf('-');
    const styles = getTooltipStyles(this.props.type);
    const placementStyle = styles.placement[index >= 0 ? placement.substring(0, index) : placement];
    const finalStyle = { ...styles.base, ...style };

    return (
      <Overlay
        show={Boolean(target())}
        placement={placement}
        target={target}
        container={container}
        popperConfig={popperConfig}
      >
        {
          (overlayInfo) => {
            const { props: overlayProps, arrowProps } = overlayInfo;

            return (
              <div
                {...overlayProps}
                id={id} className={className}
                data-qa='tooltip'
                style={{
                  ...finalStyle,
                  ...overlayProps.style
                }}
              >
                <div style={placementStyle.tooltip}>
                  <div
                    {...arrowProps}
                    style={{
                      ...styles.arrow,
                      // arrowProps.style could contain styles like top: '', left: ''. That is why
                      // we need to apply placementStyle after arrowProps
                      ...arrowProps.style,
                      ...placementStyle.arrow
                    }}
                  />
                  <div data-qa={dataQa} style={{ ...styles.inner, ...tooltipInnerStyle }}>
                    {children}
                  </div>
                </div>
              </div>
            );
          }
        }
      </Overlay>
    );
  }
}

const popperConfig = {
  modifiers: {
    preventOverflow: {
      boundariesElement: 'viewport'
    }
  }
};
