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
import { Component, Fragment } from 'react';

import PropTypes from 'prop-types';
import classNames from 'classnames';

import FormElement from 'components/Forms/FormElement';
import HoverHelp from 'components/HoverHelp';
import Art from '@app/components/Art';
import SourceIcon from 'components/Icon/SourceIcon';
import config from 'dyn-load/utils/config';

import { inlineHelp, sectionBody, sectionLabel } from 'uiTheme/less/forms.less';
import { flexColumnContainer } from 'uiTheme/less/layout.less';
import {
  elementLayoutFull,
  elementLayoutHalf,
  elementLayoutRow,
  elementLayoutRowFixed,
  elementsWithIcon,
  groupLayoutRow,
  linkContainer,
  sectionWithIcon
} from './FormSection.less';

export default class FormSection extends Component {

  static propTypes = {
    style: PropTypes.object,
    sectionConfig: PropTypes.object,
    fields: PropTypes.object,
    sectionLevel: PropTypes.number,
    disabled: PropTypes.bool
  };

  state = {
    collapsed: this.getCollapsetFromProps(this.props)
  };

  getCollapsetFromProps(props) {
    const {sectionConfig} = props;
    const sectionConfigJson = (sectionConfig) ? sectionConfig.getConfig() : {};
    return sectionConfigJson && sectionConfigJson.collapsible && sectionConfigJson.collapsible.initCollapsed;
  }

  toggleCollapse = () => {
    if (!this.getIsCollapsible()) return;

    this.setState({collapsed: !this.state.collapsed});
  };

  getIsCollapsible = () => {
    const {sectionConfig} = this.props;
    return sectionConfig && sectionConfig.getConfig().collapsible;
  };

  renderElements(sectionConfig, fields) {
    if (sectionConfig.getDirectElements().length) {
      const layout = sectionConfig.getConfig().layout;
      const isLayoutRow = layout && layout === 'row';
      const groupStyleClass = (isLayoutRow) ? groupLayoutRow : flexColumnContainer;

      return (
        <div className={groupStyleClass}>
          {
            sectionConfig.getDirectElements().map((elementConfig, index) => {
              const { size, visibilityControl} = elementConfig.getConfig();

              if (visibilityControl && config[visibilityControl.config] !== visibilityControl.showCondition) {
                return;
              }

              const isFixedSize = typeof size === 'number' && size > 0;
              const isHalfWidth = size === 'half';
              let style = null;
              if (isHalfWidth) {
                style = {flex: 'none'};
              }

              const fieldClass = classNames({
                [elementLayoutRow]: isLayoutRow && !isFixedSize,
                [elementLayoutRowFixed]: isLayoutRow && isFixedSize,
                [elementLayoutHalf]: !isFixedSize && isHalfWidth,
                [elementLayoutFull]: !isFixedSize && !isHalfWidth // full width by default
              });
              return (
                <div key={index} className={fieldClass} style={style}>
                  <FormElement key={index} fields={fields} disabled={this.props.disabled} elementConfig={elementConfig}/>
                </div>
              );
            })
          }
        </div>
      );
    }
    return '';
  }

  renderIconAndElements(sectionConfig, fields) {
    if (sectionConfig.getConfig().icon) {
      return (
        <div className={sectionWithIcon}>
          <SourceIcon src={sectionConfig.getConfig().icon}/>
          <div className={elementsWithIcon}>
            {this.renderElements(sectionConfig, fields)}
          </div>
        </div>
      );
    }
    return this.renderElements(sectionConfig, fields);
  }

  renderCollapser(sectionConfigJson) {
    if (!sectionConfigJson.collapsible) return null;

    const {collapsed} = this.state;
    const iconType = collapsed ? 'ArrowRight.svg' : 'ArrowDownSmall.svg';
    const iconAlt = collapsed ? 'Expand Section' : 'Collapse Section';

    return <div data-qa='section-toggle'>
      <Art src={iconType} alt={iconAlt} style={styles.iconStyle}/>
    </div>;
  }

  renderLink(linkConfig) {
    const label = linkConfig.label || 'Learn more ...';
    return (
      <div className={linkContainer}>
        <a href={linkConfig.url}
          title={label}
          target='_blank'>
          {label} <i className='fa fa-external-link'></i>
        </a>
      </div>
    );
  }

  render() {
    const { fields, sectionConfig, style, disabled, sectionLevel = 0 } = this.props;
    const sectionConfigJson = (sectionConfig) ? sectionConfig.getConfig() : {};
    const help = sectionConfigJson.help;
    const link = sectionConfigJson.link;

    let sectionLabelStyle = (sectionConfigJson.collapsible) ? styles.collapsibleLabel : null;
    // style overwrites for sub-section
    sectionLabelStyle = (sectionLevel) ? {...sectionLabelStyle, fontSize: '14px'} : sectionLabelStyle;
    const sectionBodyStyle = (sectionLevel) ? {marginBottom: 15} : null;

    return (
      <div className={sectionBody} style={{...style, ...sectionBodyStyle}}>
        {sectionConfigJson.name &&
          <div className={sectionLabel} style={sectionLabelStyle} onClick={this.toggleCollapse}>
            {this.renderCollapser(sectionConfigJson)}
            {sectionConfigJson.name}
            {sectionConfigJson.tooltip &&
            <HoverHelp content={sectionConfigJson.tooltip} />
            }
          </div>
        }
        {!this.state.collapsed && <Fragment>
          {help && help.position === 'top' &&
          <div className={inlineHelp}>{help.text}</div>
          }
          {link && link.position === 'top' && this.renderLink(link)}
          {sectionConfig.getSections().map((subsection, index) => (
            <FormSection
              fields={fields}
              key={index}
              disabled={disabled}
              sectionLevel={sectionLevel + 1}
              sectionConfig={subsection}/>
          ))
          }
          {this.renderIconAndElements(sectionConfig, fields)}
          {help && help.position !== 'top' &&
          <div className={inlineHelp}>{help.text}</div>
          }
          {link && link.position !== 'top' && this.renderLink(link)}
        </Fragment>
        }
      </div>
    );
  }

}

const styles = {
  iconStyle: {
    width: 26,
    height: 26
  },
  collapsibleLabel: {
    cursor: 'pointer',
    paddingTop: 5,
    borderTop: '1px solid rgba(0,0,0,0.1)'
  }
};
