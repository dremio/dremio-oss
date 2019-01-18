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
import { Component } from 'react';

import PropTypes from 'prop-types';
import classNames from 'classnames';

import FormElement from 'components/Forms/FormElement';
import HoverHelp from 'components/HoverHelp';
import SourceIcon from 'components/Icon/SourceIcon';

import { sectionLabel, sectionBody, inlineHelp } from 'uiTheme/less/forms.less';
import { flexColumnContainer } from 'uiTheme/less/layout.less';
import {
  sectionWithIcon,
  elementsWithIcon,
  linkContainer,
  groupLayoutRow,
  elementLayoutRow,
  elementLayoutHalf,
  elementLayoutFull
} from './FormSection.less';

export default class FormSection extends Component {

  static propTypes = {
    style: PropTypes.object,
    sectionConfig: PropTypes.object,
    fields: PropTypes.object,
    sectionLevel: PropTypes.number,
    disabled: PropTypes.bool
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
              const fieldClass = (elementConfig.getConfig().size === 'half')
                ? classNames(elementLayoutHalf, isLayoutRow && elementLayoutRow)
                : classNames(elementLayoutFull, isLayoutRow && elementLayoutRow);
              return (
                <div key={index} className={fieldClass}>
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

    // style overwrites for sub-section
    const sectionLabelStyle = (sectionLevel) ? {fontSize: '14px'} : null;
    const sectionBodyStyle = (sectionLevel) ? {marginBottom: 15} : null;

    return (
      <div className={sectionBody} style={{...style, ...sectionBodyStyle}}>
        {sectionConfigJson.name &&
          <div className={sectionLabel} style={sectionLabelStyle}>
            {sectionConfigJson.name}
            {sectionConfigJson.tooltip &&
            <HoverHelp content={sectionConfigJson.tooltip} />
            }
          </div>
        }
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
      </div>
    );
  }

}
