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
import FormUtils from 'utils/FormUtils/FormUtils';
import { inlineHelp, elementContainer } from 'uiTheme/less/forms.less';

export default class FormElement extends Component {

  static propTypes = {
    style: PropTypes.object,
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    disabled: PropTypes.bool
  };

  static contextTypes = {
    editing: PropTypes.bool
  };


  renderElement(elementConfig, fields) {
    if (!elementConfig.getRenderer) return;

    const field = FormUtils.getFieldByComplexPropName(fields, elementConfig.getPropName());
    const Renderer = elementConfig.getRenderer();
    return (
      <Renderer
        elementConfig={elementConfig}
        fields={fields}
        field={field}
        editing={this.context.editing} disabled={this.props.disabled}/>
    );
  }

  render() {
    const {elementConfig, fields, style} = this.props;
    const help = elementConfig.getConfig().help;
    const propName = elementConfig.getConfig().propName;

    return (
      <div className={elementContainer} style={style} data-qa={propName}>
        {help &&
        help.position === 'top' && <div className={inlineHelp}>{help.text}</div>
        }
        {this.renderElement(elementConfig, fields)}
        {help && help.position !== 'top' &&
          <div className={inlineHelp}>{help.text}</div>
        }
      </div>
    );
  }
}
