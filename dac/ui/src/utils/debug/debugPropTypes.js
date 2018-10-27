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

/* throws an error if current property is specified. Usefule when you are removing some property from
   usage and would like to check, that this property is not provided for a component anymore
*/
import { Component } from 'react';
export const obsolete = (props, propName, componentName) => {
  const value = props[propName];
  if (value !== null && value !== undefined) {
    return new Error(
      `'${propName}' is obsolete for component '${componentName}'. It should not be provided.
You should check a usage of '${componentName}'`
    );
  }
};

/*
Creates HOC that throw an error if property that is not defined in Component.propTypes is provided
*/
export const onlyDefinedProps = ComponentToWrap => {
  if (!ComponentToWrap.propTypes) {
    throw new Error(`You are trying to apply ${onlyDefinedProps.name} to ${ComponentToWrap.name},
which does not have propTypes defined`);
  }
  return class extends Component {
    componentDidMount() {
      this.checkProps(this.props);
    }

    componentWillReceiveProps(newProps) {
      this.checkProps(newProps);
    }

    checkProps(newProps) {
      const propTypes = ComponentToWrap.propTypes;
      const errorProps = [];

      if (!newProps || !propTypes) return;

      for (const prop in newProps) {
        if (newProps.hasOwnProperty(prop) && newProps[prop] && !propTypes.hasOwnProperty(prop)) {
          errorProps.push(prop);
        }
      }
      if (errorProps.length > 0) {
        throw new Error(`Properties that are not define in propTypes are provided to ${ComponentToWrap.name}.
Property list: '${errorProps.join('\', \'')}'`);
      }
    }

    render() {
      return <ComponentToWrap {...this.props} />;
    }
};
};
