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
import Art from '@app/components/Art';
import {
  title as titleInnerCls,
  section as sectionCls,
  buttonContainer,
  customButton
} from './SectionTitle.less';

export const getIconButtonConfig = ({
  key, // string,
  altText, // string
  icon, //string. SVG icon names
  onClick, // () => void
  dataQa // string
}) => {
  const style = {
    display: 'block',
    width: 24,
    height: 24
  };
  return {
    key,
    text: <Art src={`${icon}.svg`} alt={altText} style={style} title />,
    onClick,
    dataQa
  };
};

export class SectionTitle extends Component {
  static propTypes = {
    title: PropTypes.oneOfType([PropTypes.string, PropTypes.element]),
    className: PropTypes.string,
    titleClass: PropTypes.string,
    buttons: PropTypes.arrayOf(PropTypes.shape({
      key: PropTypes.string.isRequired,
      text: PropTypes.node.isRequired,
      onClick: PropTypes.func.isRequired,
      dataQa: PropTypes.string
    }))
  }

  createButton = (/* button */ {
    key,
    text,
    onClick,
    dataQa
  }) => {
    return <button
      key={key}
      className={customButton}
      onClick={onClick}
      data-qa={dataQa}>
      {text}
    </button>;
  }

  render() {
    const {
      title,
      className,
      titleClass,
      buttons
    } = this.props;

    return (<div key='title' className={classNames(sectionCls, className)}>
      <div className={classNames(titleInnerCls, titleClass)}>
        {title}
      </div>
      {
        buttons &&
        <div key='buttons' className={buttonContainer}>
          {buttons.map(this.createButton)}
        </div>
      }
    </div>);
  }
}
