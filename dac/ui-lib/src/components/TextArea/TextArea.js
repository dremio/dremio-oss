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
import React, { useState } from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import get from 'lodash.get';

import { CopyToClipboard } from 'react-copy-to-clipboard';
import SvgIcon from '@material-ui/core/SvgIcon';
import TextareaAutosize from '@material-ui/core/TextareaAutosize';
import Tooltip from '@material-ui/core/Tooltip';
import Label from '../Label';
import { ReactComponent as CopyIcon } from '../../art/copy.svg';

import './textArea.scss';

const TextArea = (props) => {
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const [tooltipTitle, setTooltipTitle] = useState('');
  const {
    classes,
    defaultValue,
    disabled,
    disableTextCopy,
    enableCopy,
    helpText,
    label,
    minLines,
    maxLines,
    name,
    noResize,
    onChange,
    onCopy,
    value,
    form: {
      errors,
      touched
    },
    ...otherProps
  } = props;

  const hasError = get(touched, name) && get(errors, name);

  const rootClass = clsx('textAreaRoot', { [classes.root]: classes.root });
  const labelClass = clsx('textAreaRoot__label', { [classes.label]: classes.label });
  const textAreaContainerClass = clsx('textAreaRoot__container', { [classes.container]: classes.container });
  const textClass = clsx(
    { '--noResize': noResize },
    { '--singleLine': ( maxLines === 1 ) },
    { '--error': hasError }
  );

  const handleCopySuccess = () => {
    if (onCopy && typeof onCopy === 'function') {
      onCopy();
    }
    setTooltipOpen(true);
    setTooltipTitle('Copied');
    setTimeout(() => {
      setTooltipOpen(false);
    }, 1000);
  };

  const handleMouseEnter = () => {
    document.body.classList.add('disable-back-scroll');
  };

  const handleMouseLeave = () => {
    document.body.classList.remove('disable-back-scroll');
  };

  const handleTextAreaCopy = (e) => {
    if (disableTextCopy) {
      e.preventDefault();
    }
  };

  return (
    <div className={rootClass}>
      {label && (
        <div className='textAreaRoot__labelContainer'>
          <Label
            value={label}
            className={labelClass}
            helpText={helpText}
            id={`textbox-label-${name}`}
          />
          { enableCopy && (
            <Tooltip
              arrow
              open={tooltipOpen}
              title={tooltipTitle}
              placement='bottom'
            >
              <span className='textAreaRoot__icon'>
                <CopyToClipboard text={value}
                  onCopy={handleCopySuccess}>
                  <SvgIcon component={CopyIcon} fontSize='small' />
                </CopyToClipboard>
              </span>
            </Tooltip>
          )}
        </div>
      )}

      <div className={textAreaContainerClass}>
        <TextareaAutosize
          defaultValue={defaultValue}
          value={value}
          disabled={disabled}
          className={textClass}
          rowsMin={minLines}
          rowsMax={minLines > maxLines ? minLines : maxLines}
          aria-labelledby={`textbox-label-${name}`}
          onChange={onChange}
          name={name}
          onPointerEnter={handleMouseEnter}
          onMouseEnter={handleMouseEnter}
          onMouseLeave={handleMouseLeave}
          onCopy={handleTextAreaCopy}
          {...otherProps}
        />
        { !label && enableCopy && (
          <Tooltip
            arrow
            open={!label && tooltipOpen}
            title={tooltipTitle}
            placement='bottom'
          >
            <span className='textAreaRoot__icon'>
              <CopyToClipboard text={value}
                onCopy={handleCopySuccess}>
                <SvgIcon component={CopyIcon} fontSize='small' />
              </CopyToClipboard>
            </span>
          </Tooltip>

        )}
      </div>
    </div>
  );
};

TextArea.propTypes = {
  classes: PropTypes.shape({
    root: PropTypes.string,
    label: PropTypes.string,
    container: PropTypes.string
  }),
  value: PropTypes.string,
  disabled: PropTypes.bool,
  form: PropTypes.object,
  disableTextCopy: PropTypes.bool,
  enableCopy: PropTypes.bool,
  label: PropTypes.string,
  minLines: PropTypes.number,
  maxLines: PropTypes.number,
  name: PropTypes.string,
  noResize: PropTypes.bool,
  onChange: PropTypes.func,
  onCopy: PropTypes.func,
  defaultValue: PropTypes.string,
  helpText: PropTypes.string
};

TextArea.defaultProps = {
  classes: {},
  form: {},
  disabled: false,
  disableTextCopy: false,
  enableCopy: false,
  label: null,
  noResize: false
};

export default TextArea;
