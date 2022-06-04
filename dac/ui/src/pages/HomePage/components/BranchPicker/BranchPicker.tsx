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
import { useEffect, useImperativeHandle, useRef, useState } from 'react';
import { connect } from 'react-redux';
import { withRouter } from 'react-router';
import { oc } from 'ts-optchain';
import {
  usePopupState,
  bindToggle,
  bindPopper
} from 'material-ui-popup-state/hooks';
import { ClickAwayListener } from '@material-ui/core';
import Popover from '@material-ui/core/Popover';
import { FormattedMessage } from 'react-intl';
import DatePicker, { ReactDatePicker } from 'react-datepicker';

import {
  fetchCommitBeforeTime as fetchCommitBeforeTimeAction,
  setReference as setReferenceAction
} from '@app/actions/nessie/nessie';
import { LogEntry, Reference } from '@app/services/nessie/client';
import Radio from '@material-ui/core/Radio';
import RadioGroup from '@material-ui/core/RadioGroup';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import FormControl from '@material-ui/core/FormControl';
import { SearchField } from '@app/components/Fields';
import { useNessieContext } from '@app/pages/NessieHomePage/utils/context';
import BranchList from './components/BranchList/BranchList';
import CommitBrowser from './components/CommitBrowser/CommitBrowser';
import BranchPickerTag from './components/BranchPickerTag/BranchPickerTag';
import CommitHash from './components/CommitBrowser/components/CommitHash/CommitHash';
import { useBranchPickerContext } from './utils';

import './BranchPicker.less';

const defaultPosition: any = {
  anchorOrigin: { horizontal: 'right', vertical: 'bottom' },
  transformOrigin: { horizontal: 'left', vertical: 'center' }
};

type ConnectedProps = {
  setReference: typeof setReferenceAction;
  fetchCommitBeforeTime: typeof fetchCommitBeforeTimeAction;
};

type BranchPickerProps = {
  redirectOnChange?: boolean;
  anchorEl?: any;
  position?: any;
};

function BranchPicker({
  router,
  redirectOnChange,
  setReference,
  fetchCommitBeforeTime,
  anchorEl,
  position = defaultPosition
}: BranchPickerProps & ConnectedProps & { router?: any }) {
  const { ref } = useBranchPickerContext();
  const { state, api, baseUrl, stateKey } = useNessieContext();
  const { reference, defaultReference, hash, date } = state;
  const popupState = usePopupState({
    variant: 'popover',
    popupId: 'branchPickerPopover'
  });
  const toggleProps = bindToggle(popupState);

  useImperativeHandle(ref, () => popupState);

  function goToHome() {
    if (redirectOnChange) router.push(baseUrl);
  }

  function changeReference(newReference?: Reference | null) {
    if (!newReference) return;
    setReference({ reference: newReference }, stateKey);
    goToHome();
  }

  function setHash(logEntry: LogEntry) {
    const newHash = oc(logEntry.commitMeta).hash();
    if (newHash) {
      setReference({ reference, hash: newHash }, stateKey);
      goToHome();
    }
  }

  const [formVal, setFormVal] = useState('head');
  const pickerRef = useRef<ReactDatePicker>(null);

  function showPicker() {
    const picker = pickerRef.current;
    if (date || !picker) return;
    if (!picker.isCalendarOpen()) picker.setOpen(true);
  }

  function onFormChange(value: string) {
    setFormVal(value);
    if (value === 'head') {
      changeReference(reference);
    }
  }

  async function onChooseDate(newDate: any) {
    if (!newDate) return; //TODO May need this to clear value
    fetchCommitBeforeTime(reference, newDate, stateKey, api, redirectOnChange);
  }

  useEffect(() => {
    if (!popupState.isOpen) return;
    if (!hash) setFormVal('head');
  }, [popupState.isOpen, hash]);

  if (!reference) return null; //Loading

  return (
    <>
      <div
        className='branchPicker'
        {...toggleProps}
        onClick={(e: any) => {
          e.stopPropagation();
          toggleProps.onClick(e);
        }}
      >
        <BranchPickerTag
          reference={reference}
          hash={hash}
          isOpen={popupState.isOpen}
        />
      </div>
      {reference && defaultReference && (
        <Popover
          {...bindPopper(popupState)}
          transitionDuration={200}
          {...position}
          {...(anchorEl && popupState.isOpen && { anchorEl })}
        >
          <ClickAwayListener onClickAway={popupState.close}>
            <div
              className='branchPicker-popup'
              onClick={(e: any) => {
                e.stopPropagation();
              }}>
              <div className='branchPicker-popup-content'>
                <div className='branchesView'>
                  <div className='branchesView-left'>
                    <BranchList
                      defaultReference={defaultReference}
                      currentReference={reference}
                      onClick={changeReference}
                    />
                  </div>
                  <div className='branchesView-right'>
                    <div className='branchPicker-popup-header'>
                      <span
                        className='branchPicker-popup-header-name text-ellipsis'
                        title={reference.name}
                      >
                        {reference.name}
                      </span>
                      {hash && (
                        <span className='branchPicker-popup-header-hashWrapper'>
                          <span className='branchPicker-popup-header-hashPrefix'>
                            @
                          </span>
                          <CommitHash
                            branch={reference.name}
                            hash={hash}
                            disabled
                          />
                        </span>
                      )}
                    </div>
                    {!!reference && (
                      <FormControl
                        component='fieldset'
                        className='branchesView-radios'
                      >
                        <RadioGroup
                          value={formVal}
                          onChange={({ target: { value } }: any) => {
                            onFormChange(value);
                          }}
                        >
                          <FormControlLabel
                            className='branchesView-radioLabel'
                            value='head'
                            control={<Radio />}
                            label='Head'
                          />
                          <FormControlLabel
                            onClick={showPicker}
                            className='branchesView-radioLabel'
                            value='date'
                            control={<Radio />}
                            label={
                              <div className='branchesView-datePicker'>
                                <FormattedMessage id='BranchPicker.ChooseTime' />
                                <DatePicker
                                  ref={pickerRef}
                                  disabled={formVal !== 'date'}
                                  showTimeSelect
                                  selected={date}
                                  onChange={onChooseDate}
                                  popperPlacement='bottom-start'
                                  customInput={<SearchField showIcon={false} />}
                                  dateFormat='M/d/yyyy hh:mm aa'
                                />
                              </div>
                            }
                          />
                          <FormControlLabel
                            className='branchesView-radioLabel branchesView-commitBrowserContainer'
                            value='commit'
                            control={<Radio />}
                            label={
                              <>
                                <span className='branchesView-commitLabel'>
                                  <FormattedMessage id='Common.Commit' />
                                </span>
                                <CommitBrowser
                                  disabled={formVal !== 'commit'}
                                  key={reference.name}
                                  branch={reference}
                                  onClick={setHash}
                                  selectedHash={hash}
                                  api={api}
                                />
                              </>
                            }
                          />
                        </RadioGroup>
                      </FormControl>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </ClickAwayListener>
        </Popover>
      )}
    </>
  );
}

const mapDispatchToProps = {
  setReference: setReferenceAction,
  fetchCommitBeforeTime: fetchCommitBeforeTimeAction
};

export default withRouter(
  //@ts-ignore
  connect(null, mapDispatchToProps)(BranchPicker)
);
