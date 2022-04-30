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

import { useMemo, useState, useRef } from 'react';
import { connect } from 'react-redux';
import classNames from 'classnames';
import { cloneDeep } from 'lodash';
// @ts-ignore
import moment from 'moment';
import { injectIntl } from 'react-intl';
// @ts-ignore
import { withRouter } from 'react-router';
import { Divider } from '@material-ui/core';

import getIconColor from '@app/utils/getIconColor';
import { showConfirmationDialog } from 'actions/confirmation';
import { getScripts, getActiveScript } from '@app/selectors/scripts';
import { getExploreState } from '@app/selectors/explore';
import { fetchScripts, updateScript, deleteScript, setActiveScript } from '@app/actions/resources/scripts';
import getUserIconInitials from '@app/utils/userIcon';
import Menu from '../Menus/Menu';
import MenuItem from '../Menus/MenuItem';
import SettingsBtn from '../Buttons/SettingsBtn';
import Art from '../Art';
import DropdownMenu from '../Menus/DropdownMenu';
import SearchDatasetsComponent from '../DatasetList/SearchDatasetsComponent';
import {
  SCRIPT_SORT_MENU,
  DATETIME_FORMAT,
  handleDeleteScript,
  handleOpenScript
} from './sqlScriptsUtils';
import SQLScriptDialog from './components/SQLScriptDialog/SQLScriptDialog';

import './SQLScripts.less';

export type SQLScriptsProps = {
  scripts: any[];
  user: any;
  activeScript: any;
  currentSql: string | null;
  intl: any;
  router: any;
  fetchSQLScripts: () => void;
  updateSQLScript: (payload: any, scriptId?: string) => void;
  deleteScript: (scriptId: string) => any;
  setActiveScript: (script: any) => void;
  showConfirmationDialog: (content: any) => void;
};

function SQLScripts(props: SQLScriptsProps): React.ReactElement {
  const {
    scripts = [],
    activeScript,
    fetchSQLScripts,
    updateSQLScript,
    user,
    intl
  } = props;

  const inputRef = useRef();
  const [sort, setSort] = useState(SCRIPT_SORT_MENU[3]);
  const [search, setSearch] = useState('');
  const [scriptForRename, setScriptForRename] = useState<any>({});

  // Create my-scripts lists
  const myScripts = useMemo(() => {
    const tempMyScripts = cloneDeep(scripts).map((script: any) => {
      return ({
        ...script,
        colors: getIconColor(script.createdBy),
        userNameFirst2: getUserIconInitials(user)
      });
    });

    return tempMyScripts;
  }, [scripts, user]);

  // Filter and sort based on the active list
  const activeScripts = useMemo(() => {
    let tempScripts = myScripts;

    if (search !== '') {
      tempScripts = tempScripts.filter((script) => script.name.toLowerCase().includes(search.toLowerCase()));
    }

    tempScripts = tempScripts.sort(sort && sort.compare);

    return tempScripts;
  }, [myScripts, sort, search]);

  const onInputRef = (input: any): void => {
    inputRef.current = input;
  };

  const clearSearch = (): void => {
    (inputRef.current as any).value = '';
    setSearch('');
  };

  const handlePostSubmit = (payload: any) => {
    if (scriptForRename.id === activeScript.id) {
      props.setActiveScript({ script: payload });
    }
    fetchSQLScripts();
  };

  const SCRIPT_ACTIONS = [
    { label: intl.formatMessage({ id: 'Common.Open' }), onClick: handleOpenScript },
    { label: intl.formatMessage({ id: 'Common.Rename' }), onClick: (_: SQLScriptsProps, script: any) => setScriptForRename(script) },
    { label: intl.formatMessage({ id: 'Common.Delete' }), onClick: handleDeleteScript, className: '--delete' }
  ];

  const ScriptActionsMenu = (menuProps: any): React.ReactElement => {
    const handleClick = (scriptAction: any): void => {
      scriptAction.onClick && scriptAction.onClick(props, menuProps.script);
      menuProps.closeMenu();
    };

    return (
      <Menu>
        {SCRIPT_ACTIONS.map((s) =>
          <MenuItem
            key={s.label}
            onClick={(e: any): void => {
              e.stopPropagation();
              handleClick(s);
            }}
            classname={s.className ? s.className : ''}
          >{s.label}</MenuItem>
        )}
      </Menu>
    );
  };

  const ScriptSortMenu = (menuProps: any): React.ReactElement => {
    const handleClick = (selectedSort: any): void => {
      setSort(selectedSort);
      menuProps.closeMenu();
    };

    return (
      <Menu>
        {SCRIPT_SORT_MENU.map((s) => {
          return s === null ? (
            <Divider className='custom-menu-divider' />
          ) : (
            <MenuItem
              key={`${s.category}-${s.dir}`}
              classname={`${s === sort ? '--selected' : ''}`}
              onClick={(): void => handleClick(s)}
            >
              {s.category}<Art src='SortArrow.svg' alt='SortArrow' className={`--sortArrow --${s.dir}`}/>
            </MenuItem>
          );
        })}
      </Menu>
    );
  };

  const RenderScripts = (
    !activeScripts.length ? (
      <span className='sqlScripts__empty'>{intl.formatMessage({ id: 'Script.NoneFound' })}</span>
    ) : (
      <Menu>
        {activeScripts.map((script) => (
          <MenuItem
            key={script.id}
            classname={`sqlScripts__menu-item ${script.id === activeScript.id ? '--selected' : ''}`}
            onClick={(): void => handleOpenScript(props, script)}
          >
            <>
              <div
                className={classNames('sideNav__user sideNav-item__dropdownIcon', '--narrow')}
                style={{
                  backgroundColor: script.colors.backgroundColor,
                  color: script.colors.color,
                  borderRadius: '50%'
                }}
              >
                <span>{script.userNameFirst2}</span>
              </div>
              <div className='sqlScripts__menu-item__content'>
                <div className='scriptName'>{script.name}</div>
                <div className='scriptCreator'>{moment(script.modifiedAt).format(DATETIME_FORMAT)}</div>
              </div>
              <SettingsBtn
                classStr='sqlScripts__menu-item__actions'
                menu={<ScriptActionsMenu script={script} />}
                hideArrowIcon
                stopPropagation
              >
                <Art src='More.svg' alt={intl.formatMessage({ id: 'Common.More' })} />
              </SettingsBtn>
            </>
          </MenuItem>
        ))}
      </Menu>
    )
  );

  return (
    <div className='sqlScripts' data-qa='sqlScripts'>
      <div className='sqlScripts__subHeading'>
        <SearchDatasetsComponent
          onInput={(e: any): void => setSearch((e.target as any).value)}
          clearFilter={clearSearch}
          closeVisible={search !== ''}
          onInputRef={onInputRef}
          placeholderText={intl.formatMessage({ id: 'Script.Search' })}
          dataQa='sqlScripts__search'
        />
        <DropdownMenu
          className='sqlScripts__subHeading__sortMenu'
          text={sort ? sort.category : ''}
          disabled={scripts.length === 0}
          hideArrow
          customImage={<Art src='SortArrow.svg' alt='SortArrow' className={`--sortArrow --${sort ? sort.dir : ''}`}/>}
          menu={<ScriptSortMenu />}
          listClass='sqlScripts__subHeading__sortMenu__dropdown'
        />
      </div>
      {RenderScripts}
      {scriptForRename && scriptForRename.id &&
        <SQLScriptDialog
          title={intl.formatMessage({ id: 'Script.Rename' })}
          script={scriptForRename}
          isOpen={!!scriptForRename.id}
          onCancel={() => setScriptForRename({})}
          onSubmit={updateSQLScript}
          postSubmit={handlePostSubmit}
          hideFail
        />
      }
    </div>
  );
}

const mapStateToProps = (state: any): any => {
  const explorePageState = getExploreState(state);
  return {
    user: state.account.get('user'),
    scripts: getScripts(state),
    activeScript: getActiveScript(state),
    currentSql: explorePageState ? explorePageState.view.currentSql : null
  } as any;
};

const reduxActions = {
  showConfirmationDialog,
  fetchSQLScripts: fetchScripts,
  updateSQLScript: updateScript,
  deleteScript,
  setActiveScript
};

export const TestSqlScripts = injectIntl(SQLScripts);

// @ts-ignore
export default withRouter(connect(mapStateToProps, reduxActions)(TestSqlScripts));
