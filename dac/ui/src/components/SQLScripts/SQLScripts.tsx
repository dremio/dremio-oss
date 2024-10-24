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

import { useMemo, useState, useRef, useEffect, MutableRefObject } from "react";
import { connect } from "react-redux";
import classNames from "clsx";
import { debounce } from "lodash";
import moment from "#oss/utils/dayjs";
import { injectIntl } from "react-intl";
import { withRouter } from "react-router";
import { useResourceSnapshot } from "smart-resource/react";
import TextHighlight from "../TextHighlight";
import SubHeaderTabs from "#oss/components/SubHeaderTabs";
import SortDropDownMenu from "#oss/components/SortDropDownMenu";
import { showConfirmationDialog } from "actions/confirmation";
import {
  getScripts,
  getMineScripts,
  getActiveScript,
} from "#oss/selectors/scripts";
import { getExploreState } from "#oss/selectors/explore";
import {
  fetchScripts,
  deleteScript,
  setActiveScript,
} from "#oss/actions/resources/scripts";
import {
  Avatar,
  ModalContainer,
  useModalContainer,
} from "dremio-ui-lib/components";
import { usePrevious } from "#oss/utils/jobsUtils";
import { resetQueryState } from "#oss/actions/explore/view";

import { loadPrivilegesListData } from "#oss/actions/resources/privilegesModalApiActions";
import Menu, { styles } from "../Menus/Menu";
import MenuItem from "../Menus/MenuItem";
import SettingsBtn from "../Buttons/SettingsBtn";
import SearchDatasetsComponent from "../DatasetList/SearchDatasetsComponent";
import {
  SCRIPT_SORT_MENU,
  DATETIME_FORMAT,
  handleDeleteScript,
  handleOpenScript as baseHandleOpenScript,
  ALL_MINE_SCRIPTS_TABS,
  filterAndSortScripts,
  INITIAL_CALL_VALUE,
  SEARCH_CALL_VALUE,
  openPrivilegesModalForScript,
  handleOpenTabScript,
} from "./sqlScriptsUtils";
import { $SqlRunnerSession } from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";
import localStorageUtils from "#oss/utils/storageUtils/localStorageUtils";

import SQLScriptRenameDialog from "./components/SQLScriptRenameDialog/SQLScriptRenameDialog";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";
import "./SQLScripts.less";
import { useSqlRunnerSession } from "dremio-ui-common/sonar/SqlRunnerSession/providers/useSqlRunnerSession.js";
import {
  newTab,
  closeTabs,
  selectTab,
} from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";

import { useMultiTabIsEnabled } from "./useMultiTabIsEnabled";
import {
  ScriptCheckbox,
  ScriptSelectAllCheckbox,
  useSqlScriptsMultiSelect,
} from "./components/SqlScriptsMultiSelect/SqlScriptsMultiSelect";
import { nameToInitials } from "#oss/exports/utilities/nameToInitials";
import { SQLScriptsBulkDeleteDialog } from "./components/SQLScriptsBulkDeleteDialog/SQLScriptsBulkDeleteDialog";
import { v4 as uuidv4 } from "uuid";
import { useSupportFlag } from "#oss/exports/endpoints/SupportFlags/getSupportFlag";
import { ENABLED_SCRIPTS_API_V3 } from "#oss/exports/endpoints/SupportFlags/supportFlagConstants";
import { deleteQuerySelectionsFromStorage } from "#oss/sagas/utils/querySelections";

export const VIEW_ID = "ScriptsPrivileges";
const CE_PERMISSIONS = ["VIEW", "MODIFY", "DELETE"];

const getPermissions = (script: any) => {
  return script.permissions ?? CE_PERMISSIONS;
};

export type SQLScriptsProps = {
  scripts: any[];
  mineScripts: any[];
  user: any;
  activeScript: any;
  currentSql: string | null;
  intl: any;
  router: any;
  location: any;
  fetchSQLScripts: (arg: {
    maxResults: number;
    searchTerm: string | null;
    createdBy: string | null;
  }) => Promise<any>;
  updateSQLScript: (payload: any, scriptId?: string) => void;
  deleteScript: (scriptId: string) => any;
  showConfirmationDialog: (content: any) => void;
  resetQueryState: (exclude?: any) => void;
  dispatchLoadPrivilegesListData: (fullPathList: any[], viewId: string) => void;
};

function SQLScripts(props: SQLScriptsProps): React.ReactElement {
  const {
    scripts: allScripts = [],
    mineScripts: myScripts,
    activeScript,
    fetchSQLScripts,
    user,
    router,
    location,
    intl,
  } = props;
  const [allScriptsResults] = useResourceSnapshot(ScriptsResource);
  const isMultiTabEnabled = useMultiTabIsEnabled();
  const inputRef = useRef<any>();
  const focusRef = useRef<any>();
  const [fetchKey, setFetchKey] = useState(() => uuidv4());
  const forceRefetchScripts = () => {
    setFetchKey(uuidv4());
  };
  const [sort, setSort] = useState(SCRIPT_SORT_MENU[3]);
  const [search, setSearch] = useState("");
  const [scriptForRename, setScriptForRename] = useState<any>({});
  const [selectedScriptsTab, setSelectedScriptsTab] = useState(
    ALL_MINE_SCRIPTS_TABS.all,
  );
  const scriptIdFromUrl = location?.query?.scriptId;
  const curScriptRef = useRef<any>(null);
  const [hasScriptsV3Api] = useSupportFlag(ENABLED_SCRIPTS_API_V3);

  useEffect(() => {
    if (!curScriptRef.current) return;
    curScriptRef.current.scrollIntoView();
  }, []);

  const handleOpenScript = baseHandleOpenScript({
    sqlRunnerSession: useSqlRunnerSession(),
    multiTabEnabled: isMultiTabEnabled,
  });

  const prevSearch = usePrevious(search);
  const prevFetchKey = usePrevious(fetchKey);

  const onFocus = () => {
    if (focusRef?.current)
      focusRef.current.className = "searchDatasetsPopover --focused";
  };

  const onBlur = () => {
    if (focusRef?.current) focusRef.current.className = "searchDatasetsPopover";
  };

  const onFocusRef = (div: MutableRefObject<any>) => {
    focusRef.current = div;
  };

  // -- Filter and sort based on the active list --
  // All: FE and BE filter for correct display number and correctly filtered list
  // Mine: FE filter only because only a max of 100 scripts possible
  const [allScriptsWithFilter, myScriptsWithFilter] = useMemo(() => {
    const filteredMineScripts = filterAndSortScripts({
      list: myScripts,
      search: search,
      sort: sort,
    });
    return [
      filterAndSortScripts({ list: allScripts, search: search, sort: sort }),
      filteredMineScripts,
    ];
  }, [allScripts, myScripts, sort, search]);

  const numberedScriptsTabs = [
    intl.formatMessage({ id: "Resource.Tree.All" }) +
      ` (${allScriptsWithFilter && allScriptsWithFilter.length})`,
    intl.formatMessage({ id: "Resource.Tree.Scripts.Mine" }) +
      ` (${myScriptsWithFilter.length})`,
  ];

  const onInputRef = (input: any): void => {
    inputRef.current = input;
  };

  // BE search for All list: user could have 100+ scripts in ALl tab so BE search
  // is needed
  useEffect(() => {
    if (search === prevSearch && fetchKey === prevFetchKey) {
      return;
    }

    const hasSearchKey = search !== "";
    fetchSQLScripts({
      maxResults: hasSearchKey ? SEARCH_CALL_VALUE : INITIAL_CALL_VALUE,
      searchTerm: hasSearchKey ? search : null,
      createdBy: null,
    });

    // Fetch "Mine" tab scripts also
    if (fetchKey !== prevFetchKey) {
      fetchSQLScripts({
        maxResults: hasSearchKey ? SEARCH_CALL_VALUE : INITIAL_CALL_VALUE,
        searchTerm: hasSearchKey ? search : null,
        createdBy: user.get("userId"),
      });
    }
  }, [
    search,
    fetchSQLScripts,
    prevSearch,
    selectedScriptsTab,
    user,
    fetchKey,
    prevFetchKey,
  ]);

  const debouncedSearch = debounce(
    (e: any): void => setSearch((e.target as any).value),
    250,
  );

  const clearSearch = (): void => {
    if (inputRef?.current) inputRef.current.value = "";
    setSearch("");
  };

  const SCRIPT_ACTIONS = [
    {
      id: "VIEW",
      label: intl.formatMessage({ id: "Common.Open" }),
      onClick: handleOpenScript,
    },
    {
      id: "MODIFY",
      label: intl.formatMessage({ id: "Common.Rename" }),
      onClick: (_: SQLScriptsProps, script: any) => setScriptForRename(script),
    },
    {
      id: "MANAGE_GRANTS",
      label: intl.formatMessage({ id: "Common.Privileges" }),
      onClick: (_: SQLScriptsProps, script: any) => {
        openPrivilegesModalForScript({
          router,
          location,
          script,
          VIEW_ID,
        });
      },
    },
    {
      id: "DELETE",
      label: intl.formatMessage({ id: "Common.Delete" }),
      onClick: (...args: any) => {
        const nextScript = args.pop();
        handleDeleteScript(
          ...args,
          () => {
            nextScript && handleOpenScript(args[0], nextScript);
          },
          isMultiTabEnabled,
        );
      },
      disabled: isMultiTabEnabled && allScriptsResults?.value?.length === 1,
      className: "--delete",
    },
  ];

  const ScriptActionsMenu = (menuProps: any): React.ReactElement => {
    const {
      script: { permissions },
      script,
      nextScript,
      closeMenu,
    } = menuProps;
    const handleClick = (
      scriptAction: any,
      userId: string,
      searchTerm: string,
    ): void => {
      scriptAction.onClick(props, script, userId, searchTerm, nextScript);
      closeMenu();
    };

    return (
      <Menu>
        {SCRIPT_ACTIONS.reduce((iterator: any[], script: any) => {
          if ((permissions ?? CE_PERMISSIONS).includes(script.id)) {
            iterator.push(
              <MenuItem
                key={script.label}
                onClick={(e: any): void => {
                  e.stopPropagation();
                  handleClick(script, user.get("userId"), search);
                }}
                disabled={script?.disabled}
                className={script.className ? script.className : ""}
              >
                {script.label}
              </MenuItem>,
            );
          }
          return iterator;
        }, [])}
      </Menu>
    );
  };

  const currentScriptsList = selectedScriptsTab.startsWith(
    ALL_MINE_SCRIPTS_TABS.all,
  )
    ? allScriptsWithFilter
    : myScriptsWithFilter;

  const selectableScripts = useMemo(
    () =>
      currentScriptsList.filter((script) =>
        getPermissions(script).includes("DELETE"),
      ),
    [currentScriptsList],
  );

  const { onCheckboxClick, onAllClick, selectedScripts } =
    useSqlScriptsMultiSelect(selectableScripts);
  const RenderScripts = !currentScriptsList.length ? (
    <span className="sqlScripts__empty">
      {intl.formatMessage({ id: "Script.NoneFound" })}
    </span>
  ) : (
    <Menu style={{ ...styles.menuStyle, flex: 1 }}>
      {currentScriptsList.map((script, i) => (
        <MenuItem
          key={script.id}
          className={`sqlScripts__menu-item ${
            script.id === activeScript.id ? "--selected" : ""
          }`}
          onClick={(): void => handleOpenScript(props, script)}
          {...(scriptIdFromUrl &&
            scriptIdFromUrl === script.id && {
              setRef: (ref: any) => (curScriptRef.current = ref),
            })}
        >
          <div
            className={classNames("sqlScripts__menu-item__leftContent", {
              "--with-checkbox": hasScriptsV3Api,
            })}
          >
            {hasScriptsV3Api && (
              <ScriptCheckbox
                disabled={!getPermissions(script).includes("DELETE")}
                checked={selectedScripts.includes(script)}
                onClick={onCheckboxClick(script)}
              />
            )}
            <div
              className={classNames(
                "sideNav__user sideNav-item__dropdownIcon",
                "--narrow",
              )}
            >
              <Avatar
                initials={nameToInitials(
                  script.createdBy.name || script.createdBy.email,
                )}
              />
            </div>
            <div
              className={classNames("sqlScripts__menu-item__nameContent", {
                "--with-checkbox": hasScriptsV3Api,
              })}
            >
              <TextHighlight
                className="scriptName"
                text={script.name}
                inputValue={search}
                tooltipPlacement="top"
                tooltipEnterDelay={500}
                tooltipEnterNextDelay={500}
              />
              <div className="scriptCreator">
                {moment(script.modifiedAt).format(DATETIME_FORMAT)}
              </div>
            </div>
            <SettingsBtn
              classStr="sqlScripts__menu-item__actions"
              menu={
                <ScriptActionsMenu
                  script={script}
                  nextScript={
                    currentScriptsList?.[i + 1] || currentScriptsList?.[i + 1]
                  }
                />
              }
              hideArrowIcon
              stopPropagation
            >
              <dremio-icon
                name="interface/more"
                alt={intl.formatMessage({ id: "Common.More" })}
              />
            </SettingsBtn>
          </div>
        </MenuItem>
      ))}
    </Menu>
  );

  const bulkDeleteDialog = useModalContainer();

  return (
    <>
      <div
        className="sqlScripts flex flex-1 flex-col"
        style={{ minHeight: 0 }}
        data-qa="sqlScripts"
      >
        <div className="sqlScripts__subHeading">
          <SubHeaderTabs
            onClickFunc={setSelectedScriptsTab}
            tabArray={numberedScriptsTabs}
            selectedTab={selectedScriptsTab}
          />
          <SortDropDownMenu
            menuList={SCRIPT_SORT_MENU}
            sortValue={sort}
            disabled={allScripts.length === 0}
            setSortValue={setSort}
            selectClass={"sqlScripts__dropdown__disabled"}
          />
        </div>
        <SearchDatasetsComponent
          onInput={debouncedSearch}
          clearFilter={clearSearch}
          closeVisible={search !== ""}
          onInputRef={onInputRef}
          placeholderText={intl.formatMessage({ id: "Script.Search" })}
          dataQa="sqlScripts__search"
          onFocus={onFocus}
          onBlur={onBlur}
          onFocusRef={onFocusRef}
        />
        {RenderScripts}
        {selectedScripts.length > 0 && (
          <ScriptSelectAllCheckbox
            onCheckboxClick={onAllClick}
            onDeleteClick={bulkDeleteDialog.open}
            scripts={selectableScripts}
            selectedScripts={selectedScripts}
          />
        )}
        {scriptForRename && scriptForRename.id && (
          <SQLScriptRenameDialog
            script={scriptForRename}
            isOpen={!!scriptForRename.id}
            onCancel={() => setScriptForRename({})}
          />
        )}
      </div>
      <ModalContainer {...bulkDeleteDialog}>
        <SQLScriptsBulkDeleteDialog
          onCancel={bulkDeleteDialog.close}
          onSuccess={async (deletedIds) => {
            const curActiveScriptId =
              $SqlRunnerSession.$merged.value?.currentScriptId;
            let activeScriptDeleted = false;
            deletedIds.forEach((scriptId) => {
              deleteQuerySelectionsFromStorage(scriptId);

              if (scriptId === curActiveScriptId) {
                activeScriptDeleted = true;
              }
            });
            const remainingOpenTabIds =
              $SqlRunnerSession.$merged.value?.scriptIds.filter(
                (cur) => !deletedIds.includes(cur),
              ) || [];

            // All tabs closed, either create a new script or open an existing one
            let existingScriptToOpen = null;
            if (remainingOpenTabIds.length === 0) {
              await newTab(); // No more scripts, create a new one
            } else {
              // Or look for an existing script and create new tab if none after tabs closed
              // Set the current tab in SQL Runner session, backend will throw an error when closing the last one
              // Call handleOpenTabScript to push new route, load jobs etc after calling closeTabs
              existingScriptToOpen = ScriptsResource.getResource().value?.find(
                (script) => script.id === remainingOpenTabIds[0],
              );
              if (existingScriptToOpen) {
                await selectTab(existingScriptToOpen.id);
              }
            }

            await closeTabs(deletedIds);

            if (existingScriptToOpen) {
              handleOpenTabScript(router)(existingScriptToOpen);
            } else if (
              activeScriptDeleted ||
              remainingOpenTabIds.length === 0
            ) {
              handleOpenTabScript(router)(
                ScriptsResource.getResource().value?.find(
                  (script) =>
                    script.id ===
                    $SqlRunnerSession.$merged.value?.currentScriptId,
                ),
              );
            }

            ScriptsResource.fetch();
            forceRefetchScripts();
            bulkDeleteDialog.close();
          }}
          scripts={selectedScripts}
        />
      </ModalContainer>
    </>
  );
}

const mapStateToProps = (state: any, ownProps: any): any => {
  const { location } = ownProps;
  const explorePageState = getExploreState(state);
  return {
    location,
    user: state.account.get("user"),
    scripts: getScripts(state),
    mineScripts: getMineScripts(state),
    activeScript: getActiveScript(state),
    currentSql: explorePageState ? explorePageState.view.currentSql : null,
  } as any;
};

const reduxActions = {
  showConfirmationDialog,
  fetchSQLScripts: fetchScripts,
  deleteScript,
  resetQueryState,
  dispatchLoadPrivilegesListData: loadPrivilegesListData,
  setActiveScript,
};

export const TestSqlScripts = injectIntl(SQLScripts);

export default withRouter(
  connect(mapStateToProps, reduxActions)(TestSqlScripts),
);
