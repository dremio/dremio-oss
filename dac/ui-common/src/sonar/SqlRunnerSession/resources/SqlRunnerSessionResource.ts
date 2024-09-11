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
import { SmartResource } from "smart-resource";
import { v4 as uuidv4 } from "uuid";
import { getSqlRunnerSession } from "../endpoints/getSqlRunnerSession";
import { createOptimisticResource } from "../../../utilities/createOptimisticResource";
import { closeSqlRunnerSessionTab } from "../endpoints/closeSqlRunnerSessionTab";
import { openSqlRunnerSessionTab } from "../endpoints/openSqlRunnerSessionTab";
import { NewScript, createScript } from "../../scripts/endpoints/createScript";
import { generateNewTabName } from "../utilities/generateNewTabName";
import { ScriptsResource } from "../../scripts/resources/ScriptsResource";
import { SQLRunnerSession } from "../SqlRunnerSession.type";
import { sqlRunnerSessionLogger } from "../sqlRunnerSessionLogger";
import { BadRequestError } from "../../../errors/BadRequestError";
import { closeSqlRunnerSessionTabs } from "../endpoints/closeSqlRunnerSessionTabs";
import { addTemporaryPrefix } from "../utilities/temporaryTabs";

export const $SqlRunnerSession = createOptimisticResource(
  new SmartResource(async () => {
    const session = await getSqlRunnerSession();

    // Handle "invalid" empty sessions
    if (session.scriptIds.length !== 0) {
      return session;
    }

    await newTab();
    return getSqlRunnerSession();
  }),
);

const nextSelectedTab = (
  state: SQLRunnerSession,
  closingTabId: SQLRunnerSession["currentScriptId"],
): SQLRunnerSession["currentScriptId"] => {
  // Don't change selected tab if it's not the one being closed
  if (state.currentScriptId !== closingTabId) {
    return state.currentScriptId;
  }

  // Don't allow the tab to be closed if it's the last one in the list
  if (state.scriptIds.length <= 1) {
    return state.currentScriptId;
  }

  const closingTabIdx = state.scriptIds.indexOf(closingTabId);

  // If it's the last tab, return the second to last scriptId
  if (closingTabIdx === state.scriptIds.length - 1) {
    return state.scriptIds.at(-2)!;
  }

  // Otherwise return the last scriptId
  return state.scriptIds.at(-1)!;
};

// Get the new script ID after other SQLRunnerSession state has changed
const updateCurrentScript = (state: SQLRunnerSession): SQLRunnerSession => {
  // Don't change selected tab if it's still in the open tabs list
  if (state.scriptIds.includes(state.currentScriptId)) {
    return state;
  }

  // Return the last scriptId in the list
  return {
    ...state,
    currentScriptId: state.scriptIds.at(-1)!,
  };
};

export const closeTabs = async (ids: string[]) => {
  sqlRunnerSessionLogger.debug(`Closing tab ids`, ids);

  const simulateUpdatedState = (state: SQLRunnerSession) =>
    updateCurrentScript({
      ...state,
      scriptIds: state.scriptIds.filter((scriptId) => !ids.includes(scriptId)),
    });

  const removeUpdate =
    $SqlRunnerSession.addOptimisticUpdate(simulateUpdatedState);

  try {
    await closeSqlRunnerSessionTabs(ids);

    // Merge the updated state
    if (!$SqlRunnerSession.$source.value) {
      await $SqlRunnerSession.resource.fetch();
    } else {
      $SqlRunnerSession.$source.next(
        simulateUpdatedState($SqlRunnerSession.$source.value),
      );
    }
  } catch (e) {
    // TODO: show error
  } finally {
    removeUpdate();
  }
};

export const closeTab = async (id: string) => {
  sqlRunnerSessionLogger.debug(`Closing tab id ${id}`);
  // Optimistically render the tab as closed

  const simulateUpdatedState = (state: SQLRunnerSession) => ({
    ...state,
    ...state,
    scriptIds: state.scriptIds.filter((scriptId) => scriptId !== id),
    currentScriptId: nextSelectedTab(state, id),
  });
  const removeUpdate =
    $SqlRunnerSession.addOptimisticUpdate(simulateUpdatedState);

  try {
    await closeSqlRunnerSessionTab({ scriptId: id });

    // Merge the updated state
    if (!$SqlRunnerSession.$source.value) {
      await $SqlRunnerSession.resource.fetch();
    } else {
      $SqlRunnerSession.$source.next(
        simulateUpdatedState($SqlRunnerSession.$source.value),
      );
    }
  } catch (e) {
    // TODO: show error
  } finally {
    removeUpdate();
  }
};

export const selectTab = async (id: string) => {
  // Don't do anything if the tab is already selected
  if (
    $SqlRunnerSession.$merged.value &&
    id === $SqlRunnerSession.$merged.value.currentScriptId
  ) {
    sqlRunnerSessionLogger.debug(
      `Tried to selectTab on a tab that's already selected: tab ${id}`,
    );
    return;
  }

  sqlRunnerSessionLogger.debug(`Selecting tab ${id}`);

  // Optimistically show the requested tab as selected
  const removeUpdate = $SqlRunnerSession.addOptimisticUpdate((state) => ({
    ...state,
    scriptIds: state.scriptIds.includes(id)
      ? state.scriptIds
      : [...state.scriptIds, id],
    currentScriptId: id,
  }));

  try {
    const updatedSqlRunnerSession = await openSqlRunnerSessionTab({
      scriptId: id,
    });
    removeUpdate();

    // Update the state directly from response rather than an additional refetch on the resource
    $SqlRunnerSession.$source.next(updatedSqlRunnerSession);
  } catch (e) {
    // TODO: show error
    removeUpdate();
  }
};

export const newTab = async () => {
  // Fake/temporary ID that we use to render a pending tab
  const id = uuidv4();

  sqlRunnerSessionLogger.debug(
    `Rendering and selecting fake / temporary tab with id: ${id}`,
  );

  // Render a fake pending tab while we create the script and update the session in the background
  const removeUpdate = $SqlRunnerSession.addOptimisticUpdate((state) => ({
    ...state,
    scriptIds: [...state.scriptIds, id],
    currentScriptId: id,
  }));

  try {
    // Create the associated script first
    const script = await createScript({
      content: "",
      context: [],
      description: "",
      name: addTemporaryPrefix(generateNewTabName()),
    });

    sqlRunnerSessionLogger.debug(`Created script with id: ${script.id}`);

    // Remove the fake pending tab
    removeUpdate();

    // Update the SQL runner session and refresh scripts resource
    await Promise.all([selectTab(script.id), ScriptsResource.fetch()]);
  } catch (e) {
    // TODO: show error
    removeUpdate();
  }
};

export const newPopulatedTab = async (
  newScript: Omit<NewScript, "name"> | NewScript,
) => {
  // Fake/temporary ID that we use to render a pending tab
  const id = uuidv4();

  sqlRunnerSessionLogger.debug(
    `Rendering and selecting fake / temporary tab with id: ${id}`,
  );

  // Render a fake pending tab while we create the script and update the session in the background
  const removeUpdate = $SqlRunnerSession.addOptimisticUpdate((state) => ({
    ...state,
    scriptIds: [...state.scriptIds, id],
    currentScriptId: id,
  }));

  try {
    // Create the associated script first
    const script = await createScript({
      ...newScript,
      name: "name" in newScript ? newScript.name : generateNewTabName(),
    });

    sqlRunnerSessionLogger.debug(`Created script with id: ${script.id}`);

    // Remove the fake pending tab
    removeUpdate();

    // Update the SQL runner session and refresh scripts resource
    await Promise.all([selectTab(script.id), ScriptsResource.fetch()]);

    return script;
  } catch (e) {
    removeUpdate();

    if (
      e instanceof BadRequestError &&
      e.responseBody.errorMessage.includes(
        "Maximum scripts limit per user is reached.",
      )
    ) {
      throw new BadRequestError(e.res, {
        errors: [
          {
            code: "scripts:max_limit_reached",
            title: "You can't create more than 1000 scripts.", //Unused
          },
        ],
      } as any);
    } else {
      throw e;
    }
  }
};
