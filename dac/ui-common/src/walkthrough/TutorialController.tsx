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
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useReducer,
} from "react";
import { getTracingContext } from "../contexts/TracingContext";
import { WalkthroughStep } from "./WalkthroughStep/WalkthroughStep";
import { getCompletedTutorials, openTutorialMenu } from "./utilities/tutorials";
import { EndTutorialDialog } from "./EndTutorialDialog";
import { ModalContainer } from "dremio-ui-lib/components";
import { useModalContainer } from "dremio-ui-lib/components";

type TutorialState = {
  activeTutorialId: string | null;
  activeTutorialStep: number | null;
  currentOutletId: string | null;
  isTutorialHidden: boolean;
  wikiEditor: any;
};

type TutorialContext = TutorialState & {
  dispatch: any;
  nextStep: () => void;
  endTutorial: () => void;
  hideTutorial: (hide: boolean) => void;
  setWikiEditor: (editor: any) => void;
};

type TutorialActions =
  | { type: "set_outlet"; outletId: string | null }
  | { type: "next_step" }
  | { type: "end_tutorial" }
  | { type: "complete_tutorial" }
  | { type: "start_tutorial"; tutorialId: string }
  | { type: "hide_tutorial"; hide: boolean }
  | { type: "set_wiki_editor"; editor: any };

const activeTutorialReducer = (
  state: TutorialState["activeTutorialId"] = null,
  action: TutorialActions
): TutorialState["activeTutorialId"] => {
  const completedTutorials = getCompletedTutorials();
  switch (action.type) {
    case "start_tutorial":
      return action.tutorialId;
    case "end_tutorial":
      return null;
    case "complete_tutorial": {
      if (state) completedTutorials.push(state);
      localStorage.setItem(
        "completedTutorials",
        JSON.stringify(completedTutorials)
      );
      return null;
    }
    default:
      return state;
  }
};

const activeTutorialStepReducer = (
  state: TutorialState["activeTutorialStep"] = null,
  action: TutorialActions
): TutorialState["activeTutorialStep"] => {
  switch (action.type) {
    case "start_tutorial":
      return 0;
    case "end_tutorial":
      return null;
    case "next_step":
      return state! + 1;
    default:
      return state;
  }
};

const currentOutletReducer = (
  state: TutorialState["currentOutletId"] = null,
  action: TutorialActions
): TutorialState["currentOutletId"] => {
  switch (action.type) {
    case "set_outlet":
      return action.outletId;
    default:
      return state;
  }
};

const isTutorialHiddenReducer = (
  state: TutorialState["isTutorialHidden"] = localStorage.getItem?.(
    "isTutorialHidden"
  ) === "true",
  action: TutorialActions
): TutorialState["isTutorialHidden"] => {
  switch (action.type) {
    case "hide_tutorial": {
      localStorage.setItem("isTutorialHidden", `${action.hide}`);
      return action.hide;
    }
    default:
      return state;
  }
};

const wikiEditorReducer = (
  state: TutorialState["wikiEditor"] = null,
  action: TutorialActions
): TutorialState["wikiEditor"] => {
  switch (action.type) {
    case "set_wiki_editor": {
      return action.editor;
    }
    default:
      return state;
  }
};

const tutorialStateReducer = (
  state: TutorialState,
  action: TutorialActions
): TutorialState => {
  return {
    activeTutorialId: activeTutorialReducer(state.activeTutorialId, action),
    activeTutorialStep: activeTutorialStepReducer(
      state.activeTutorialStep,
      action
    ),
    currentOutletId: currentOutletReducer(state.currentOutletId, action),
    isTutorialHidden: isTutorialHiddenReducer(state.isTutorialHidden, action),
    wikiEditor: wikiEditorReducer(state.wikiEditor, action),
  };
};

export const tutorialContext = createContext<TutorialContext>(null as any);

const logTutorialEvents =
  (reducer: (state: TutorialState, action: TutorialActions) => TutorialState) =>
  (state: TutorialState, action: TutorialActions): TutorialState => {
    const { appEvent } = getTracingContext();
    switch (action.type) {
      case "start_tutorial":
        appEvent(`product-tutorials:${action.tutorialId}:started`);
        break;
      case "end_tutorial": {
        appEvent(`product-tutorials:${state.activeTutorialId}:ended`);
        openTutorialMenu();
        break;
      }
      case "complete_tutorial": {
        appEvent(`product-tutorials:${state.activeTutorialId}:completed`);
        openTutorialMenu();
        break;
      }
    }

    return reducer(state, action);
  };

export const TutorialController = (props: any) => {
  const [state, dispatch] = useReducer(
    logTutorialEvents(tutorialStateReducer),
    tutorialStateReducer({} as any, { type: "INIT" } as any)
  );
  const endTutorialDialog = useModalContainer();

  const nextStep = useCallback(() => dispatch({ type: "next_step" }), []);
  const endTutorial = useCallback(() => {
    endTutorialDialog.open();
  }, []);
  const completeTutorial = useCallback(
    () => dispatch({ type: "complete_tutorial" }),
    []
  );
  const hideTutorial = useCallback(
    (hide: boolean) => dispatch({ type: "hide_tutorial", hide: hide }),
    []
  );
  const setWikiEditor = useCallback(
    (editor: any) => dispatch({ type: "set_wiki_editor", editor: editor }),
    []
  );
  const ctxVal = useMemo(
    () => ({
      ...state,
      endTutorial,
      completeTutorial,
      nextStep,
      hideTutorial,
      setWikiEditor,
      dispatch,
    }),
    [state, dispatch]
  );
  return (
    <tutorialContext.Provider value={ctxVal}>
      {props.children}
      <ModalContainer {...endTutorialDialog}>
        <EndTutorialDialog
          onAccept={() => {
            dispatch({ type: "end_tutorial" });
            endTutorialDialog.close();
          }}
          onCancel={endTutorialDialog.close}
        />
      </ModalContainer>
    </tutorialContext.Provider>
  );
};

export const useTutorialController = () => useContext(tutorialContext);

export const useOutletId = (id: string | null) => {
  const { dispatch } = useTutorialController();

  useEffect(() => {
    dispatch({ type: "set_outlet", outletId: id });
  }, [id]);
};

export const TutorialOutlet = (props: {
  id: string | null;
  renderActiveStep: () => JSX.Element;
}) => {
  const { currentOutletId } = useTutorialController();

  if (props.id !== currentOutletId) {
    return null;
  }

  return props.renderActiveStep();
};

export const createActiveStepRenderer =
  (getTutorials: () => Map<string, any>) => () => {
    const { activeTutorialId, activeTutorialStep } = useTutorialController();
    if (!activeTutorialId || activeTutorialStep === null) {
      return null;
    }

    const tutorials = getTutorials();

    const step = tutorials.get(activeTutorialId)?.[activeTutorialStep];

    if (!step)
      return (
        <WalkthroughStep title="Something went wrong when we tried to show this tutorial."></WalkthroughStep>
      );

    return step;
  };
