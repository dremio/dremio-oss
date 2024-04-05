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

export const COMPLETED_TUTORIALS = "completedTutorials";

export const getCompletedTutorials = () => {
  const completedTutorials = localStorage.getItem(COMPLETED_TUTORIALS) ?? "[]";
  try {
    return JSON.parse(completedTutorials) as string[];
  } catch (err) {
    console.error("INVALID DATA TO PARSE");
    return [] as string[];
  }
};

export const markTutorialCompleted = (tutorialId: string): void => {
  const completedTutorials = getCompletedTutorials();

  if (completedTutorials.includes(tutorialId)) {
    return;
  }

  localStorage.setItem(
    COMPLETED_TUTORIALS,
    JSON.stringify([...completedTutorials, tutorialId]),
  );
};

export const tutorialIsCompleted = (tutorialId: string) =>
  getCompletedTutorials().includes(tutorialId);

export const getTutorialMenuButton = (): HTMLElement | null =>
  document.querySelector('[data-qa="Tutorials"]');

export const openTutorialMenu = () => {
  try {
    const tutorialMenuButton = getTutorialMenuButton()!;
    if (tutorialMenuButton.getAttribute("aria-expanded") === "false") {
      tutorialMenuButton.click();
    }
  } catch (e) {
    console.error(e);
  }
};
