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
import { define, SVGUseAdapter } from "smart-icon";

const SPRITE_SELECTOR_ATTRIBUTE = "data-dremio-icon";

const removeSvgSprite = (): void => {
  const matches = globalThis.document.body.querySelectorAll(
    `[${SPRITE_SELECTOR_ATTRIBUTE}=true]`,
  );

  Array.from(matches).forEach((match) => {
    globalThis.document.body.removeChild(match);
  });
};

export const loadSvgSprite = (path: string) =>
  fetch(path)
    .then((res) => res.text())
    .then((svg) => {
      const svgElement = new DOMParser()
        .parseFromString(svg, "text/html")
        .getElementsByTagName("svg")[0];

      if (!svgElement) {
        throw new Error("Failed to load " + path);
      }

      svgElement.setAttribute(SPRITE_SELECTOR_ATTRIBUTE, "true");

      Array.from(svgElement.getElementsByTagName("parsererror")).forEach(
        (el) => {
          console.warn(el.textContent);
          svgElement.removeChild(el);
        },
      );

      removeSvgSprite();

      globalThis.document.body.insertAdjacentElement("beforeend", svgElement);
    })
    .catch((e) => {
      console.error(e);
    });

/**
 * Globally defines the `<dremio-icon>` custom element and configures runtime icon path resolution
 * @param spritePath The HTTP path for the icon spritesheet at runtime
 */
export const configureDremioIcon = (): void => {
  define("dremio-icon", {
    adapter: SVGUseAdapter,
    aliases: {
      "interface/select-expand": "interface/caretDown",
      "job-state/job-completed": "job-state/completed",
    },
    resolvePath: (name) => `#${name}`,
  });
};
