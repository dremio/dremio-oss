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
/* eslint react/prop-types: 0 */
import { additionalSetup } from "../src/additionalSetup";
import { configure } from "@testing-library/dom";
import "./commonGlobalVariables";
import en from "dyn-load/locales/en.json";
import mockCssModules from "mock-css-modules";
import config from "../webpack.config";
import "../src/additionalSetup";

//data-testid = data-qa for get/queryByTestId in react-testing-library
configure({ testIdAttribute: "data-qa" });
additionalSetup();
require("jsdom-global")(undefined, {
  url: "http://localhost/",
  pretendToBeVisual: true,
});

global.requestAnimationFrame = (cb) => cb();
global.customElements = {
  get: () => {},
  define: () => {},
};

// Prevent compiling of .less
mockCssModules.register([".less"]); // needed for css module
require.extensions[".css"] = () => {};
require.extensions[".svg"] = () => {};
require.extensions[".gif"] = () => {};
require.extensions[".png"] = () => {};
require.extensions[".lottie"] = () => {};

const Module = require("module");
// import of react and enzyme must be after jsdom initialization. See https://github.com/airbnb/enzyme/issues/395
const { Component } = require("react");
const Enzyme = require("enzyme");
const Adapter = require("@wojtekmaj/enzyme-adapter-react-17");
Enzyme.configure({
  adapter: new Adapter(),
  disableLifecycleMethods: true, // this is default behaviour that has place for enzyme v.2
});

// Svg tests fails with npm 6.1. A workaround is found here https://github.com/jsdom/jsdom/issues/1721
if (typeof URL.createObjectURL === "undefined") {
  Object.defineProperty(URL, "createObjectURL", { value: () => "mock_url" });
}

global.window.config = {
  // will this it's FE-DEV, BE-DEV
  language: "dataEN",
};

window.requestAnimationFrame = setTimeout;
window.cancelAnimationFrame = clearTimeout;

// jsDom does not implement offset* methods.  This will break tests
// for components that use react-virtualized-auto-sizer
// (https://github.com/jsdom/jsdom/issues/135#issuecomment-68191941)
Object.defineProperties(window.HTMLElement.prototype, {
  offsetLeft: {
    get() {
      return parseFloat(window.getComputedStyle(this).marginLeft) || 0;
    },
  },
  offsetTop: {
    get() {
      return parseFloat(window.getComputedStyle(this).marginTop) || 0;
    },
  },
  offsetHeight: {
    get() {
      return parseFloat(window.getComputedStyle(this).height) || 0;
    },
  },
  offsetWidth: {
    get() {
      return parseFloat(window.getComputedStyle(this).width) || 0;
    },
  },
});

global.localStorage = (() => {
  const store = {};
  const getItem = (key) => {
    // check for globalApp to avoid console log from localStorageUtil
    return key === "globalApp" ? "null" : store[key];
  };
  const setItem = (key, value) => {
    store[key] = value;
  };
  const removeItem = (key) => {
    delete store[key];
  };
  return {
    getItem,
    setItem,
    removeItem,
  };
})();

global.sessionStorage = (() => {
  const store = {};
  const getItem = (key) => {
    return store[key];
  };
  const setItem = (key, value) => {
    store[key] = value;
  };
  const removeItem = (key) => {
    delete store[key];
  };
  return {
    getItem,
    setItem,
    removeItem,
  };
})();

global.MutationObserver = class {
  constructor(callback) {}
  disconnect() {}
  observe(element, initObject) {}
};

global.ResizeObserver = class {
  observe() {}
  unobserve() {}
  disconnect() {}
};

global.MessageChannel = class {
  port1 = class {
    close() {}
    postMessage(_message) {}
  };
  port2 = class {
    close() {}
    postMessage(_message) {}
  };
};

global.SVGPathSeg = function () {};
global.SVGPathSegClosePath = function () {};
global.SVGPathSegMovetoAbs = function () {};
global.SVGPathSegMovetoRel = function () {};
global.SVGPathSegLinetoAbs = function () {};
global.SVGPathSegLinetoRel = function () {};
global.SVGPathSegCurvetoCubicAbs = function () {};
global.SVGPathSegCurvetoCubicRel = function () {};
global.SVGPathSegCurvetoQuadraticAbs = function () {};
global.SVGPathSegCurvetoQuadraticRel = function () {};
global.SVGPathSegCurvetoQuadraticRel = function () {};
global.SVGPathSegArcAbs = function () {};
global.SVGPathSegArcRel = function () {};
global.SVGPathSegLinetoHorizontalAbs = function () {};
global.SVGPathSegLinetoHorizontalRel = function () {};
global.SVGPathSegLinetoVerticalAbs = function () {};
global.SVGPathSegLinetoVerticalRel = function () {};
global.SVGPathSegCurvetoCubicSmoothAbs = function () {};
global.SVGPathSegCurvetoCubicSmoothRel = function () {};
global.SVGPathSegCurvetoQuadraticSmoothAbs = function () {};
global.SVGPathSegCurvetoQuadraticSmoothRel = function () {};
window.SVGPathElement = global.SVGPathElement = function () {};
global.SVGPathSegList = function () {};

class FakeComponent extends Component {
  render() {
    return <span>fake</span>;
  }
}

const checkIntlId = (id) => {
  if (!en[id]) throw new Error(`Intl id "${id}" does not exist.`);
};

const aliases = Object.entries(config.resolve.alias);
const applyAliases = (module) => {
  const cnt = aliases.length;
  for (let i = 0; i < cnt; i++) {
    const [key, path] = aliases[i];
    if (module.indexOf(key) === 0) {
      return module.replace(key, path);
    }
  }
  return module;
};
const originalRequire = Module.prototype.require;
Module.prototype.require = function (module) {
  // provide a fake react-intl lib:
  // - make it so that IntlProvider isn't required
  // - make it so that our tests can check output strings without hardcoding to English (as the English can change for design reasons)
  // WARNING: this is partial - grow it as you use more APIs
  if (module === "react-intl") {
    const tmpIntl = {
      formatMessage(desc) {
        if (!desc.id) throw new Error("Missing `id` for formatMessage.");
        checkIntlId(desc.id);
        return JSON.stringify(arguments);
      },
    };
    return {
      FormattedMessage(...props) {
        if (!props.id) throw new Error("Missing `id` for FormattedMessage.");
        checkIntlId(props.id);
        return <span>{JSON.stringify(props)}</span>;
      },
      injectIntl(Wrapped) {
        // hack in the intl props in a way that keeps the tests simple
        Wrapped.defaultProps = Wrapped.defaultProps || {};
        Wrapped.defaultProps.intl = tmpIntl;
        return Wrapped;
      },
      createIntlCache() {
        return;
      },
      createIntl() {
        return tmpIntl;
      },
      useIntl: () => tmpIntl,
    };
  }

  if (module === "dremio-ui-common/contexts/IntlContext.js") {
    return {
      getIntlContext: () => ({
        addMessages: () => {},
        t: (messageKey) => {
          return messageKey;
        },
      }),
    };
  }

  if (
    module ===
    "dremio-ui-common/sonar/components/Monaco/components/SqlViewer/SqlViewer.js"
  ) {
    return () => {};
  }

  module = applyAliases(module); // use webpack aliases for correct module resolving

  // since we are not in webpack, make glob-loader
  // work. (this is only tested for use with images/)
  if (/\.pattern$/.test(module)) {
    const patternFile = require.resolve(module);
    const pattern = require("fs").readFileSync(patternFile, "utf8"); // eslint-disable-line no-sync
    const relativeTo = patternFile.split("/").slice(0, -1).join("/");
    const files = require("glob").sync(relativeTo + "/" + pattern);

    const fileFakes = {};
    for (const file of files) {
      fileFakes["." + file.slice(relativeTo.length)] = {
        default: FakeComponent,
      };
    }

    return fileFakes;
  }

  if (module.includes("LayoutWorker.worker")) {
    class LayoutWorker {
      constructor() {
        this.default = () => {};
      }
    }
    return LayoutWorker;
  } else if (module.includes("SQLParsingWorker.worker")) {
    class SQLParsingWorker {
      constructor() {
        this.default = () => {};
      }
      postMessage(_message, _transfer) {}
    }
    return SQLParsingWorker;
  }

  return originalRequire.call(this, module);
};
