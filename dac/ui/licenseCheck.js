#!/usr/bin/env node
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
/*eslint no-console: 0, max-len: 0, no-sync: 0, no-restricted-properties:0*/


/*

Before running, just to be safe:
rm -rf node_modules
npm install
(Chris notes: not sure if this is needed.)

*/

// Important Notes:
//
// From https://spdx.org/frequently-asked-questions-faq-0:
// - How does one represent a file or package that is disjunctively licensed (i.e., a license choice)?
//   - Disjunctive licensing can be represented via a license expression using the OR operator. For example, a file that is disjunctively licensed under either the GPL-2.0 or MIT would be represented using the following disjunctive expression: GPL-2.0 OR MIT.
// - How does one represent a file that is licensed under two or more licenses?
//   - Conjunctive licensing can be represented via a license expression using the AND operator. For example, a file that is subject to the Apache-2.0, MIT, and GPL-2.0 would be represented using the following conjunctive expression: Apache-2.0 AND MIT AND GPL-2.0


const fs = require('fs');

const checkNodeVersion = require('check-node-version');

const checker = require('license-checker');
require('isomorphic-fetch');

const csv = require('csv');

process.chdir(__dirname);

const NO_WRITE = process.argv.includes('--no-write');

const BLESSED = new Set(['MIT', 'CC0-1.0', 'ISC', 'BSD-2-Clause', 'Apache-2.0', 'Unlicense', 'Public Domain', 'BSD-3-Clause', '(OFL-1.1 AND MIT)']);

const NORMALIZED_LICENSES = {
  // normalize:
  'Apache 2.0': 'Apache-2.0',
  'Apache License, Version 2.0': 'Apache-2.0',

  // choose MIT over GPL
  '(MIT OR GPL-2.0)': 'MIT',
  '(GPL-2.0 OR MIT)': 'MIT'
};

const KNOWN = {
  'csprng': {
    tagScheme: v => v
  },
  'ws': {
    tagScheme: v => v
  },
  'sequin': {
    tagScheme: v => v
  },
  'ua-parser-js': {
    tagScheme: v => v
  },

  // the following include versions for safety:
  'stackframe@0.3.1': {
    licenses: 'Unlicense' // Chris confirmed
  },
  'husl@5.0.3': {
    licenses: 'MIT', // Chris confirmed
    // custom tags
    licenseURL: 'https://raw.githubusercontent.com/hsluv/hsluv/_legacyjs5.0.3/README.md'
  },
  'options@0.0.6': {
    licenses: 'MIT', // Chris confirmed
    // doesn't use tags
    licenseURL: 'https://github.com/einaros/options.js/raw/65cc69a05e257d6974bb914d47eaf91d92d43ebd/README.md'
  },
  'change-emitter@0.1.6': {
    noFile: true
  },
  'mumath@3.3.4': {
    licenseURL: 'https://raw.githubusercontent.com/dfcreative/mumath/master/UNLICENSE'
  },
  'jsplumb@2.1.4': {
    // custom name, and doesn't use `v#` tagging scheme
    // licenseURL: 'https://raw.githubusercontent.com/sporritt/jsPlumb/2.1.4/jsPlumb-LICENSE.txt'
    licenseText: '\t\t\t\tMIT LICENSE\n\nCopyright (c) 2010 - 2014 jsPlumb, http://jsplumbtoolkit.com/\n\nPermission is hereby granted, free of charge, to any person obtaining\na copy of this software and associated documentation files (the\n"Software"), to deal in the Software without restriction, including\nwithout limitation the rights to use, copy, modify, merge, publish,\ndistribute, sublicense, and/or sell copies of the Software, and to\npermit persons to whom the Software is furnished to do so, subject to\nthe following conditions:\n\nThe above copyright notice and this permission notice shall be\nincluded in all copies or substantial portions of the Software.\n\nTHE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,\nEXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF\nMERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND\nNONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE\nLIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION\nOF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION\nWITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.'
  },
  'regenerator-runtime@0.10.5': {
    // custom tag scheme
    licenseURL: 'https://raw.githubusercontent.com/facebook/regenerator/runtime%400.10.5/LICENSE'
  },
  'ua-parser-js@0.7.12': {
    // strip GPL
    licenseText: 'Copyright © 2012-2016 Faisal Salman <fyzlman@gmail.com>\n\nPermission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:\n\nThe above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.'
  },
  'hoconfig-js@0.1.12': {
    // no tags
    licenseURL: 'https://raw.githubusercontent.com/mingqi/hoconfig-js/15488c60a188365e41b952bf923a325b36c77274/LICENSE'
  },
  'humanable@0.0.2': {
    noFile: true
  },
  'hoist-non-react-statics@1.2.0': {
    // just says BSD; checked license file and found BSD 3-clause text
    licenses: 'BSD-3-Clause'
  },
  'typo-js': {
    licenseURL: 'https://raw.githubusercontent.com/cfinke/Typo.js/master/license.txt'
  },
  'css-select@1.2.0': {
    licenses: 'BSD-2-Clause',
    //this line does not affect algorithm, but i put it here for reference
    licenseURL: 'https://raw.githubusercontent.com/fb55/css-select/v1.2.0/LICENSE'
  },
  'css-what@2.1.0': {
    licenses: 'BSD-2-Clause',
    //this line does not affect algorithm, but i put it here for reference
    licenseURL: 'https://github.com/fb55/css-what/blob/v2.1.0/LICENSE'
  },
  'domelementtype@1.1.3': {
    licenses: 'BSD-2-Clause'
  },
  'domelementtype@1.3.0': {
    licenses: 'BSD-2-Clause'
  },
  'domutils@1.5.1': {
    licenses: 'BSD-2-Clause',
    //this line does not affect algorithm, but i put it here for reference
    licenseURL: 'https://raw.githubusercontent.com/fb55/domutils/347ed4e848dd7129ec9efd18553588636f6d5bf5/LICENSE'
  },
  'entities@1.1.1': {
    licenses: 'BSD-2-Clause',
    //this line does not affect algorithm, but i put it here for reference
    licenseURL: 'https://github.com/fb55/entities/blob/v1.1.1/LICENSE'
  },
  'nth-check@1.0.1': {
    licenses: 'BSD-2-Clause',
    // no license file for this history https://github.com/fb55/nth-check/tree/257338e5bbd53228236abd4cc09539b66b27dd11
    noFile: true
  },
  'railroad-diagrams@1.0.0': {
    licenses: 'CC0-1.0',
    noFile: true
  },
  'has@1.0.1': {
    licenseURL: 'https://raw.githubusercontent.com/tarruda/has/v1.0.1/LICENSE-MIT'
  },
  'regenerator-runtime@0.12.1': {
    licenses: 'MIT',
    licenseURL: 'https://raw.githubusercontent.com/facebook/regenerator/fde052c6c3563e007d233870ecd4a41920773bbd/LICENSE'
  },
  'boolbase@1.0.0': {
    licenseURL: 'https://raw.githubusercontent.com/fb55/boolbase/be0bcd8a4e917a0a5895e95b523fbbed05a64871/LICENSE'
  },
  'debounce@1.2.0': {
    licenses: 'MIT',
    // got from readme https://github.com/component/debounce
    licenseText: `MIT
  Original implementation is from underscore.js which also has an MIT license.`
  },
  'is-in-browser@1.1.3': {
    licenses: 'MIT',
    noFile: true
  }
};

const NAMES = 'LICENSE License.txt LICENSE.md README.md readme.md'.split(' ').reverse();
const fetchLicense = (module) => {
  const uiRepo = module.repository.replace(/\/tree\/master\/.*/, '');
  const repoVersion = module.repository.replace(/\/tree\/master\/.*/, '') + '/' + module.tagScheme(module.version);
  const url = repoVersion.replace(/https:\/\/github.com\//, 'https://raw.githubusercontent.com/');

  const urls = module.licenseURL ? [module.licenseURL] : NAMES.map(n => url + '/' + n);
  const exit = function() {
    console.log(...Array.from(arguments));
    return setErrorToModule(module, 'Could not find license text in ' + uiRepo);
  };

  const fetchNext = async () => {
    if (!urls.length) {
      return exit();
    }
    const response = await fetch(urls.pop());

    console.log('FETCHED', response.url);
    // if (module.name === 'dom-helpers') console.log(response);
    if (!response.ok) {
      return fetchNext();
    }

    const text = await response.text();
    let extractedText = text;

    if (response.url.toLowerCase().endsWith('/readme.md')) {
      // h2 case
      extractedText = text.replace(/[\s\S]+^## License/im, '');
      if (text !== extractedText) {
        extractedText = extractedText.replace(/^##? [\s\S]+/im, '').trim();
      } else {
        extractedText = '';
      }

      // h1 case
      if (!extractedText) {
        extractedText = text.replace(/[\s\S]+^# License/im, '');
        if (text !== extractedText) {
          extractedText = extractedText.replace(/^# [\s\S]+/im, '').trim();
        }
      }

      if (!extractedText) {
        return exit({extractedText, text});
      }
    }
    module.licenseText = extractedText;
    return module;
  };

  return fetchNext();
};

const setErrorToModule = (module, errorMessage) => Object.assign(module, { errorMessage, isError: true});

async function main() {
  checker.init({
    start: './',
    production: true,
    customFormat: {version: null, name: null} // extra includes
  }, async (checkerError, modules) => {
    if (checkerError) return console.error(checkerError);

    for (const [key, module] of Object.entries(modules)) {
      if (module.name === 'dremio-ui') delete modules[key];
    }

    const promises = [];

    for (const [key, module] of Object.entries(modules)) {
      Object.assign(module, {
        tagScheme: v => 'v' + v,
        noFile: false
      }, KNOWN[module.name], KNOWN[key]); // let known override

      if (NORMALIZED_LICENSES[module.licenses]) {
        module.licenses = NORMALIZED_LICENSES[module.licenses];
      }

      if (!BLESSED.has(module.licenses)) {
        setErrorToModule(module, `Found license type that has not been blessed yet: ${module.licenses}`);
        promises.push(module);
        continue;
      }

      if (module.licenseText) {
        promises.push(module);
        continue;
      }

      if (module.noFile) {
        promises.push(Object.assign(module, {licenseText: null}));
        continue;
      }

      if (!module.licenseFile) {
        const repo = module.repository;
        const isGH = repo && repo.startsWith('https://github.com/');
        if (isGH) {
          promises.push(fetchLicense(module));
          continue;
        }

        console.error(`Don't know how to handle ${key}.`);
        console.error(module);
        process.exit(-1);
      }

      promises.push(new Promise((resolve, reject) => {
        fs.readFile(module.licenseFile, 'utf8', (error, licenseText) => error ? reject(error) : resolve(Object.assign(module, {licenseText})));
      }));
    }
    try {
      const data = await Promise.all(promises);

      data.forEach((module) => {
        if (module.licenseText) {
          // cleanliness
          module.licenseText = module.licenseText.replace(/\r\n/g, '\n').replace(/^\s+$/gm, '').replace(/\n+$/, '').replace(/^\n+/, '');
        }
      });

      // try to find the copyright line:
      data.forEach((module) => {
        const clean = txt => txt.replace(/^-*$/gm, '').trim();

        // see if "copyright" is the first markdown paragraph
        let match = (module.licenseText || '').match(/[\s\S]*?(?:$|\n\n)/i);
        if (match) {
          const firstPara = match[0];
          if (firstPara.match(/copyright|©|\(c\)/i)) {
            module.copyright = clean(firstPara);
            return;
          }
        }

        // see if "copyright" is any markdown paragraph with a year
        match = (module.licenseText || '').match(/.*(copyright|©|\(c\)).*[0-9]{4}[\s\S]*?(?:$|\n\n)/i);
        if (match) {
          module.copyright = clean(match[0]);
        }
      });

      let text = data.map((module) => {
        let licenseText = module.licenseText;
        if (licenseText === null) {
          licenseText = module.licenses; // best we can do
        }

        return `${module.name}\n${module.version}\n${module.repository}\n\n${licenseText}`;
      }).join('\n\n' + '-'.repeat(80) + '\n\n');

      // quick-n-dirty entity swap. may need to improve in the future, but for now this covers
      text = text.replace(/&lt;/gi, '<').replace(/&gt;/gi, '>');

      !NO_WRITE && fs.writeFileSync('NOTICE_UI_PRODUCTION.txt', text);
      console.log('\nNOTICE_UI_PRODUCTION.txt written for production dependencies.');

      const gplMatch = text.match(/.*GPL.*/i);
      if (gplMatch && gplMatch[0] !== 'Dual licensed under GPLv2 & MIT') {
        console.error('Found "GPL" in the NOTICE_UI_PRODUCTION.');
        process.exit(-1);
      }

      // check if there is any error
      let isError = false;

      data.forEach((module) => {
        if (module.isError) {
          isError = true;
          console.error(`module: '${module.name}@${module.version}'. ${module.errorMessage}`, module);
        }
      });

      if (isError) {
        process.exit(-1);
      }

      csv.stringify(data, {
        header: true,
        columns: {
          '_': '', // placeholder
          name: 'name',
          groupId: 'groupId', // not a field we have
          artifactId: 'artifactId', // not a field we have
          version: 'version',
          repository: 'url',
          licenses: 'license',
          'in distribution': 'in distribution', // not a field we have
          checked: 'checked', // not a field we have
          text: 'text', // not a field we have
          copyright: 'notice',
          comment: 'comment' // placeholder
        }
      }, (error, cvsText) => {
        if (error) return exitWithError(error);

        !NO_WRITE && fs.writeFileSync('NOTICE_UI_PRODUCTION.csv', cvsText);
        console.log('\nNOTICE_UI_PRODUCTION.csv written for production dependencies.');
      });
    } catch (e) {
      exitWithError(e);
    }
  });
}

function exitWithError() {
  console.error(...arguments);
  process.exit(-1);
}

checkNodeVersion({ npm: '>= 5.7' }, async (error, results) => {
  if (error) {
    exitWithError(error);
    return;
  }

  if (!results.isSatisfied) {
    exitWithError('NPM must be v5.7 or greater due to https://github.com/npm/npm/issues/19596');
    return;
  }

  await main();
});
