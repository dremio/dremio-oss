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

const marked = require("marked");
const markedRenderer = new marked.Renderer();
//open links in a new tab
markedRenderer.link = function (href, title, text) {
  const link = marked.Renderer.prototype.link.call(this, href, title, text);
  return link.replace("<a", "<a target='_blank'");
};

marked.setOptions({
  sanitize: true, // protect from XSS
  renderer: markedRenderer,
});

// disable some eslint rules, as source code does not compliant to it
/* eslint prefer-const: "off" */
/* eslint no-cond-assign: "off" */
/* eslint no-bitwise: "off" */
/* eslint no-nested-ternary: "off" */
/* eslint complexity: "off" */
/* eslint object-shorthand: "off" */
/* eslint no-var: "off" */

// it is mostly original code without any changes. Search for '[Dremio override]' comment to find altered code
// [Dremio override] DX-12872 --------------------------
// Original 'marked.Lexer.prototype.token' uses internal variable 'block' that is exposed through Lexer.rules
// make it available here.
// see https://github.com/markedjs/marked/blob/v0.7.0/lib/marked.js#L154
const block = marked.Lexer.rules;
// -----------------------------------------------------

// Method from original source code https://github.com/markedjs/marked/blob/v0.7.0/lib/marked.js#L1496
// it is untouched
//-----------------------------------
// Remove trailing 'c's. Equivalent to str.replace(/c*$/, '').
// /c*$/ is vulnerable to REDOS.
// invert: Remove suffix of non-c chars instead. Default falsey.
function rtrim(str, c, invert) {
  if (str.length === 0) {
    return "";
  }

  // Length of suffix matching the invert condition.
  var suffLen = 0;

  // Step left until we fail to match the invert condition.
  while (suffLen < str.length) {
    var currChar = str.charAt(str.length - suffLen - 1);
    if (currChar === c && !invert) {
      suffLen++;
    } else if (currChar !== c && invert) {
      suffLen++;
    } else {
      break;
    }
  }

  return str.substr(0, str.length - suffLen);
}

// Method from original source code https://github.com/markedjs/marked/blob/v0.7.0/lib/marked.js#L1461
// it is untouched
function splitCells(tableRow, count) {
  // ensure that every cell-delimiting pipe has a space
  // before it to distinguish it from an escaped pipe
  var row = tableRow.replace(/\|/g, function (match, offset, str) {
      var escaped = false,
        curr = offset;
      while (--curr >= 0 && str[curr] === "\\") escaped = !escaped;
      if (escaped) {
        // odd number of slashes means | is escaped
        // so we leave it alone
        return "|";
      } else {
        // add space before unescaped |
        return " |";
      }
    }),
    cells = row.split(/ \|/),
    i = 0;

  if (cells.length > count) {
    cells.splice(count);
  } else {
    while (cells.length < count) cells.push("");
  }

  for (; i < cells.length; i++) {
    // leading or trailing whitespace is ignored per the gfm spec
    cells[i] = cells[i].trim().replace(/\\\|/g, "|");
  }
  return cells;
}

marked.Lexer.prototype.token = function (src, top) {
  src = src.replace(/^ +$/gm, "");
  var next,
    loose,
    cap,
    bull,
    b,
    item,
    listStart,
    listItems,
    t,
    space,
    i,
    tag,
    l,
    isordered,
    istask,
    ischecked;

  while (src) {
    // newline
    if ((cap = this.rules.newline.exec(src))) {
      src = src.substring(cap[0].length);
      if (cap[0].length > 1) {
        this.tokens.push({
          type: "space",
          text: cap[0], // [Dremio override] DX-12872 save the text. It is needed for correct newline rendering
        });
      }
    }

    // code
    if ((cap = this.rules.code.exec(src))) {
      var lastToken = this.tokens[this.tokens.length - 1];
      src = src.substring(cap[0].length);
      // An indented code block cannot interrupt a paragraph.
      if (lastToken && lastToken.type === "paragraph") {
        lastToken.text += "\n" + cap[0].trimRight();
      } else {
        cap = cap[0].replace(/^ {4}/gm, "");
        this.tokens.push({
          type: "code",
          codeBlockStyle: "indented",
          text: !this.options.pedantic ? rtrim(cap, "\n") : cap,
        });
      }
      continue;
    }

    // fences
    if ((cap = this.rules.fences.exec(src))) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: "code",
        lang: cap[2] ? cap[2].trim() : cap[2],
        text: cap[3] || "",
      });
      continue;
    }

    // heading
    if ((cap = this.rules.heading.exec(src))) {
      // [Dremio override] DX-12872 --------------------------
      // we should NOT remove new line charactes here. Leave them to be process by new line section
      // Without this new lines after headings would be collapsed.
      const index = src.indexOf("\n");
      src = src.substring(index >= 0 ? index : cap[0].length);
      // original code was
      // src = src.substring(cap[0].length);
      // end of [Dremio override] --------------------------
      this.tokens.push({
        type: "heading",
        depth: cap[1].length,
        text: cap[2],
      });
      continue;
    }

    // table no leading pipe (gfm)
    if ((cap = this.rules.nptable.exec(src))) {
      item = {
        type: "table",
        header: splitCells(cap[1].replace(/^ *| *\| *$/g, "")),
        align: cap[2].replace(/^ *|\| *$/g, "").split(/ *\| */),
        cells: cap[3] ? cap[3].replace(/\n$/, "").split("\n") : [],
      };

      if (item.header.length === item.align.length) {
        src = src.substring(cap[0].length);

        for (i = 0; i < item.align.length; i++) {
          if (/^ *-+: *$/.test(item.align[i])) {
            item.align[i] = "right";
          } else if (/^ *:-+: *$/.test(item.align[i])) {
            item.align[i] = "center";
          } else if (/^ *:-+ *$/.test(item.align[i])) {
            item.align[i] = "left";
          } else {
            item.align[i] = null;
          }
        }

        for (i = 0; i < item.cells.length; i++) {
          item.cells[i] = splitCells(item.cells[i], item.header.length);
        }

        this.tokens.push(item);

        continue;
      }
    }

    // hr
    if ((cap = this.rules.hr.exec(src))) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: "hr",
      });
      continue;
    }

    // blockquote
    if ((cap = this.rules.blockquote.exec(src))) {
      src = src.substring(cap[0].length);

      this.tokens.push({
        type: "blockquote_start",
      });

      cap = cap[0].replace(/^ *> ?/gm, "");

      // Pass `top` to keep the current
      // "toplevel" state. This is exactly
      // how markdown.pl works.
      this.token(cap, top);

      this.tokens.push({
        type: "blockquote_end",
      });

      continue;
    }

    // list
    if ((cap = this.rules.list.exec(src))) {
      src = src.substring(cap[0].length);
      bull = cap[2];
      isordered = bull.length > 1;

      listStart = {
        type: "list_start",
        ordered: isordered,
        start: isordered ? +bull : "",
        loose: false,
      };

      this.tokens.push(listStart);

      // Get each top-level item.
      cap = cap[0].match(this.rules.item);

      listItems = [];
      next = false;
      l = cap.length;
      i = 0;

      for (; i < l; i++) {
        item = cap[i];

        // Remove the list item's bullet
        // so it is seen as the next token.
        space = item.length;
        item = item.replace(/^ *([*+-]|\d+\.) */, "");

        // Outdent whatever the
        // list item contains. Hacky.
        if (~item.indexOf("\n ")) {
          space -= item.length;
          item = !this.options.pedantic
            ? item.replace(new RegExp("^ {1," + space + "}", "gm"), "")
            : item.replace(/^ {1,4}/gm, "");
        }

        // Determine whether the next list item belongs here.
        // Backpedal if it does not belong in this list.
        if (i !== l - 1) {
          b = block.bullet.exec(cap[i + 1])[0];
          if (
            bull.length > 1
              ? b.length === 1
              : b.length > 1 || (this.options.smartLists && b !== bull)
          ) {
            src = cap.slice(i + 1).join("\n") + src;
            i = l - 1;
          }
        }

        // Determine whether item is loose or not.
        // Use: /(^|\n)(?! )[^\n]+\n\n(?!\s*$)/
        // for discount behavior.
        loose = next || /\n\n(?!\s*$)/.test(item);
        if (i !== l - 1) {
          next = item.charAt(item.length - 1) === "\n";
          if (!loose) loose = next;
        }

        if (loose) {
          listStart.loose = true;
        }

        // Check for task list items
        istask = /^\[[ xX]\] /.test(item);
        ischecked = undefined;
        if (istask) {
          ischecked = item[1] !== " ";
          item = item.replace(/^\[[ xX]\] +/, "");
        }

        t = {
          type: "list_item_start",
          task: istask,
          checked: ischecked,
          loose: loose,
        };

        listItems.push(t);
        this.tokens.push(t);

        // Recurse.
        this.token(item, false);

        this.tokens.push({
          type: "list_item_end",
        });
      }

      if (listStart.loose) {
        l = listItems.length;
        i = 0;
        for (; i < l; i++) {
          listItems[i].loose = true;
        }
      }

      this.tokens.push({
        type: "list_end",
      });

      continue;
    }

    // html
    if ((cap = this.rules.html.exec(src))) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: this.options.sanitize ? "paragraph" : "html",
        pre:
          !this.options.sanitizer &&
          (cap[1] === "pre" || cap[1] === "script" || cap[1] === "style"),
        text: this.options.sanitize
          ? this.options.sanitizer
            ? this.options.sanitizer(cap[0])
            : escape(cap[0])
          : cap[0],
      });
      continue;
    }

    // def
    if (top && (cap = this.rules.def.exec(src))) {
      src = src.substring(cap[0].length);
      if (cap[3]) cap[3] = cap[3].substring(1, cap[3].length - 1);
      tag = cap[1].toLowerCase().replace(/\s+/g, " ");
      if (!this.tokens.links[tag]) {
        this.tokens.links[tag] = {
          href: cap[2],
          title: cap[3],
        };
      }
      continue;
    }

    // table (gfm)
    if ((cap = this.rules.table.exec(src))) {
      item = {
        type: "table",
        header: splitCells(cap[1].replace(/^ *| *\| *$/g, "")),
        align: cap[2].replace(/^ *|\| *$/g, "").split(/ *\| */),
        cells: cap[3] ? cap[3].replace(/\n$/, "").split("\n") : [],
      };

      if (item.header.length === item.align.length) {
        src = src.substring(cap[0].length);

        for (i = 0; i < item.align.length; i++) {
          if (/^ *-+: *$/.test(item.align[i])) {
            item.align[i] = "right";
          } else if (/^ *:-+: *$/.test(item.align[i])) {
            item.align[i] = "center";
          } else if (/^ *:-+ *$/.test(item.align[i])) {
            item.align[i] = "left";
          } else {
            item.align[i] = null;
          }
        }

        for (i = 0; i < item.cells.length; i++) {
          item.cells[i] = splitCells(
            item.cells[i].replace(/^ *\| *| *\| *$/g, ""),
            item.header.length
          );
        }

        this.tokens.push(item);

        continue;
      }
    }

    // lheading
    if ((cap = this.rules.lheading.exec(src))) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: "heading",
        depth: cap[2].charAt(0) === "=" ? 1 : 2,
        text: cap[1],
      });
      continue;
    }

    // top-level paragraph
    if (top && (cap = this.rules.paragraph.exec(src))) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: "paragraph",
        text:
          cap[1].charAt(cap[1].length - 1) === "\n"
            ? cap[1].slice(0, -1)
            : cap[1],
      });
      continue;
    }

    // text
    if ((cap = this.rules.text.exec(src))) {
      // Top-level should never reach here.
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: "text",
        text: cap[0],
      });
      continue;
    }

    if (src) {
      throw new Error("Infinite loop on byte: " + src.charCodeAt(0));
    }
  }

  return this.tokens;
};
/* eslint-enable */

//override space rendering

const originalTok = marked.Parser.prototype.tok;

marked.Parser.prototype.tok = function () {
  switch (this.token.type) {
    case "space": {
      /*
      Assume a case:
      1) `a
      b`
      'a' and 'b' would be turned to paragraphs and token.text would contain a single new line character
      To display that markdown correctly, we should end up with:
      <p>a</p>
      <p>b</p>
      So we should insert 0 line breaks in that case.
      2) `a

      b`.
      token.text would contain 2 new line charactes. We should end up with
      <p>a</p>
      <br>
      <p>b</p>
      So only 1 line break is need,
      3) etc.
      So in general we should insert token.length.length - 1 line breaks.
    */
      return new Array(this.token.text.length).join("<br>");
    }
    default:
      return originalTok.apply(this, arguments);
  }
};
