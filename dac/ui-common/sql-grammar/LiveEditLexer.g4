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
lexer grammar LiveEditLexer;
import LiveEditLexerDQID, DremioLexer;

/** Overrides DremioLexer LETTER to add @ to the characters set, to support autocompleting unquoted home space */
fragment LETTER : [@$A-Z_a-zÀ-ÖØ-öø-ÿĀ-῿぀-㆏㌀-㍿㐀-㴭一-鿿豈-﫿]  ;
