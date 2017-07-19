
## Data

# description:
- rows: an array containing the grid rows
```js
  rows: [
     {row: [ { v:"a1"}, { v:"b1"}, { v:"c1"}]},
     {row: [ { v:"a2"}, { v:"b2"}, { v:"c2"}]},
     {row: [ { v:"a3"}, { v:"b3"}, { v:"c3"}]}
  ]
```
each row object has a row field which is an array of cell objects:
cell fields:
  - v: the value to display as a string
  - type: optional type of the value if the col is mixed

- returned_row_count: the returned row count. (the size of the rows array)

- columns: list of the column descriptors

```js
columns: [
      {
        "name": "A",
        "type": "string",
        "index": 0
      },
  ...
  ]
```
each column has the following fields:
 - name: name of the column to display
 - type: the type of the column: string, number, mixed, ...
 - color: the color of the column

> Julien: should we have a "state" instead that describes what the state of the col. Then there is logic in the ui to make it a special color   

 - index: the column index for easy lookup in the "rows" field.

remaining to define:
 - specific format jor join/split/extract preview

# Example
```js
{
  "rows": [
	{ "row": [{ v:"928"}, { v:"0"}, { v:"1995-03-02T00:00:00.000-08:00"}, { v:"306388.83219999995"}]},
	{ "row": [{ v:"28550"}, { v:"0"}, { v:"29"}, { v:"279494.2793"}]},
	{ "row": [{ v:"4742"}, { v:"0"}, { v:"1995-03-23T00:00:00.000-08:00"}, { v:"239414.08630000002"}]},
	{ "row": [{ v:"5444"}, { v:"0"}, { v:"29"}, { v:"238808.0227"}]},
	{ "row": [{ v:"8133"}, { v:"0"}, { v:"1995-02-27T00:00:00.000-08:00"}, { v:"206362.0245"}]},
	{ "row": [{ v:"58117"}, { v:"0"}, { v:"February"}, { v:"203358.58109999998"}]},
	{ "row": [{ v:"22561"}, { v:"0"}, { v:"1995-01-21T00:00:00.000-08:00"}, { v:"202431.9766"}]},
	{ "row": [{ v:"39909"}, { v:"0"}, { v:"January"}, { v:"192495.8032"}]},
	{ "row": [{ v:"51461"}, { v:"0"}, { v:"1995-02-11T00:00:00.000-08:00"}, { v:"188664.7808"}]},
	{ "row": [{ v:"4642"}, { v:"0"}, { v:"29"}, { v:"180873.07739999998"}]}
  ],
  "returned_row_count": 10,
  "columns": [{
	"name": "l_orderkey",
	"type": "string",
	"index": 0
  }, {
	"name": "o_shippriority",
	"type": "string",
	"index": 1
  }, {
	"name": "o_orderdate",
	"type": "mixed",
	"index": 2
  }, {
	"name": "revenue",
	"type": "number",
	"index": 3
  }]
}
```
