/*
 * Copyright 2017-2017 Imply Data, Inc.
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

import * as hasOwnProp from 'has-own-prop';
import { Transform } from 'readable-stream';
import { AttributeInfo } from './attributeInfo';
import { PlywoodValue, Datum, Dataset } from './dataset';

/*
 two types of events (maybe 3):
 { hello: 'world', x: 5 }  // short for (3):

 1. { __$$type: 'value', value: 5 }
 2. { __$$type: 'init', attributes: AttributeInfo[], keys: string[] }
 3. { __$$type: 'row', row: { hello: "world", x: 5 }, keyProp?: "hello" }
 4. { __$$type: 'within', keyProp: "hello", propValue: "world", attribute: "subDataset", apply: { ... } }
 */

export interface PlyBitFull {
  __$$type: 'value' | 'init' | 'row' | 'within';
  value?: PlywoodValue;
  attributes?: AttributeInfo[];
  keys?: string[];
  row?: Datum;
  keyProp?: string;
  propValue?: PlywoodValue;
  attribute?: string;
  within?: PlyBit;
}

export type PlyBit = PlyBitFull | Datum;

export interface PlywoodValueIterator {
  (): PlyBit | null;
}

interface KeyPlywoodValueIterator {
  attribute: string;
  datasetIterator: PlywoodValueIterator;
}

export function iteratorFactory(value: PlywoodValue): PlywoodValueIterator {
  if (value instanceof Dataset) return datasetIteratorFactory(value);

  let nextBit: PlyBit = { __$$type: 'value', value };
  return () => {
    const ret = nextBit;
    nextBit = null;
    return ret;
  };
}

export function datasetIteratorFactory(dataset: Dataset): PlywoodValueIterator {
  let curRowIndex = -2;
  let curRow: Datum = null;
  let cutRowDatasets: KeyPlywoodValueIterator[] = [];

  function nextSelfRow() {
    curRowIndex++;
    cutRowDatasets = [];
    let row = dataset.data[curRowIndex];
    if (row) {
      curRow = {};
      for (let k in row) {
        let v = row[k];
        if (v instanceof Dataset) {
          cutRowDatasets.push({
            attribute: k,
            datasetIterator: datasetIteratorFactory(v)
          });
        } else {
          curRow[k] = v;
        }
      }
    } else {
      curRow = null;
    }
  }

  return () => {
    if (curRowIndex === -2) { // Initial run
      curRowIndex++;
      return {
        __$$type: 'init',
        attributes: dataset.attributes,
        keys: dataset.keys
      };
    }

    let pb: PlyBit;
    while (cutRowDatasets.length && !pb) {
      pb = cutRowDatasets[0].datasetIterator();
      if (!pb) cutRowDatasets.shift();
    }

    if (pb) {
      return {
        __$$type: 'within',
        attribute: cutRowDatasets[0].attribute,
        within: pb
      };
    }

    nextSelfRow();
    return curRow;
  };
}

export class PlywoodValueBuilder {
  private _value: PlywoodValue = null;
  private _attributes: AttributeInfo[];
  private _keys: string[];
  private _data: Datum[];
  private _curAttribute: string = null;
  private _curValueBuilder: PlywoodValueBuilder = null;

  private _finalizeLastWithin() {
    if (!this._curValueBuilder) return;
    let lastDatum = this._data[this._data.length - 1];
    if (!lastDatum) throw new Error('unexpected within');
    lastDatum[this._curAttribute] = this._curValueBuilder.getValue();
    this._curAttribute = null;
    this._curValueBuilder = null;
  }

  public processBit(bit: PlyBit) {
    if (typeof bit !== 'object') throw new Error(`invalid bit: ${bit}`);
    let fullBit: PlyBitFull = hasOwnProp(bit, '__$$type') ? (bit as PlyBitFull) : { __$$type: 'row', row: bit as Datum };
    switch (fullBit.__$$type) {
      case 'value':
        this._value = fullBit.value;
        this._data = null;
        this._curAttribute = null;
        this._curValueBuilder = null;
        break;

      case 'init':
        this._finalizeLastWithin();
        this._attributes = fullBit.attributes;
        this._keys = fullBit.keys;
        this._data = [];
        break;

      case 'row':
        this._finalizeLastWithin();
        if (!this._data) this._data = [];
        this._data.push(fullBit.row);
        break;

      case 'within':
        if (!this._curValueBuilder) {
          this._curAttribute = fullBit.attribute;
          this._curValueBuilder = new PlywoodValueBuilder();
        }
        this._curValueBuilder.processBit(fullBit.within);
        break;

      default:
        throw new Error(`unexpected __$$type: ${fullBit.__$$type}`);
    }
  }

  public getValue(): PlywoodValue {
    const { _data } = this;
    if (_data) {
      if (this._curValueBuilder) {
        let lastDatum = _data[_data.length - 1];
        if (!lastDatum) throw new Error('unexpected within');
        lastDatum[this._curAttribute] = this._curValueBuilder.getValue();
      }
      return new Dataset({
        attributes: this._attributes,
        keys: this._keys,
        data: _data
      });
    } else {
      return this._value;
    }
  }
}

