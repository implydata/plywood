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

import { Transform } from 'readable-stream';
import { AttributeInfo } from './attributeInfo';
import { PlywoodValue, Datum, Dataset } from './dataset';

/*
Types of bits:

 1. { type: 'value', value: 5 }
 2. { type: 'init', attributes: AttributeInfo[], keys: string[] }
 3. { type: 'datum', datum: { hello: "world", x: 5 }, keyProp?: "hello" }
 4. { type: 'within', keyProp: "hello", propValue: "world", attribute: "subDataset", apply: { ... } }
 */

export interface PlyBit {
  type: 'value' | 'init' | 'datum' | 'within';
  value?: PlywoodValue;
  attributes?: AttributeInfo[];
  keys?: string[];
  datum?: Datum;
  keyProp?: string;
  propValue?: PlywoodValue;
  attribute?: string;
  within?: PlyBit;
}

export interface PlywoodValueIterator {
  (): PlyBit | null;
}

interface KeyPlywoodValueIterator {
  attribute: string;
  datasetIterator: PlywoodValueIterator;
}

export function iteratorFactory(value: PlywoodValue): PlywoodValueIterator {
  if (value instanceof Dataset) return datasetIteratorFactory(value);

  let nextBit: PlyBit = { type: 'value', value };
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
      let initEvent: PlyBit = {
        type: 'init',
        attributes: dataset.attributes
      };
      if (dataset.keys.length) initEvent.keys = dataset.keys;
      return initEvent;
    }

    let pb: PlyBit;
    while (cutRowDatasets.length && !pb) {
      pb = cutRowDatasets[0].datasetIterator();
      if (!pb) cutRowDatasets.shift();
    }

    if (pb) {
      return {
        type: 'within',
        attribute: cutRowDatasets[0].attribute,
        within: pb
      };
    }

    nextSelfRow();
    return curRow ? {
      type: 'datum',
      datum: curRow
    } : null;
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
    switch (bit.type) {
      case 'value':
        this._value = bit.value;
        this._data = null;
        this._curAttribute = null;
        this._curValueBuilder = null;
        break;

      case 'init':
        this._finalizeLastWithin();
        this._attributes = bit.attributes;
        this._keys = bit.keys;
        this._data = [];
        break;

      case 'datum':
        this._finalizeLastWithin();
        if (!this._data) this._data = [];
        this._data.push(bit.datum);
        break;

      case 'within':
        if (!this._curValueBuilder) {
          this._curAttribute = bit.attribute;
          this._curValueBuilder = new PlywoodValueBuilder();
        }
        this._curValueBuilder.processBit(bit.within);
        break;

      default:
        throw new Error(`unexpected type: ${bit.type}`);
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

