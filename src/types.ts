export type PlyTypeSingleValue = 'NULL' | 'BOOLEAN' | 'NUMBER' | 'TIME' | 'STRING';
export type PlyTypeSimple = PlyTypeSingleValue | 'NUMBER_RANGE' | 'TIME_RANGE' | 'STRING_RANGE' | 'SET' | 'SET/NULL' | 'SET/BOOLEAN' | 'SET/NUMBER' | 'SET/TIME' | 'SET/STRING' | 'SET/NUMBER_RANGE' | 'SET/TIME_RANGE' | 'SET/STRING_RANGE';

export type PlyType = PlyTypeSimple | 'DATASET';

export interface SimpleFullType {
  type: PlyTypeSimple;
}

export interface DatasetFullType {
  type: 'DATASET';
  datasetType: Lookup<FullType>;
  parent?: DatasetFullType;
  remote?: boolean;
}

export type FullType = SimpleFullType | DatasetFullType;
