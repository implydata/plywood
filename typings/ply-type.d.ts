declare type PlyTypeSingleValue = 'NULL' | 'BOOLEAN' | 'NUMBER' | 'TIME' | 'STRING';
declare type PlyTypeSimple = PlyTypeSingleValue | 'NUMBER_RANGE' | 'TIME_RANGE' | 'STRING_RANGE' | 'SET' | 'SET/NULL' | 'SET/BOOLEAN' | 'SET/NUMBER' | 'SET/TIME' | 'SET/STRING' | 'SET/NUMBER_RANGE' | 'SET/TIME_RANGE';

declare type PlyType = PlyTypeSimple | 'DATASET';

declare interface SimpleFullType {
  type: PlyTypeSimple;
}

declare interface DatasetFullType {
  type: 'DATASET';
  datasetType: Lookup<FullType>;
  parent?: DatasetFullType;
  remote?: boolean;
}

declare type FullType = SimpleFullType | DatasetFullType;
