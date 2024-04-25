import type * as RDF from "@rdfjs/types";
import { RelationType } from "@treecg/types";

// Schema for data collection
export type SDSRecord<T extends boolean = false> = {
  stream: string;
  payload: RDF.Term;
  buckets: RDF.Term[];
  timestampValue: T extends true ? Date : string | undefined;
};

// Schema for index collection
export type TREEFragment = {
  id: string;
  root: boolean;
  immutable: boolean;
  streamId: string;
  members: string[];
};

export type FragmentExtension = {
  count: number;
  timeStamp: Date;
  span: number;
  page: number;
};

// Schema for data collection
export type DataRecord = {
  id: string;
  data: string;
  timestamp: Date;
};

// Schema for relation collection
export type Relation = {
  from: string;
  bucket: string;
  type: RelationType;
  value?: string;
  path?: string;
};

export type DBConfig = {
  url: string;
  metadata: string;
  data: string;
  index: string;
};
