import {RelationType} from '@treecg/types';

export type TREEFragment = {
   id?: string,
   streamId: string,
   value?: string,
   relations: Array<{
      type: RelationType,
      value?: string,
      bucket: string,
      path?: string,
      timestampRelation?: boolean
   }>,
   members?: string[],
   count: number,
   timeStamp?: Date,
   span: number,
   immutable: boolean,
   root: boolean,
   page: number
};
