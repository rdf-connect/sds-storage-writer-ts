
import type * as RDF from '@rdfjs/types';
import { Member, RelationType } from '@treecg/types';
import { Collection } from "mongodb";

export type MongoFragment = {
  id?: string,
  streamId: string,
  value?: string,
  relations: { type: RelationType, value: string, bucket: string, path?: string, timestampRelation?: boolean }[],
  members?: string[],
  count: number,
  timeStamp?: string
};

export type ExtractIndices = (member: Member, mongo: Collection<MongoFragment>, timestampPath?: RDF.Term) => Promise<void>;
export interface Fragment {
  streamId: string,
  extract: ExtractIndices,
}

//TODO! timestampValue is a date
export async function handleTimestampPath(id: string, streamId: string, path: string, timestampValue: string, memberId: string, mongo: Collection<MongoFragment>, maxSize: number) {
  // smallerIndex fragment is the fragment where we want to put this member (lagest smaller fragment)
  const smallerIndex: undefined | MongoFragment = (await mongo.find({ id, streamId, timeStamp: { $lte: timestampValue } }).sort({ timeStamp: -1 }).limit(1).toArray())[0];

  // There is room in the current bucket, add it and return
  if (smallerIndex && (smallerIndex.count < maxSize || timestampValue === smallerIndex.timeStamp)) {
    await mongo.updateOne({ streamId, id, timeStamp: smallerIndex.timeStamp }, { $inc: { count: 1 }, $push: { members: memberId } });
    return;
  }

  // We will have to create a new timestamp bucket
  const relations: MongoFragment["relations"] = [];

  // if there is a smallerIndex, relate to it
  if (smallerIndex) {
    relations.push({ type: RelationType.LessThan, value: timestampValue, bucket: smallerIndex.timeStamp!, path, timestampRelation: true });
    await mongo.updateOne({ streamId, id, timeStamp: smallerIndex.timeStamp }, { "$push": { relations: { path, type: RelationType.GreaterThanOrEqualTo, value: timestampValue, bucket: timestampValue, timestampRelation: true } } })
  }

  // There is no smaller index, let's see if there is a larger index 
  const largerIndex: undefined | MongoFragment = (await mongo.find({ id, streamId, timeStamp: { "$gt": timestampValue } }).sort({ timeStamp: -1 }).limit(1).toArray())[0];
  if (!!largerIndex) {
    // We potentially add a greater than or equal to relation to the next bucket
    relations.push({ type: RelationType.GreaterThanOrEqualTo, value: largerIndex.timeStamp!, bucket: largerIndex.timeStamp!, path, timestampRelation: true });
    // And to that bucket, we add a less then relation to the new bucket
    await mongo.updateOne({ streamId, id, timeStamp: largerIndex.timeStamp }, { "$push": { relations: { path, type: RelationType.LessThan, value: largerIndex.timeStamp!, bucket: timestampValue, timestampRelation: true } } })
  }

  await mongo.insertOne({ streamId, id, count: 1, relations, members: [memberId], timeStamp: timestampValue });
}

