
import type * as RDF from '@rdfjs/types';
import { Member, RelationType } from '@treecg/types';
import { Collection } from "mongodb";

export type MongoFragment = {
    id?: string,
    streamId: string,
    value?: string,
    relations: { type: RelationType, value: string, bucket: string, path?: string }[],
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
    const smallerIndex: undefined | MongoFragment = (await mongo.find({  id, streamId, timeStamp: { $lte: timestampValue } }).sort({ timeStamp: -1 }).limit(1).toArray())[0];

    if (smallerIndex) {
        if (smallerIndex.count < maxSize || timestampValue === smallerIndex.timeStamp) {
            await mongo.updateOne({  streamId, id, timeStamp: smallerIndex.timeStamp }, { $inc: { count: 1 }, $push: { members: memberId } });
        } else {
            const largerIndex: undefined | MongoFragment = (await mongo.find({  id, timeStamp: { $gt: timestampValue }, streamId }).sort({ timeStamp: -1 }).limit(1).toArray())[0];

            const relations: MongoFragment["relations"] = [{ type: RelationType.LessThan, value: timestampValue, bucket: smallerIndex.timeStamp!, path }];

            if (!!largerIndex) {
                relations.push({ type: RelationType.GreaterThanOrEqualTo, value: largerIndex.timeStamp!, bucket: largerIndex.timeStamp!, path });
                await mongo.updateOne({  streamId, id, timeStamp: largerIndex.timeStamp, path }, { "$push": { relations: { type: RelationType.LessThan, value: largerIndex.timeStamp!, bucket: timestampValue } } })
            }

            await mongo.updateOne({ streamId, id, timeStamp: smallerIndex.timeStamp, path }, { "$push": { relations: { type: RelationType.GreaterThanOrEqualTo, value: timestampValue, bucket: timestampValue } } })
            await mongo.insertOne({  streamId, id, count: 1, relations, members: [memberId], timeStamp: timestampValue });
        }
    } else {
        const largerIndex: undefined | MongoFragment = (await mongo.find({  id, streamId }).sort({ timeStamp: -1 }).limit(1).toArray())[0];
        const relations: MongoFragment["relations"] = [];

        if (!!largerIndex) {
            relations.push({ type: RelationType.GreaterThanOrEqualTo, value: largerIndex.timeStamp!, bucket: largerIndex.timeStamp! });

            await mongo.updateOne({  streamId, id, timeStamp: largerIndex.timeStamp, path }, { "$push": { relations: { type: RelationType.LessThan, value: largerIndex.timeStamp!, bucket: timestampValue } } })
        }

        await mongo.insertOne({  streamId, id, count: 1, relations, members: [memberId], timeStamp: timestampValue });
    }
}

