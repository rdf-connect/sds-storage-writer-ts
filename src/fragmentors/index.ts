
import type * as RDF from '@rdfjs/types';
import { Member, RelationType } from '@treecg/types';
import { Collection } from "mongodb";

export type MongoFragment = { leaf: boolean, ids: string[], fragmentId: string, relations: { type: RelationType, values: string[], bucket: string }[], members?: string[], count: number, timeStamp?: string };
export type ExtractIndices = (member: Member, mongo: Collection<MongoFragment>, timestampPath?: RDF.Term) => Promise<void>;
export interface Fragment {
    id: string,
    extract: ExtractIndices,
}

const maxSize = 2;

//TODO! timestampValue is a date
export async function handleTimestampPath(ids: string[], fragmentId: string, timestampValue: string, memberId: string, mongo: Collection<MongoFragment>) {
    const smallerIndex: undefined | MongoFragment = (await mongo.find({ leaf: true, ids, timeStamp: { $lte: timestampValue }, fragmentId }).sort({ timeStamp: -1 }).limit(1).toArray())[0];

    if (smallerIndex) {
        if (smallerIndex.count < maxSize || timestampValue === smallerIndex.timeStamp) {
            await mongo.updateOne({ leaf: true, fragmentId, ids, timeStamp: smallerIndex.timeStamp }, { $inc: { count: 1 }, $push: { members: memberId } });
        } else {
            const largerIndex: undefined | MongoFragment = (await mongo.find({ leaf: true, ids, timeStamp: { $gt: timestampValue }, fragmentId }).sort({ timeStamp: -1 }).limit(1).toArray())[0];

            const relations: MongoFragment["relations"] = [{ type: RelationType.LessThan, values: [timestampValue], bucket: smallerIndex.timeStamp! }];

            if (!!largerIndex) {
                relations.push({ type: RelationType.GreaterThanOrEqualTo, values: [largerIndex.timeStamp!], bucket: largerIndex.timeStamp! });
                await mongo.updateOne({ leaf: true, fragmentId, ids, timeStamp: largerIndex.timeStamp }, { "$push": { relations: { type: RelationType.LessThan, values: [largerIndex.timeStamp!], bucket: timestampValue } } })
            }

            await mongo.updateOne({ leaf: true, fragmentId, ids, timeStamp: smallerIndex.timeStamp }, { "$push": { relations: { type: RelationType.GreaterThanOrEqualTo, values: [timestampValue], bucket: timestampValue } } })
            await mongo.insertOne({ leaf: true, fragmentId, ids, count: 1, relations, members: [memberId], timeStamp: timestampValue });
        }
    } else {
        const largerIndex: undefined | MongoFragment = (await mongo.find({ leaf: true, ids, fragmentId }).sort({ timeStamp: -1 }).limit(1).toArray())[0];

        const relations: MongoFragment["relations"] = [];

        if (!!largerIndex) {
            relations.push({ type: RelationType.GreaterThanOrEqualTo, values: [largerIndex.timeStamp!], bucket: largerIndex.timeStamp! });

            await mongo.updateOne({ leaf: true, fragmentId, ids, timeStamp: largerIndex.timeStamp }, { "$push": { relations: { type: RelationType.LessThan, values: [largerIndex.timeStamp!], bucket: timestampValue } } })
        }

        await mongo.insertOne({ leaf: true, fragmentId, ids, count: 1, relations, members: [memberId], timeStamp: timestampValue });
    }
}