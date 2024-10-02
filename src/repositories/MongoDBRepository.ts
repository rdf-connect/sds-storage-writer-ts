import { Repository } from "./Repository";
import { AnyBulkWriteOperation, Db, MongoClient } from "mongodb";
import { getLoggerFor } from "../utils/logUtil";
import { Lock } from "async-await-mutex-lock";
import { Member } from "@treecg/types";
import { Parser } from "n3";
import { TREEFragment } from "../fragmentHelper";
import { DataRecord } from "../index";
import { Bucket, Record, Relation } from "../extractor";

export class MongoDBRepository implements Repository {
    protected url: string;
    protected metadata: string;
    protected data: string;
    protected index: string;

    protected client: MongoClient;
    protected db: Db;

    protected logger = getLoggerFor(this);
    protected lock = new Lock();

    constructor(url: string, metadata: string, data: string, index: string) {
        this.url = url;
        this.metadata = metadata;
        this.data = data;
        this.index = index;
    }

    async open(): Promise<void> {
        this.client = await new MongoClient(this.url).connect();
        this.db = this.client.db();

        this.logger.debug(`Connected to ${this.url}`);
    }

    async close(): Promise<void> {
        await this.client.close();

        this.logger.debug(`Closed connection to ${this.url}`);
    }

    async ingestMetadata(
        type: string,
        id: string,
        value: string,
    ): Promise<void> {
        await this.lock.acquire("metaMongoDB");
        try {
            await this.db
                .collection(this.metadata)
                .updateOne({ type, id }, { $set: { value } }, { upsert: true });
        } finally {
            this.lock.release("metaMongoDB");
        }
    }

    async findMetadataFragmentations(): Promise<Member[]> {
        return await this.db
            .collection(this.metadata)
            .find({
                type: "fragmentation",
            })
            .map((entry) => {
                return {
                    id: entry.id,
                    quads: new Parser().parse(entry.value),
                };
            })
            .toArray();
    }

    async createIndices(): Promise<void> {
        await this.db.collection(this.data).createIndex({ id: 1 });
        await this.db
            .collection(this.index)
            .createIndex({ streamId: 1, id: 1 });
    }

    prepareDataBulk(): AnyBulkWriteOperation<DataRecord>[] {
        return [];
    }

    async ingestDataBulk(
        bulk: AnyBulkWriteOperation<DataRecord>[],
    ): Promise<void> {
        await this.lock.acquire("dataMongoDB");
        try {
            await this.db.collection<DataRecord>(this.data).bulkWrite(bulk);
        } finally {
            this.lock.release("dataMongoDB");
        }
    }

    async handleRecord(
        record: Record,
        data: string,
        bulk: AnyBulkWriteOperation<DataRecord>[],
    ): Promise<void> {
        bulk.push({
            updateOne: {
                filter: {
                    id: record.payload,
                },
                update: {
                    $setOnInsert: {
                        data: data,
                    },
                },
                upsert: true,
            },
        });
    }

    prepareIndexBulk(): AnyBulkWriteOperation<TREEFragment>[] {
        return [];
    }

    async ingestIndexBulk(
        bulk: AnyBulkWriteOperation<TREEFragment>[],
    ): Promise<void> {
        await this.lock.acquire("indexMongoDB");
        try {
            await this.db.collection<TREEFragment>(this.index).bulkWrite(bulk);
        } finally {
            this.lock.release("indexMongoDB");
        }
    }

    async handleMember(
        record: Record,
        bucket: string,
        bulk: AnyBulkWriteOperation<TREEFragment>[],
    ): Promise<void> {
        bulk.push({
            updateOne: {
                filter: {
                    streamId: record.stream,
                    id: bucket,
                },
                update: {
                    $addToSet: { members: record.payload },
                },
                upsert: true,
            },
        });
    }

    async handleBucket(
        bucket: Bucket,
        parameters: any,
        bulk: AnyBulkWriteOperation<TREEFragment>[],
    ): Promise<void> {
        bulk.push({
            updateOne: {
                filter: {
                    streamId: bucket.stream,
                    id: bucket.id,
                },
                update: {
                    $set: parameters,
                },
                upsert: true,
            },
        });
    }

    async handleRelation(
        relation: Relation,
        path: string | undefined,
        value: string | undefined,
        bulk: AnyBulkWriteOperation<TREEFragment>[],
    ): Promise<void> {
        bulk.push({
            updateOne: {
                filter: {
                    streamId: relation.stream,
                    id: relation.origin,
                },
                update: {
                    $addToSet: {
                        relations: {
                            bucket: relation.bucket,
                            path: path,
                            type: relation.type,
                            value: value,
                        },
                    },
                },
                upsert: true,
            },
        });
    }
}