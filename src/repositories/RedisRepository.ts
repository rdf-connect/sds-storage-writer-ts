import { Repository } from "./Repository";
import { getLoggerFor } from "../utils/logUtil";
import { Lock } from "async-await-mutex-lock";
import { createClient, RedisClientType } from "redis";
import { Member } from "@treecg/types";
import { DataFactory, Parser } from "n3";
import { Bucket, Record, Relation } from "../extractor";
import namedNode = DataFactory.namedNode;

export class RedisRepository implements Repository {
    protected url: string;
    protected metadata: string;
    protected data: string;
    protected index: string;

    protected client: RedisClientType;

    protected logger = getLoggerFor(this);
    protected lock = new Lock();

    constructor(url: string, metadata: string, data: string, index: string) {
        this.url = url;
        this.metadata = metadata;
        this.data = data;
        this.index = index;
    }

    async open(): Promise<void> {
        this.client = createClient({ url: this.url });
        await this.client.connect();

        this.logger.debug(`Connected to ${this.url}`);
    }

    async close(): Promise<void> {
        await this.client.disconnect();

        this.logger.debug(`Closed connection to ${this.url}`);
    }

    async ingestMetadata(
        type: string,
        id: string,
        value: string,
    ): Promise<void> {
        await this.lock.acquire("metaRedis");
        try {
            await this.client.set(
                `${this.metadata}:${encodeURIComponent(type)}:${encodeURIComponent(id)}`,
                value,
            );
        } finally {
            this.lock.release("metaRedis");
        }
    }

    async findMetadataFragmentations(): Promise<Member[]> {
        const keys = await this.client.keys(`${this.metadata}:fragmentation:*`);
        if (keys.length === 0) {
            return [];
        }
        const entries = await this.client.mGet(keys);

        return entries.map((entry, i) => {
            const key = keys[i].split(":");
            return {
                id: namedNode(encodeURIComponent(key[2])),
                quads: new Parser().parse(entry!),
            };
        });
    }

    async createIndices(): Promise<void> {
        // Empty
    }

    prepareDataBulk(): Promise<string | null>[] {
        return [];
    }

    async ingestDataBulk(bulk: Promise<string | null>[]): Promise<void> {
        await Promise.all(bulk);
    }

    async handleRecord(
        record: Record,
        data: string,
        bulk: Promise<string | null>[],
    ): Promise<void> {
        bulk.push(
            this.client.set(
                `${this.data}:${encodeURIComponent(record.payload)}`,
                data,
            ),
        );
    }

    prepareIndexBulk(): Promise<string | number | null>[] {
        return [];
    }

    async ingestIndexBulk(
        bulk: Promise<string | number | null>[],
    ): Promise<void> {
        await Promise.all(bulk);
    }

    async handleMember(
        record: Record,
        bucket: string,
        bulk: Promise<string | number | null>[],
    ): Promise<void> {
        bulk.push(
            this.client.sAdd(
                `${this.index}:${encodeURIComponent(record.stream)}:${encodeURIComponent(bucket)}:members`,
                record.payload,
            ),
        );
    }

    async handleBucket(
        bucket: Bucket,
        parameters: any,
        bulk: Promise<string | number | null>[],
    ): Promise<void> {
        // If parameters contains `members`, use the handleMember method and remove the key.
        if (parameters.members || parameters.empty) {
            bulk.push(
                this.client.del(
                    `${this.index}:${encodeURIComponent(bucket.stream)}:${encodeURIComponent(bucket.id)}:members`,
                ),
            );
            for (const member of parameters.members) {
                await this.handleMember(member, bucket.id, bulk);
            }
            delete parameters.members;
        }

        // Set streamId and id parameters
        parameters.streamId = bucket.stream;
        parameters.id = bucket.id;

        bulk.push(
            this.client.json.set(
                `${this.index}:${encodeURIComponent(bucket.stream)}:${encodeURIComponent(bucket.id)}`,
                ".",
                parameters,
            ),
        );
    }

    async handleRelation(
        relation: Relation,
        path: string | undefined,
        value: string | undefined,
        bulk: Promise<string | number | null>[],
    ): Promise<void> {
        bulk.push(
            this.client.sAdd(
                `${this.index}:${encodeURIComponent(relation.stream)}:${encodeURIComponent(relation.origin)}:relations`,
                JSON.stringify({
                    type: relation.type,
                    stream: relation.stream,
                    origin: relation.origin,
                    bucket: relation.bucket,
                    path: path,
                    value: value,
                }),
            ),
        );
    }
}
