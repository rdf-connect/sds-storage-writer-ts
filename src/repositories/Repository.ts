import { DataRecord, DBConfig } from "../index";
import { env } from "process";
import { MongoDBRepository } from "./MongoDBRepository";
import { Member } from "@treecg/types";
import { AnyBulkWriteOperation } from "mongodb";
import { TREEFragment } from "../fragmentHelper";
import { Bucket, Record, Relation } from "../extractor";

export type DataBulkOperations = AnyBulkWriteOperation<DataRecord>[];

export type IndexBulkOperations = AnyBulkWriteOperation<TREEFragment>[];

export interface Repository {
    open(): Promise<void>;

    close(): Promise<void>;

    ingestMetadata(type: string, id: string, value: string): Promise<void>;

    findMetadataFragmentations(): Promise<Member[]>;

    createIndices(): Promise<void>;

    prepareDataBulk(): DataBulkOperations;

    ingestDataBulk(bulk: DataBulkOperations): Promise<void>;

    handleRecord(
        record: Record,
        data: string,
        bulk: DataBulkOperations,
    ): Promise<void>;

    prepareIndexBulk(): IndexBulkOperations;

    ingestIndexBulk(bulk: IndexBulkOperations): Promise<void>;

    handleMember(
        record: Record,
        bucket: string,
        bulk: IndexBulkOperations,
    ): Promise<void>;

    handleBucket(
        bucket: Bucket,
        parameters: any,
        bulk: IndexBulkOperations,
    ): Promise<void>;

    handleRelation(
        relation: Relation,
        path: string | undefined,
        value: string | undefined,
        bulk: IndexBulkOperations,
    ): Promise<void>;
}

export function getRepository(dbConfig: DBConfig): Repository {
    const url =
        dbConfig.url || env.DB_CONN_STRING || "mongodb://localhost:27017/ldes";

    if (url.startsWith("mongodb://")) {
        return new MongoDBRepository(
            url,
            dbConfig.metadata,
            dbConfig.data,
            dbConfig.index,
        );
    } else {
        throw new Error("Unknown database type");
    }
}
