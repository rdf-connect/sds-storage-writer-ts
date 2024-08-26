import type * as RDF from "@rdfjs/types";
import { Stream } from "@rdfc/js-runner";
import {
    LDES,
    Member,
    PROV,
    RDF as RDFT,
    RelationType,
    SDS,
} from "@treecg/types";
import { AnyBulkWriteOperation, Collection, MongoClient } from "mongodb";
import { DataFactory, Parser, Quad_Object, Writer } from "n3";
import { env } from "process";
import { TREEFragment } from "./fragmentHelper";
import { getLoggerFor } from "./utils/logUtil";
import { Extract, Extractor, RdfThing } from "./extractor";
/* @ts-expect-error no type declaration available */
import canonize from "rdf-canonize";

const logger = getLoggerFor("ingest");

function maybe_parse(data: RDF.Quad[] | string): RDF.Quad[] {
    if (typeof data === "string" || data instanceof String) {
        const parse = new Parser();
        return parse.parse(<string>data);
    } else {
        return data;
    }
}

type DenyQuad = (q: RDF.Quad, currentId: RDF.Term) => boolean;

// Set<String> yikes!
function filterMember(
    quads: RDF.Quad[],
    id: RDF.Term,
    blacklist: DenyQuad[] = [],
    done?: Set<string>,
): RDF.Quad[] {
    const d: Set<string> = done === undefined ? new Set() : done;
    const quadIsBlacklisted = (q: RDF.Quad) => blacklist.some((b) => b(q, id));
    d.add(id.value);

    const out: RDF.Quad[] = quads.filter(
        (q) => q.subject.equals(id) && !quadIsBlacklisted(q),
    );
    const newObjects = quads
        .filter((q) => q.subject.equals(id) && !quadIsBlacklisted(q))
        .map((q) => q.object)
        .filter(
            (o) => o.termType === "BlankNode" || o.termType === "NamedNode",
        );
    for (const id of newObjects) {
        if (d.has(id.value)) continue;
        out.push(...filterMember(quads, id, blacklist, d));
    }

    const newSubjects = quads
        .filter((q) => q.object.equals(id) && !quadIsBlacklisted(q))
        .map((q) => q.subject)
        .filter(
            (o) => o.termType === "BlankNode" || o.termType === "NamedNode",
        );
    for (const id of newSubjects) {
        if (d.has(id.value)) continue;
        out.push(...filterMember(quads, id, blacklist, d));
    }

    return out;
}

export type DataRecord = {
    id: string;
    data: string;
};

type Relation = {
    type: RelationType;
    value?: string;
    bucket: string;
    path?: string;
};

type Bucket = {
    id: string;
    root: boolean;
    stream: string;
    relations: Relation[];
    immutable?: boolean;
};

export type DBConfig = {
    url: string;
    metadata: string;
    data: string;
    index: string;
};

function emptyBuckets(
    data: RDF.Quad[],
    operations: AnyBulkWriteOperation<TREEFragment>[],
) {
    const bucketsToEmpty = data
        .filter(
            (q) =>
                q.predicate.equals(SDS.terms.custom("empty")) &&
                q.object.value === "true" &&
                q.graph.equals(SDS.terms.custom("DataDescription")),
        )
        .map((q) => q.subject);

    // Only perform the operation if there are buckets to empty.
    if (bucketsToEmpty.length === 0) {
        return;
    }

    operations.push({
        updateMany: {
            filter: { id: { $in: bucketsToEmpty.map((b) => b.value) } },
            update: {
                $set: { members: [] },
            },
            upsert: true,
        },
    });
}

async function handleRecords(
    extract: Extract,
    collection: Collection<DataRecord>,
    operations: AnyBulkWriteOperation<TREEFragment>[],
) {
    const dataSer = new Writer().quadsToString(extract.getData());

    const records = extract.getRecords();
    // only set the data if the id does not exist
    const bulkUpdate: AnyBulkWriteOperation<DataRecord>[] = records.map(
        (rec) => ({
            updateOne: {
                filter: {
                    id: rec.payload,
                },
                update: {
                    $setOnInsert: {
                        data: dataSer,
                    },
                },
                upsert: true,
            },
        }),
    );

    await collection.bulkWrite(bulkUpdate);

    // Add this payload as a member to the correct buckets
    for (const rec of records) {
        for (const bucket of rec.buckets) {
            operations.push({
                updateOne: {
                    filter: {
                        streamId: rec.stream,
                        id: bucket,
                    },
                    update: {
                        $addToSet: { members: rec.payload },
                    },
                    upsert: true,
                },
            });
        }
    }
}

function handleBuckets(
    extract: Extract,
    operations: AnyBulkWriteOperation<TREEFragment>[],
) {
    const buckets = extract.getBuckets();

    for (const bucket of buckets) {
        const set: {
            immutable?: boolean;
            root?: boolean;
            empty?: boolean;
            members?: string[];
        } = {};
        if (bucket.root !== undefined) {
            set.root = bucket.root;
        }

        if (bucket.empty !== undefined) {
            set.empty = bucket.empty;
            set.members = [];
        }

        if (bucket.immutable !== undefined) {
            set.immutable = bucket.immutable;
        }

        operations.push({
            updateOne: {
                filter: {
                    streamId: bucket.stream,
                    id: bucket.id,
                },
                update: {
                    $set: set,
                },
                upsert: true,
            },
        });
    }
}

const df = DataFactory;
async function pathString(thing?: RdfThing): Promise<string | undefined> {
    if (!thing) {
        return;
    }

    const quads = [
        df.quad(
            df.namedNode(""),
            df.namedNode("http://purl.org/dc/terms/subject"),
            <Quad_Object>thing.id,
        ),
        ...thing.quads.map((x) => df.quad(x.subject, x.predicate, x.object)),
    ];
    const canonical = await canonize.canonize(quads, { algorithm: "RDFC-1.0" });
    return canonical;
}

async function handleRelations(
    extract: Extract,
    operations: AnyBulkWriteOperation<TREEFragment>[],
) {
    const relations = extract.getRelations();

    for (const rel of relations) {
        const pathValue = await pathString(rel.path);
        const valueValue = await pathString(rel.value);

        operations.push({
            updateOne: {
                filter: {
                    streamId: rel.stream,
                    id: rel.origin,
                },
                update: {
                    $addToSet: {
                        relations: {
                            bucket: rel.bucket,
                            path: pathValue,
                            type: rel.type,
                            value: valueValue,
                        },
                    },
                },
                upsert: true,
            },
        });
    }
}

async function setup_metadata(
    metadata: Stream<string | RDF.Quad[]>,
    metaCollection: Collection,
    setTimestamp: (stream: string, value: string) => void,
    onClose: () => void,
) {
    let ingestMetadata = true;

    metadata.on("end", () => {
        ingestMetadata = false;
        return onClose();
    });

    const dbFragmentations: Member[] = await metaCollection
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

    logger.debug(
        `[setup_metadata] Found ${dbFragmentations.length} fragmentations (${dbFragmentations.map(
            (x) => x.id.value,
        )})`,
    );

    const handleMetadata = async (meta: string | RDF.Quad[]) => {
        meta = maybe_parse(meta);
        if (!ingestMetadata) {
            logger.error(
                "[setup_metadata] Cannot handle metadata, mongo is closed",
            );
            return;
        }

        const streams = meta
            .filter(
                (q) =>
                    q.predicate.equals(RDFT.terms.type) &&
                    q.object.equals(SDS.terms.Stream),
            )
            .map((q) => q.subject);

        for (const streamId of streams) {
            const streamMember = filterMember(meta, streamId, [
                (q, id) =>
                    q.predicate.equals(PROV.terms.used) && q.object.equals(id),
                (q, id) =>
                    q.predicate.equals(SDS.terms.dataset) &&
                    q.object.equals(id),
            ]);

            const datasetId = streamMember.find(
                (q) =>
                    q.subject.equals(streamId) &&
                    q.predicate.equals(SDS.terms.dataset),
            )?.object;
            if (datasetId) {
                const timestampPathObject = streamMember.find(
                    (q) =>
                        q.subject.equals(datasetId) &&
                        q.predicate.equals(LDES.terms.timestampPath),
                )?.object;
                if (timestampPathObject) {
                    setTimestamp(streamId.value, timestampPathObject.value);
                }
            }

            const ser = new Writer().quadsToString(streamMember);
            await metaCollection.updateOne(
                { type: SDS.Stream, id: streamId.value },
                { $set: { value: ser } },
                { upsert: true },
            );
        }
    };

    metadata.data(handleMetadata);

    if (metadata.lastElement) {
        await handleMetadata(metadata.lastElement);
    }
}

export async function ingest(
    data: Stream<string | RDF.Quad[]>,
    metadata: Stream<string | RDF.Quad[]>,
    database: DBConfig,
) {
    const url =
        database.url || env.DB_CONN_STRING || "mongodb://localhost:27017/ldes";
    const mongo = await new MongoClient(url).connect();
    const db = mongo.db();

    logger.debug(`Connected to ${url}`);

    const streamTimestampPaths: { [streamId: string]: string } = {};

    const ingestMetadata = true;
    let ingestData = true;
    let closed = false;

    const closeMongo = () => {
        if (!ingestMetadata && !ingestData && !closed) {
            logger.info("Closing MongoDB client connection");
            closed = true;
            return mongo.close();
        }
    };

    data.on("end", () => {
        ingestData = false;
        return closeMongo();
    });

    await setup_metadata(
        metadata,
        db.collection(database.metadata),
        (k, v) => (streamTimestampPaths[k] = v),
        closeMongo,
    );
    logger.debug("Attached metadata handler");

    const memberCollection = db.collection<DataRecord>(database.data);
    await memberCollection.createIndex({ id: 1 });
    const indexCollection = db.collection<TREEFragment>(database.index);
    await indexCollection.createIndex({ streamId: 1, id: 1 });

    const extractor = new Extractor();

    data.data(async (input: RDF.Quad[] | string) => {
        const data = maybe_parse(input);
        if (!ingestData) {
            logger.error("Cannot handle data, mongo is closed");
            return;
        }

        const extract = extractor.extract_quads(data);
        const indexOperations: AnyBulkWriteOperation<TREEFragment>[] = [];

        await handleRecords(extract, memberCollection, indexOperations);
        handleBuckets(extract, indexOperations);
        await handleRelations(extract, indexOperations);

        await indexCollection.bulkWrite(indexOperations);
    });

    logger.debug("Attached data handler");
}
