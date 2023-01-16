import type * as RDF from '@rdfjs/types';
import { Stream } from "@treecg/connector-types";
import { LDES, Member, PROV, RDF as RDFT, RelationType, SDS } from '@treecg/types';
import { MongoClient } from "mongodb";
import { Parser, Writer } from "n3";
import { env } from "process";
import winston from 'winston';
import { handleTimestampPath, MongoFragment } from './fragmentHelper';

const consoleTransport = new winston.transports.Console();
const logger = winston.createLogger({
    format: winston.format.combine(
        winston.format.colorize({ level: true }),
        winston.format.simple()
    ), transports: [consoleTransport]
});

consoleTransport.level = process.env.LOG_LEVEL || "debug";

type SR<T> = {
    [P in keyof T]: Stream<T[P]>;
}

type Data = {
    data: RDF.Quad[],
    metadata: RDF.Quad[],
}


type DenyQuad = (q: RDF.Quad, currentId: RDF.Term) => boolean;

// Set<String> yikes!
function filterMember(quads: RDF.Quad[], id: RDF.Term, blacklist: DenyQuad[] = [], done?: Set<String>): RDF.Quad[] {
    const d: Set<String> = done === undefined ? new Set() : done;
    const quadIsBlacklisted = (q: RDF.Quad) => blacklist.some(b => b(q, id));
    d.add(id.value);

    const out: RDF.Quad[] = quads.filter(q => q.subject.equals(id) && !quadIsBlacklisted(q));
    const newObjects = quads.filter(q => q.subject.equals(id) && !quadIsBlacklisted(q)).map(q => q.object).filter(o => o.termType === "BlankNode" || o.termType === "NamedNode");
    for (let id of newObjects) {
        if (d.has(id.value)) continue;
        out.push(...filterMember(quads, id, blacklist, d));
    }

    const newSubjects = quads.filter(q => q.object.equals(id) && !quadIsBlacklisted(q)).map(q => q.subject).filter(o => o.termType === "BlankNode" || o.termType === "NamedNode");
    for (let id of newSubjects) {
        if (d.has(id.value)) continue;
        out.push(...filterMember(quads, id, blacklist, d));
    }

    return out;
}


export async function ingest(
    data: Stream<RDF.Quad[]>,
    metadata: Stream<RDF.Quad[]>,
    metacollection: string,
    dataCollection: string,
    indexCollectionName: string,
    mUrl?: string,
    maxSize = 10
) {
    console.log({ data, metadata, metacollection, dataCollection, indexCollectionName, mUrl, maxSize });
    const url = mUrl || env.DB_CONN_STRING || "mongodb://localhost:27017/ldes";
    logger.debug("Using mongo url " + url);

    // Connect to database
    const mongo = await new MongoClient(url).connect();

    const db = mongo.db();
    logger.debug("Connected");

    const streamTimestampPaths: { [streamId: string]: RDF.Term } = {};

    let ingestMetadata = true;
    let ingestData = true;
    let closed = false;

    const closeMongo = () => {
        if (!ingestMetadata && !ingestData && !closed) {
            logger.info("Closing mongo");
            closed = true;
            return mongo.close();
        }
    };

    metadata.on("end", () => {
        ingestMetadata = false;
        return closeMongo();
    });

    data.on("end", () => {
        ingestData = false;
        return closeMongo();
    });

    console.log("Done setting up end callbacks");
    const metaCollection = db.collection(metacollection);
    console.log("Found meta collection ", metaCollection);
    const dbFragmentations: Member[] = await metaCollection.find({ "type": "fragmentation" })
        .map(entry => { console.log("Found entry", entry); return { id: entry.id, quads: new Parser().parse(entry.value) } })
        .toArray();
    logger.debug(`Found ${dbFragmentations.length} fragmentations (${dbFragmentations.map(x => x.id.value)})`);

    const handleMetadata = async (meta: RDF.Quad[]) => {
        console.log("ingest: Handling metadata")
        if (!ingestMetadata) {
            logger.error("Cannot handle metadata, mongo is closed");
            return;
        }

        const streams = meta.filter(q => q.predicate.equals(RDFT.terms.type) && q.object.equals(SDS.terms.Stream)).map(q => q.subject);

        for (let streamId of streams) {
            const streamMember = filterMember(meta, streamId, [
                (q, id) => q.predicate.equals(PROV.terms.used) && q.object.equals(id),
                (q, id) => q.predicate.equals(SDS.terms.dataset) && q.object.equals(id),
            ]);

            const datasetId = streamMember.find(q => q.subject.equals(streamId) && q.predicate.equals(SDS.terms.dataset))?.object;
            if (datasetId) {
                const timestampPathObject = streamMember.find(q => q.subject.equals(datasetId) && q.predicate.equals(LDES.terms.timestampPath))?.object;
                if (timestampPathObject) {
                    streamTimestampPaths[streamId.value] = timestampPathObject;
                }
            }

            const timestampPath = streamTimestampPaths[streamId.value];
            logger.debug(`Update metadata for ${streamId.value} (datasetId ${datasetId?.value}, timestampPath ${timestampPath?.value})`);

            const ser = new Writer().quadsToString(streamMember);
            await metaCollection.updateOne({ "type": SDS.Stream, "id": streamId.value }, { $set: { value: ser } }, { upsert: true });
        }
    };

    metadata.data(handleMetadata);

    if (metadata.lastElement) {
        handleMetadata(metadata.lastElement);
    }

    console.log("Attached metadata handler");

    const memberCollection = db.collection(dataCollection);
    const indexCollection = db.collection<MongoFragment>(indexCollectionName);

    data.data(async (data: RDF.Quad[]) => {
        console.log("ingest: Handling data")
        if (!ingestData) {
            logger.error("Cannot handle data, mongo is closed");
            return;
        }


        const records = data.filter(q => q.predicate.equals(SDS.terms.payload));

        const idsDone = new Set<String>();


        const timestampValueCache: { [record: string]: RDF.Term | undefined } = {};
        const getTimestampValue: (record: RDF.Quad) => RDF.Term | undefined = (record) => {
            // only correct use of 'in'
            if (record.value in timestampValueCache) {
                return timestampValueCache[record.subject.value];
            }

            const streamId = data.find(q => q.predicate.equals(SDS.terms.stream) && q.subject.equals(record.subject))?.object;
            const timestampPath = streamId ? streamTimestampPaths[streamId.value] : undefined;

            const timestampValue = timestampPath ? data.find(
                quad => quad.subject.equals(record.object) && quad.predicate.equals(timestampPath)
            )?.object : undefined;

            timestampValueCache[record.subject.value] = timestampValue;

            return timestampValue;
        };

        for (let record of records) {
            const id = record.object;

            if (idsDone.has(id.value)) continue;
            idsDone.add(id.value);


            const present = await memberCollection.count({ id: id.value }) > 0;
            if (present) continue;

            const timestampValue = getTimestampValue(record)?.value;


            logger.debug("Adding member " + id.value);

            const member = filterMember(data, id, [(q) => q.predicate.equals(SDS.terms.payload)]);

            const ser = new Writer().quadsToString(member);
            await memberCollection.insertOne({ id: id.value, data: ser, timestamp: timestampValue });
        }

        const handledRelations: Set<RDF.Term> = new Set();

        for (let r of records) {
            const recordId = r.subject;
            const memberId = r.object;
            const rec = data.filter(q => q.subject.equals(recordId));

            const streamsHandled = await indexCollection.find({ memberId: memberId }).map(entry => <string>entry.streamId).toArray();

            const stream = rec.find(rec => rec.predicate.equals(SDS.terms.stream))?.object;
            // stream not found, can't do anything
            if (!stream) {
                const ser = new Writer().quadsToString(rec);
                logger.warn("Found record without streams\n" + ser);
                continue;
            }
            // stream already handled, gtfo
            if (streamsHandled.some(s => s == stream.value)) { continue; }

            const timestampValue = getTimestampValue(r)?.value;
            const timestampPath = streamTimestampPaths[stream.value];

            const buckets = rec.filter(rec => rec.predicate.equals(SDS.terms.bucket)).map(b => b.object);
            if (buckets.length == 0) {
                console.log("no buckets found, only handling timestamp thing")
                if (timestampValue) {
                    await handleTimestampPath("", stream.value, timestampPath!.value, timestampValue, memberId.value, indexCollection, maxSize);
                } else {
                    // no bucket and no timestamp value :(
                    logger.debug("No timestamp path or bucket found, what is life?");
                    indexCollection.updateOne({ root: true, leaf: true, streamId: stream.value, id: "" }, { $push: { members: memberId.value } }, { upsert: true });
                }
            } else {
                // insert bucket information
                const leaf = !timestampValue;

                for (let bucket of buckets) {
                    const relations = data.filter(q => q.predicate.equals(SDS.terms.relation) && (
                        data.some(q2 => q2.subject.equals(q.object) && q2.predicate.equals(SDS.terms.relationBucket) && q2.object.equals(bucket)) ||
                        q.subject.equals(bucket)
                    ));

                    for (let relation of relations) {
                        const sourceBucket = relation.subject;
                        const relId = relation.object;

                        if (handledRelations.has(relId))
                            continue;
                        handledRelations.add(relId);

                        const relObj = data.filter(q => q.subject.equals(relId));

                        const type = <RelationType>relObj.find(q => q.predicate.equals(SDS.terms.relationType))!.object.value;
                        const target = relObj.find(q => q.predicate.equals(SDS.terms.relationBucket))!.object.value;
                        const path = relObj.find(q => q.predicate.equals(SDS.terms.relationPath))!.object.value;
                        const value = relObj.find(q => q.predicate.equals(SDS.terms.relationValue))!.object.value;

                        const newRelation = { type, value, bucket: target, path };
                        await indexCollection.updateOne({ leaf, streamId: stream.value, id: sourceBucket.value }, { "$push": { relations: newRelation } }, { "upsert": true });
                    }
                }

                if (timestampValue) {
                    console.log(timestampPath!.value);
                    await Promise.all(
                        buckets.map(bucket =>
                            handleTimestampPath(bucket.value, stream.value, timestampPath!.value, timestampValue, memberId.value, indexCollection, maxSize)
                        )
                    );
                } else {
                    await Promise.all(
                        buckets.map(bucket => indexCollection.updateOne({ leaf: true, streamId: stream.value, id: bucket.value }, { $push: { members: memberId.value } }, { upsert: true }))
                    );
                }
            }
        }
    });
    console.log("Attached data handler");
}

