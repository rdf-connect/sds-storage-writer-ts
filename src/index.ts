import type * as RDF from '@rdfjs/types';
import { Stream } from "@treecg/connector-types";
import { LDES, Member, PROV, RDF as RDFT, SDS } from '@treecg/types';
import { MongoClient } from "mongodb";
import { Parser, Store, Writer } from "n3";
import { env } from "process";
import winston from 'winston';
import { Fragment, MongoFragment } from './fragmentors';
import { TimestampFragmentor } from './fragmentors/timestamp';
import { extractBucketStrategies, getMember, interpretBucketstrategy } from './utils';

const consoleTransport = new winston.transports.Console();
const logger = winston.createLogger({
    format: winston.format.combine(
        winston.format.colorize({ level: true }),
        winston.format.simple()
    ), transports: [consoleTransport]
});

consoleTransport.level = process.env.LOG_LEVEL || "info";

type SR<T> = {
    [P in keyof T]: Stream<T[P]>;
}

type Data = {
    data: RDF.Quad[],
    metadata: RDF.Quad[],
}


export async function ingest(sr: SR<Data>, metacollection: string, dataCollection: string, indexCollectionName: string, timestampFragmentation?: string, mUrl?: string) {
    let timestampPath: RDF.Term | undefined = undefined;

    const url = mUrl || env.DB_CONN_STRING || "mongodb://localhost:27017/ldes";
    logger.debug("Using mongo url " + url);

    // Connect to database
    const mongo = await new MongoClient(url).connect();
    const db = mongo.db();


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

    sr.metadata.on("end", () => {
        ingestMetadata = false;
        return closeMongo();
    });

    sr.data.on("end", () => {
        ingestData = false;
        return closeMongo();
    });

    const metaCollection = db.collection(metacollection);
    const dbFragmentations: Member[] = await metaCollection.find({ "type": "fragmentation" })
        .map(entry => { return { id: entry.id, quads: new Parser().parse(entry.value) } })
        .toArray();
    logger.debug(`Found ${dbFragmentations.length} fragmentations (${dbFragmentations.map(x => x.id.value)})`);

    let state: Fragment[] = dbFragmentations.map(interpretBucketstrategy);

    if (timestampFragmentation) {
        logger.debug("Adding timestamp fragmentation");
        state.push(new TimestampFragmentor(timestampFragmentation));
    }

    const updateFragmentation = async (id: string, quads: RDF.Quad[]) => {
        const ser = new Writer().quadsToString(quads);
        await metaCollection.updateOne({ "type": "fragmentation", "id": id }, { $set: { value: ser } }, { upsert: true });
    };

    const handleMetadata = async (meta: RDF.Quad[]) => {
        if (!ingestMetadata) {
            logger.error("Cannot handle metadata, mongo is closed");
            return;
        }

        const store = new Store(meta);

        const stream = store.getSubjects(RDFT.terms.type, SDS.terms.Stream, null)
            .find(sub => store.getQuads(null, PROV.terms.used, sub, null).length === 0);
        const streamMember = getMember(stream!, store, new Set());

        if (stream) {
            const datasetId = store.getObjects(stream!, SDS.terms.dataset, null)[0];
            if (datasetId) {
                timestampPath = store.getObjects(datasetId, LDES.terms.timestampPath, null)[0];
            }

            logger.debug(`Update metadata for ${stream.value} (datasetId ${datasetId?.value}, timestampPath ${timestampPath?.value})`);

            const ser = new Writer().quadsToString(streamMember);
            await metaCollection.updateOne({ "type": SDS.Stream, "id": stream!.value }, { $set: { value: ser } }, { upsert: true });
        }

        const bucketStrategies = extractBucketStrategies(meta);

        logger.debug(`Found ${bucketStrategies.length} fragmentations (${bucketStrategies.map(x => x.id.value)})`);
        state = bucketStrategies.map(interpretBucketstrategy);

        if (timestampFragmentation) {
            logger.debug("Adding timestamp fragmentation");
            state.push(new TimestampFragmentor(timestampFragmentation));
        }

        await Promise.all(bucketStrategies.map(member => updateFragmentation(member.id.value, member.quads)));
    };

    sr.metadata.data(handleMetadata);

    if (sr.metadata.lastElement) {
        handleMetadata(sr.metadata.lastElement);
    }

    const memberCollection = db.collection(dataCollection);
    const indexCollection = db.collection<MongoFragment>(indexCollectionName);

    sr.data.data(async (data) => {
        if (!ingestData) {
            logger.error("Cannot handle data, mongo is closed");
            return;
        }

        const id = data[0].subject;
        const present = await memberCollection.count({ id: id.value }) > 0;

        if (!present) {
            logger.debug("Adding member " + id.value);
            let timestampValue = undefined;

            if (!!timestampPath) {
                timestampValue = data.find(quad => quad.subject.equals(id) && quad.predicate.equals(timestampPath!))?.object.value
            }

            const ser = new Writer().quadsToString(data);
            await memberCollection.insertOne({ id: id.value, data: ser, timestamp: timestampValue });
        }

        const currentFragmentations = await indexCollection.find({ memberId: id }).map(entry => <string>entry.fragmentId).toArray();
        const fragmentIsNotPresent = (fragment: Fragment) => currentFragmentations.every(seen => seen != fragment.id);

        const promises = state.filter(fragmentIsNotPresent).map(fragment => {
            return fragment.extract({ id, quads: data }, indexCollection, timestampPath);
        });

        await Promise.all(promises);
    });
}

