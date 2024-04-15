import type * as RDF from "@rdfjs/types";
import { Stream } from "@ajuvercr/js-runner";
import { Collection, MongoClient } from "mongodb";
import { Writer } from "n3";
import { handleTimestampPath } from "./fragmentHelper";
import { handleBucketData } from "./sdsIngestor";
import { filterMember, logger, maybe_parse, setup_metadata } from "./utils";
import {
  DataRecord,
  FragmentExtension,
  Relation,
  SDSRecord,
  TREEFragment,
} from "./types";
import { SDS } from "@treecg/types";

const DATABASE_COLLECTION = {
  index: "INDEX",
  data: "DATA",
  relations: "RELATIONS",
  metadata: "META",
};

function gatherRecords(
  data: RDF.Quad[],
  timestampPaths: { [stream: string]: string },
): SDSRecord[] {
  const out: SDSRecord[] = [];

  for (const recordId of data
    .filter((q) => q.predicate.equals(SDS.terms.payload))
    .map((x) => x.subject)) {
    const stream = data.find(
      (q) => q.subject.equals(recordId) && q.predicate.equals(SDS.terms.stream),
    )?.object.value;
    if (!stream) {
      logger.error("Found SDS record without a stream!");
      continue;
    }
    const payload = data.find(
      (q) =>
        q.subject.equals(recordId) && q.predicate.equals(SDS.terms.payload),
    )!.object;
    const buckets = data
      .filter(
        (q) =>
          q.subject.equals(recordId) && q.predicate.equals(SDS.terms.bucket),
      )
      .map((x) => x.object);

    const tPath = timestampPaths[stream];

    const timestampValue = tPath
      ? data.find(
          (q) => q.subject.equals(payload) && q.predicate.value === tPath,
        )?.object.value
      : undefined;

    out.push({
      stream,
      payload,
      buckets,
      timestampValue,
    });
  }

  return out;
}

// This could be a memory problem in the long run
// TODO: Find a way to persist written records efficiently
const addedMembers: Set<string> = new Set();
async function addDataRecord(
  updateRecords: DataRecord[],
  record: SDSRecord<true>,
  quads: RDF.Quad[],
  collection: Collection<DataRecord>,
) {
  const value = record.payload.value;

  // Check if record has been registered in the local memory
  if (addedMembers.has(value)) return;
  addedMembers.add(value);

  // Check if record is already written in the collection
  const present = (await collection.countDocuments({ id: value })) > 0;
  if (present) return;

  const member = filterMember(quads, record.payload, [
    (q) => q.predicate.equals(SDS.terms.payload),
  ]);

  const ser = new Writer().quadsToString(member);

  updateRecords.push({
    id: value,
    data: ser,
    timestamp: record.timestampValue,
  });
}

export async function ingest(
  data: Stream<string | RDF.Quad[]>,
  metadata: Stream<string | RDF.Quad[]>,
  database: string,
  maxSize: number = 100,
  k: number = 4,
  minBucketSpan: number = 300,
) {
  const url = database || "mongodb://localhost:27017/ldes";
  const mongo = await new MongoClient(url).connect();
  const db = mongo.db();

  logger.debug(`[ingest] Connected to ${url}`);

  const streamTimestampPaths: { [streamId: string]: string } = {};

  let ingestMetadata = true;
  let ingestData = true;
  let closed = false;

  const closeMongo = () => {
    if (!ingestMetadata && !ingestData && !closed) {
      logger.info("[ingest] Closing MongoDB client connection");
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
    db.collection(DATABASE_COLLECTION.metadata),
    (k, v) => (streamTimestampPaths[k] = v),
    closeMongo,
  );
  logger.debug("[ingest] Attached metadata handler");

  const memberCollection = db.collection<DataRecord>(DATABASE_COLLECTION.data);
  const indexCollection = db.collection<TREEFragment & FragmentExtension>(
    DATABASE_COLLECTION.index,
  );

  const relationCollection = db.collection<Relation>(
    DATABASE_COLLECTION.relations,
  );

  data.data(async (input: RDF.Quad[] | string) => {
    const data = maybe_parse(input);
    if (!ingestData) {
      logger.error("Cannot handle data, mongo is closed");
      return;
    }

    // Format member objects in preparation for storage writing
    const records: SDSRecord<true>[] = gatherRecords(data, streamTimestampPaths)
      .filter((r) => {
        if (!r.timestampValue) {
          logger.error("Cannot ingest record without timestamp value");
          return false;
        } else {
          return true;
        }
      })
      .map((x) => {
        const out = <SDSRecord<true>>(<any>x);
        out.timestampValue = new Date(x.timestampValue!);
        return out;
      });

    records.sort(
      (a, b) => a.timestampValue.getTime() - b.timestampValue.getTime(),
    );

    logger.debug(`[ingest] Handling ${records.length} record(s)`);

    // Make sure duplicated members are skipped
    const updateData: DataRecord[] = [];
    for (const r of records) {
      await addDataRecord(updateData, r, data, memberCollection);
    }

    // Write members to DATA collection
    if (updateData.length > 0) {
      // Do we really need to await this?
      await memberCollection.insertMany(updateData);
      logger.debug(
        `[ingest] Inserted ${updateData.length} new members to the data collection`,
      );
    }

    // Update INDEX collection accordingly
    for (const r of records) {
      await handleTimestampPath(
        r,
        streamTimestampPaths[r.stream],
        indexCollection,
        memberCollection,
        relationCollection,
        maxSize,
        k,
        minBucketSpan,
        logger,
      );
    }

    // If the fragmentation strategy is the default timestamp-based
    // we need to handle the labeling of buckets/fragments as immutable
    // based on the current time
    // Last known member timestamp
    const lastMemberTimestamp = records[records.length - 1].timestampValue;

    if (
      records[0].timestampValue.getTime() >
      records[records.length - 1].timestampValue.getTime()
    ) {
      throw "Arthur messed up";
    }
    // Gather all mutable fragments that have expired

    // TODO: Check if we can directly update all expired fragments
    // Not sure how to add the timestamp and the span while querying
    const expiredBuckets = await indexCollection
      .find({
        timeStamp: { $lte: lastMemberTimestamp },
        immutable: false,
      })
      .toArray();

    for (const buck of expiredBuckets) {
      const expDate = buck.timeStamp!.getTime() + buck.span;
      if (expDate < lastMemberTimestamp.getTime()) {
        logger.debug(
          `Labeling bucket ${buck.timeStamp?.toISOString()} (span: ${
            buck.span
          }) as immutable`,
        );
        // Label these buckets as immutable
        await indexCollection.updateOne(buck, {
          $set: { immutable: true },
        });
      }
    }
  });

  logger.debug("[ingest] Attached data handler");
}

export async function sds_ingest(
  data: Stream<string | RDF.Quad[]>,
  metadata: Stream<string | RDF.Quad[]>,
  database: string,
) {
  const url = database;
  const mongo = await new MongoClient(url).connect();
  const db = mongo.db();

  logger.debug(`[ingest] Connected to ${url}`);

  const streamTimestampPaths: { [streamId: string]: string } = {};

  let ingestMetadata = true;
  let ingestData = true;
  let closed = false;

  const closeMongo = () => {
    if (!ingestMetadata && !ingestData && !closed) {
      logger.info("[ingest] Closing MongoDB client connection");
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
    db.collection(DATABASE_COLLECTION.metadata),
    (k, v) => (streamTimestampPaths[k] = v),
    closeMongo,
  );
  logger.debug("[ingest] Attached metadata handler");

  const memberCollection = db.collection<DataRecord>(DATABASE_COLLECTION.data);
  const indexCollection = db.collection<TREEFragment>(
    DATABASE_COLLECTION.index,
  );
  const relationCollection = db.collection<Relation>(
    DATABASE_COLLECTION.relations,
  );

  data.data(async (input: RDF.Quad[] | string) => {
    if (!ingestData) {
      logger.error("Cannot handle data, mongo is closed");
      return;
    }

    await handleBucketData(<string>input, {
      index: indexCollection,
      relations: relationCollection,
      member: memberCollection,
    });
    return;
  });

  logger.debug("[ingest] Attached data handler");
}
