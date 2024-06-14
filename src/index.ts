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
import { Collection, MongoClient } from "mongodb";
import { Parser, Writer } from "n3";
import { env } from "process";
import winston from "winston";
import { handleTimestampPath, TREEFragment } from "./fragmentHelper";

const consoleTransport = new winston.transports.Console();
const logger = winston.createLogger({
   format: winston.format.combine(
      winston.format.colorize({ level: true }),
      winston.format.simple(),
   ),
   transports: [consoleTransport],
});

consoleTransport.level = process.env.LOG_LEVEL || "info";

type SDSRecord = {
   stream: string;
   payload: RDF.Term;
   buckets: RDF.Term[];
   timestampValue?: string;
};

export type DataRecord = {
   id: string,
   data: string,
   timestamp: Date
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

type DenyQuad = (q: RDF.Quad, currentId: RDF.Term) => boolean;

// Set<String> yikes!
function filterMember(
   quads: RDF.Quad[],
   id: RDF.Term,
   blacklist: DenyQuad[] = [],
   done?: Set<String>,
): RDF.Quad[] {
   const d: Set<String> = done === undefined ? new Set() : done;
   const quadIsBlacklisted = (q: RDF.Quad) => blacklist.some((b) => b(q, id));
   d.add(id.value);

   const out: RDF.Quad[] = quads.filter(
      (q) => q.subject.equals(id) && !quadIsBlacklisted(q),
   );
   const newObjects = quads
      .filter((q) => q.subject.equals(id) && !quadIsBlacklisted(q))
      .map((q) => q.object)
      .filter((o) => o.termType === "BlankNode" || o.termType === "NamedNode");
   for (let id of newObjects) {
      if (d.has(id.value)) continue;
      out.push(...filterMember(quads, id, blacklist, d));
   }

   const newSubjects = quads
      .filter((q) => q.object.equals(id) && !quadIsBlacklisted(q))
      .map((q) => q.subject)
      .filter((o) => o.termType === "BlankNode" || o.termType === "NamedNode");
   for (let id of newSubjects) {
      if (d.has(id.value)) continue;
      out.push(...filterMember(quads, id, blacklist, d));
   }

   return out;
}

function maybe_parse(data: RDF.Quad[] | string): RDF.Quad[] {
   if (typeof data === "string" || data instanceof String) {
      const parse = new Parser();
      return parse.parse(<string>data);
   } else {
      return data;
   }
}

function getRelatedBuckets(
   quads: RDF.Quad[],
   bucket: RDF.Term,
   done: Set<string>,
): RDF.Term[] {
   const set: RDF.Term[] = [];
   const get = (q: RDF.Term) => {
      if (done.has(q.value)) {
         return;
      }
      done.add(q.value);
      set.push(q);
      // Find forward relations
      quads
         .filter(
            (x) => x.subject.equals(q) && x.predicate.equals(SDS.terms.relation),
         )
         .map((x) => x.object)
         .flatMap((bn) =>
            quads
               .filter(
                  (q) =>
                     q.subject.equals(bn) &&
                     q.predicate.equals(SDS.terms.relationBucket),
               )
               .map((x) => x.object),
         )
         .forEach(get);

      // Find backwards relations
      quads
         .filter(
            (x) =>
               x.object.equals(q) && x.predicate.equals(SDS.terms.relationBucket),
         )
         .map((x) => x.subject)
         .flatMap((bn) =>
            quads
               .filter(
                  (q) =>
                     q.object.equals(bn) && q.predicate.equals(SDS.terms.relation),
               )
               .map((x) => x.subject),
         )
         .forEach(get);
   };

   get(bucket);
   return set;
}

function parseBool(bo?: string): boolean {
   if (!bo) {
      return false;
   } else {
      const bos = bo.toLowerCase();
      return bos === "t" || bos === "true" || bos === "1";
   }
}

function gatherBuckets(
   buckets: Bucket[],
   data: RDF.Quad[],
   subject: RDF.Term,
   stream: string,
   found: Set<string>,
) {
   for (let bucket of getRelatedBuckets(data, subject, found)) {
      const isRoot = data.find(
         (q) =>
            q.subject.equals(bucket) &&
            q.predicate.equals(SDS.terms.custom("isRoot")),
      )?.object.value;
      const immutable = data.find(
         (q) =>
            q.subject.equals(bucket) &&
            q.predicate.equals(SDS.terms.custom("immutable")),
      )?.object.value;
      const b = {
         root: isRoot === "true",
         id: bucket.value,
         relations: <Relation[]>[],
         stream,
         immutable: parseBool(immutable),
      };

      const relations = data
         .filter(
            (q) =>
               q.subject.equals(bucket) && q.predicate.equals(SDS.terms.relation),
         )
         .map((x) => x.object);
      for (let rel of relations) {
         const relObj = data.filter((q) => q.subject.equals(rel));

         const type = <RelationType>(
            relObj.find((q) => q.predicate.equals(SDS.terms.relationType))!.object
               .value
         );
         const target = relObj.find((q) =>
            q.predicate.equals(SDS.terms.relationBucket),
         )!.object.value;
         const path = relObj.find((q) =>
            q.predicate.equals(SDS.terms.relationPath),
         )?.object.value;
         const value = relObj.find((q) =>
            q.predicate.equals(SDS.terms.relationValue),
         )?.object.value;

         b.relations.push({ type, bucket: target, path, value });
      }

      buckets.push(b);
   }
}

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
//const addedMembers: Set<string> = new Set();
async function addDataRecord(
   updateRecords: DataRecord[],
   record: SDSRecord,
   quads: RDF.Quad[],
   collection: Collection<DataRecord>,
) {
   const value = record.payload.value;

   // Check if record has been registered in the local memory
   //if (addedMembers.has(value)) return;
   //addedMembers.add(value);

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
      timestamp: new Date(record.timestampValue!),
   });
}

const setRoots: Set<string> = new Set();
const immutables: Set<string> = new Set();
async function addBucket(
   bucket: Bucket,
   collection: Collection<TREEFragment>,
) {
   // Handle root setting
   if (bucket.root && !setRoots.has(bucket.stream)) {
      setRoots.add(bucket.stream);
      await collection.updateOne(
         { streamId: bucket.stream, id: bucket.id },
         {
            $set: { root: true },
         },
         { upsert: true },
      );
   }

   if (bucket.immutable && !immutables.has(bucket.id)) {
      immutables.add(bucket.id);
      await collection.updateOne(
         { streamId: bucket.stream, id: bucket.id },
         {
            $set: { immutable: true },
         },
         { upsert: true },
      );
   }

   for (let newRelation of bucket.relations) {
      await collection.updateOne(
         { streamId: bucket.stream, id: bucket.id },
         {
            $push: { relations: newRelation },
         },
         { upsert: true },
      );
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
      `Found ${dbFragmentations.length} fragmentations (${dbFragmentations.map(
         (x) => x.id.value,
      )})`,
   );

   const handleMetadata = async (meta: string | RDF.Quad[]) => {
      meta = maybe_parse(meta);
      if (!ingestMetadata) {
         logger.error("Cannot handle metadata, mongo is closed");
         return;
      }

      const streams = meta
         .filter(
            (q) =>
               q.predicate.equals(RDFT.terms.type) &&
               q.object.equals(SDS.terms.Stream),
         )
         .map((q) => q.subject);

      for (let streamId of streams) {
         const streamMember = filterMember(meta, streamId, [
            (q, id) => q.predicate.equals(PROV.terms.used) && q.object.equals(id),
            (q, id) => q.predicate.equals(SDS.terms.dataset) && q.object.equals(id),
         ]);

         const datasetId = streamMember.find(
            (q) =>
               q.subject.equals(streamId) && q.predicate.equals(SDS.terms.dataset),
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
      handleMetadata(metadata.lastElement);
   }
}

export async function ingest(
   data: Stream<string | RDF.Quad[]>,
   metadata: Stream<string | RDF.Quad[]>,
   database: DBConfig,
   maxSize: number = 100,
   k: number = 4,
   minBucketSpan: number = 300
) {
   const url = database.url || env.DB_CONN_STRING || "mongodb://localhost:27017/ldes";
   const mongo = await new MongoClient(url).connect();
   const db = mongo.db();

   logger.debug(`[ingest] Connected to ${url}`);

   const streamTimestampPaths: { [streamId: string]: string } = {};

   let ingestMetadata = true;
   let ingestData = true;
   let closed = false;
   let lastMemberTimestamp = null;

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
      db.collection(database.metadata),
      (k, v) => (streamTimestampPaths[k] = v),
      closeMongo,
   );
   logger.debug("[ingest] Attached metadata handler");

   const memberCollection = db.collection<DataRecord>(database.data);
   const indexCollection = db.collection<TREEFragment>(database.index);

   const pushMemberToDB = async (record: SDSRecord) => {
      const bs = record.buckets;
      if (bs.length === 0) {
         await indexCollection.updateOne(
            { root: true, streamId: record.stream, id: "" },
            { $push: { members: record.payload.value } },
            { upsert: true },
         );
      } else {
         for (let bucket of bs) {
            await indexCollection.updateOne(
               { streamId: record.stream, id: bucket.value },
               { $push: { members: record.payload.value } },
               { upsert: true },
            );
         }
      }
   };

   const pushTimstampMemberToDB = async (record: SDSRecord) => {
      const bs = record.buckets;
      const timestamp = new Date(record.timestampValue!);

      if (bs.length === 0) {
         await handleTimestampPath(
            null,
            record.stream,
            streamTimestampPaths[record.stream],
            timestamp,
            record.payload.value,
            indexCollection,
            memberCollection,
            maxSize,
            k,
            minBucketSpan,
            logger
         );
      } else {
         for (let bucket of bs) {
            await handleTimestampPath(
               bucket.value,
               record.stream,
               streamTimestampPaths[record.stream],
               timestamp,
               record.payload.value,
               indexCollection,
               memberCollection,
               maxSize,
               k,
               minBucketSpan,
               logger
            );
         }
      }
   };

   data.data(async (input: RDF.Quad[] | string) => {
      const data = maybe_parse(input);
      if (!ingestData) {
         logger.error("Cannot handle data, mongo is closed");
         return;
      }

      // Format member objects in preparation for storage writing
      const records = gatherRecords(data, streamTimestampPaths);
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
         logger.debug(`[ingest] Inserted ${updateData.length} new members to the data collection`);
      }

      // Update INDEX collection accordingly
      for (const r of records) {
         await (r.timestampValue ? pushTimstampMemberToDB(r) : pushMemberToDB(r));
      }

      const isDefaultTimestamp = records[0].timestampValue !== undefined;

      // If the fragmentation strategy is the default timestamp-based
      // we need to handle the labeling of buckets/fragments as immutable
      // based on the current time
      if (isDefaultTimestamp) {
         // Last known member timestamp
         lastMemberTimestamp = new Date(records[records.length -1].timestampValue!)
         // Gather all mutable fragments that have expired

         // TODO: Check if we can directly update all expired fragments
         // Not sure how to add the timestamp and the span while querying
         const expiredBuckets = await indexCollection.find({
            timeStamp: { $lte: lastMemberTimestamp },
            immutable: false
         }).toArray();

         for (const buck of expiredBuckets) {
            const expDate = buck.timeStamp!.getTime() + buck.span;
            if (expDate < lastMemberTimestamp.getTime()) {
               logger.debug(`Labeling bucket ${buck.timeStamp?.toISOString()} (span: ${buck.span}) as immutable`);
               // Label these buckets as immutable
               await indexCollection.updateOne(buck, {
                  $set: { immutable: true }
               });
            }
         }
      }

      // This next steps are only performed if the fragmentation strategy
      // is already defined in the SDS metadata and the stream is not relying
      // on the default timestamp-based strategy, which is handled within the
      // pushTimstampMemberToDB() function.
      if (!isDefaultTimestamp) {
         const found: Set<string> = new Set();
         const buckets: Bucket[] = [];

         for (const r of records) {
            r.buckets.forEach((b) =>
               gatherBuckets(buckets, data, b, r.stream, found),
            );
         }

         for (let bucket of buckets) {
            await addBucket(bucket, indexCollection);
         }
      }
   });

   logger.debug("[ingest] Attached data handler");
}
