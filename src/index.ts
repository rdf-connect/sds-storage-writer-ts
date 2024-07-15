import type * as RDF from "@rdfjs/types";
import {Stream} from "@rdfc/js-runner";
import {LDES, Member, PROV, RDF as RDFT, RelationType, SDS,} from "@treecg/types";
import {AnyBulkWriteOperation, Collection, MongoClient} from "mongodb";
import {Parser, Writer} from "n3";
import {env} from "process";
import winston from "winston";
import {TREEFragment} from "./fragmentHelper";

const consoleTransport = new winston.transports.Console();
const logger = winston.createLogger({
   format: winston.format.combine(
      winston.format.colorize({level: true}),
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

function parseBool(bo?: string): boolean | undefined {
   if (bo === undefined) {
      return undefined;
   } else if (!bo) {
      return false;
   } else {
      const bos = bo.toLowerCase();
      return bos === "t" || bos === "true" || bos === "1";
   }
}

function gatherBuckets(
   data: RDF.Quad[],
): Bucket[] {
   const buckets: Bucket[] = [];

   const bucketTerms: Set<RDF.Term> = new Set();
   data.filter(q =>
      q.predicate.equals(RDFT.terms.type)
      && q.object.equals(SDS.terms.custom("Bucket"))
      && q.graph.equals(SDS.terms.custom("DataDescription")))
      .map(q => q.subject).forEach(bucket => bucketTerms.add(bucket));

   data.filter(q =>
      q.predicate.equals(SDS.terms.bucket)
      && q.graph.equals(SDS.terms.custom("DataDescription")))
      .map(q => q.object).forEach(bucket => bucketTerms.add(bucket));

   bucketTerms.forEach(bucket => {

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

      // If the subject has a triple with the Stream, use that one. Otherwise, we need to find the stream via the record the bucket is attached to.
      const stream = data.find(
         (q) =>
            q.subject.equals(bucket) &&
            q.predicate.equals(SDS.terms.stream),
      )?.object.value || data.find(
         (q) =>
            q.subject.equals(data.find(
               (record) =>
                  record.predicate.equals(SDS.terms.bucket) &&
                  record.object.equals(bucket),
            )?.subject) &&
            q.predicate.equals(SDS.terms.stream),
      )?.object.value;
      if (!stream) {
         logger.error(`Found SDS bucket without a stream! ${bucket.value}`);
         return;
      }

      const b = {
         root: isRoot === "true",
         id: bucket.value,
         relations: <Relation[]>[],
         stream,
         immutable: parseBool(immutable),
      };

      // Find all relations of the bucket
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

         b.relations.push({type, bucket: target, path, value});
      }

      buckets.push(b);
   });
   return buckets;
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

function emptyBuckets(data: RDF.Quad[], operations: AnyBulkWriteOperation<TREEFragment>[]) {
   const bucketsToEmpty = data.filter(q =>
      q.predicate.equals(SDS.terms.custom("empty")) &&
      q.object.value === "true" &&
      q.graph.equals(SDS.terms.custom("DataDescription")))
      .map(q => q.subject);

   operations.push({
      updateMany: {
         filter: {id: {$in: bucketsToEmpty.map(b => b.value)}},
         update: {
            $set: {members: []},
         },
         upsert: true,
      }
   });
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

   const member = filterMember(quads, record.payload, [
      (q) => q.predicate.equals(SDS.terms.payload),
   ]);

   if (!member?.length) {
      return;
   }

   // Check if record is already written in the collection
   const present = (await collection.countDocuments({id: value}, {limit: 1})) > 0;
   if (present) return;


   const ser = new Writer().quadsToString(member);

   updateRecords.push({
      id: value,
      data: ser,
      timestamp: new Date(record.timestampValue!),
   });
}

const setRoots: Set<string> = new Set();
const immutables: Set<string> = new Set();

function addBucket(
   bucket: Bucket,
   operations: AnyBulkWriteOperation<TREEFragment>[],
) {
   const set: any = {};
   // Handle root setting
   if (bucket.root && !setRoots.has(bucket.stream)) {
      setRoots.add(bucket.stream);
      set.root = true;
   }

   if (bucket.immutable && !immutables.has(bucket.id)) {
      immutables.add(bucket.id);
      set.immutable = true;
   } else if (bucket.immutable === false) {
      // If it is explicitly set as false, set it in the database, so we persist the bucket.
      set.immutable = false;
   }

   operations.push({
      updateOne: {
         filter: {streamId: bucket.stream, id: bucket.id},
         update: {
            $set: set,
            $addToSet: {relations: {$each: bucket.relations}},
         },
         upsert: true,
      }
   });
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
            {type: SDS.Stream, id: streamId.value},
            {$set: {value: ser}},
            {upsert: true},
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
   const url = database.url || env.DB_CONN_STRING || "mongodb://localhost:27017/ldes";
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
      db.collection(database.metadata),
      (k, v) => (streamTimestampPaths[k] = v),
      closeMongo,
   );
   logger.debug("[ingest] Attached metadata handler");

   const memberCollection = db.collection<DataRecord>(database.data);
   const indexCollection = db.collection<TREEFragment>(database.index);

   const pushMemberToDB = (record: SDSRecord, operations: AnyBulkWriteOperation<TREEFragment>[]) => {
      const bs = record.buckets;
      if (bs.length === 0) {
         operations.push({
            updateOne: {
               filter: {root: true, streamId: record.stream, id: ""},
               update: {
                  $addToSet: {members: record.payload.value},
               },
               upsert: true,
            }
         });
      } else {
         operations.push({
            updateMany: {
               filter: {streamId: record.stream, id: {$in: bs.map(b => b.value)}},
               update: {
                  $addToSet: {members: record.payload.value},
               },
               upsert: true,
            }
         });
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
      const indexOperations: AnyBulkWriteOperation<TREEFragment>[] = [];
      for (const r of records) {
         pushMemberToDB(r, indexOperations);
      }

      const buckets: Bucket[] = gatherBuckets(data);
      for (let bucket of buckets) {
         addBucket(bucket, indexOperations);
      }

      // Empty buckets that are marked so.
      emptyBuckets(data, indexOperations);

      await indexCollection.bulkWrite(indexOperations);
   });

   logger.debug("[ingest] Attached data handler");
}
