import { Collection } from "mongodb";
import { Parser, Writer } from "n3";
import { CBDShapeExtractor, Extractor } from "sds-processors";
import { RelationType } from "@treecg/types";
import { DataRecord, Relation, TREEFragment } from "./types";
import { serializeRdfThing } from "./utils";

export type MongoCollections = {
  member: Collection<DataRecord>;
  index: Collection<TREEFragment>;
  relations: Collection<Relation>;
};

const extractor = new Extractor(new CBDShapeExtractor(), undefined);

export async function handleBucketData(input: string, db: MongoCollections) {
  const quads = new Parser().parse(input);

  const records = await extractor.parse_records(quads);

  // Add data objects
  for (let record of records) {
    const rdf = new Writer().quadsToString(record.data.quads);
    await db.member.updateOne(
      {
        id: record.data.id.value,
      },
      {
        $setOnInsert: { data: rdf },
      },
      { upsert: true },
    );

    if (record.bucket) {
      await db.index.updateOne(
        { id: record.bucket.id.value, streamId: record.stream.value },
        {
          $addToSet: { members: record.data.id.value },
          $set: {
            root: record.bucket.root || false,
            immutable: record.bucket.immutable || false,
          },
        },
        {
          upsert: true,
        },
      );
    }
  }

  let relationsChecked = 0;
  // Doing the relation things
  const done = new Set<string>();
  for (let record of records) {
    let bucket = record.bucket;
    while (bucket) {
      // Let's do a thing
      if (done.has(bucket.id.value)) {
        break;
      }
      done.add(bucket.id.value);

      // make sure the bucket exits
      await db.index.updateOne(
        { id: bucket.id.value, streamId: record.stream.value },
        {
          $set: {
            root: bucket.root || false,
            immutable: bucket.immutable || false,
          },
        },
        {
          upsert: true,
        },
      );

      // Add the relation
      if (bucket.parent) {
        relationsChecked += 1;
        const rel = bucket.parent.links.find(
          (x) => x.target.value == bucket!.id.value,
        )!;

        const path = rel.path ? serializeRdfThing(rel.path) : undefined;

        const value = rel.value
          ? serializeRdfThing({ id: rel.value, quads: [] })
          : undefined;

        await db.relations.updateOne(
          {
            from: bucket.parent.id.value,
            bucket: rel.target.value,
          },
          {
            $setOnInsert: {
              from: bucket.parent.id.value,
              bucket: rel.target.value,
              type: <RelationType>rel.type.value,
              value,
              path,
            },
          },
          { upsert: true },
        );
      }

      bucket = bucket.parent;
    }
  }
}
