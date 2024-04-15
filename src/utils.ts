import { Stream } from "@ajuvercr/js-runner";
import type * as RDF from "@rdfjs/types";
import { LDES, Member, PROV, RDF as RDFT, SDS } from "@treecg/types";
import { Collection } from "mongodb";
import { Parser, Quad_Object, Store, Writer } from "n3";
import { DataFactory } from "rdf-data-factory";
import winston from "winston";

const consoleTransport = new winston.transports.Console();
consoleTransport.level = process.env.LOG_LEVEL || "debug";

export const logger = winston.createLogger({
  format: winston.format.combine(
    winston.format.colorize({ level: true }),
    winston.format.simple(),
  ),
  transports: [consoleTransport],
});

const df = new DataFactory();

export function serializeRdfThing(thing: {
  id: RDF.Term;
  quads: RDF.Quad[];
}): string {
  const quads = [];
  quads.push(
    df.quad(
      df.namedNode(""),
      df.namedNode("http://purl.org/dc/terms/subject"),
      <Quad_Object>thing.id,
    ),
  );
  quads.push(...thing.quads);

  const filtered = quads.filter(
    (x, i, xs) => xs.findIndex((p) => p.equals(x)) == i,
  );
  return new Writer().quadsToString(filtered);
}

export type DenyQuad = (q: RDF.Quad, currentId: RDF.Term) => boolean;
// Set<String> yikes!
export function filterMember(
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

export function maybe_parse(data: RDF.Quad[] | string): RDF.Quad[] {
  if (typeof data === "string" || data instanceof String) {
    const parse = new Parser();
    return parse.parse(<string>data);
  } else {
    return data;
  }
}

export function getMember(
  subject: RDF.Term,
  store: Store,
  done: Set<RDF.Term>,
): RDF.Quad[] {
  const newQuads = store.getQuads(subject, null, null, null);
  done.add(subject);

  const newSubjects = newQuads
    .map((q) => q.object)
    .filter((q) => q.termType === "BlankNode" || q.termType == "NamedNode")
    .filter((q) => !done.has(q));

  return [
    ...newQuads,
    ...newSubjects.flatMap((s) => getMember(s, store, done)),
  ];
}

export function getMembersByType(type: RDF.Term, store: Store): Member[] {
  return store.getSubjects(RDFT.terms.type, type, null).map((sub) => {
    if (sub.termType !== "NamedNode") throw "Memmbers can only be named nodes!";

    const quads = getMember(sub, store, new Set());
    return { id: sub, quads };
  });
}

export function extractBucketStrategies(meta: RDF.Quad[]): Member[] {
  const store = new Store(meta);
  return getMembersByType(LDES.terms.BucketizeStrategy, store);
}

export async function setup_metadata(
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
