import type * as RDF from "@rdfjs/types";
import { Stream } from "@rdfc/js-runner";
import { LDES, Member, PROV, RDF as RDFT, SDS } from "@treecg/types";
import { DataFactory, Parser, Quad_Object, Writer } from "n3";
import { getLoggerFor } from "./utils/logUtil";
import { Extract, Extractor, RdfThing } from "./extractor";
/* @ts-expect-error no type declaration available */
import canonize from "rdf-canonize";
import { Lock } from "async-await-mutex-lock";
import {
    getRepository,
    IndexBulkOperations,
    Repository,
} from "./repositories/Repository";

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

export type DBConfig = {
    url: string;
    metadata: string;
    data: string;
    index: string;
};

const lock = new Lock();

async function handleRecords(
    extract: Extract,
    repository: Repository,
    operations: IndexBulkOperations,
) {
    const dataSer = new Writer().quadsToString(extract.getData());

    const records = extract.getRecords();
    // only set the data if the id does not exist
    const dataOperations = repository.prepareDataBulk();

    for (const rec of records) {
        await repository.handleRecord(rec, dataSer, dataOperations);
    }

    await repository.ingestDataBulk(dataOperations);

    // Add this payload as a member to the correct buckets
    for (const rec of records) {
        for (const bucket of rec.buckets) {
            await repository.handleMember(rec, bucket, operations);
        }
    }
}

async function handleBuckets(
    extract: Extract,
    repository: Repository,
    operations: IndexBulkOperations,
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

        await repository.handleBucket(bucket, set, operations);
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
    repository: Repository,
    operations: IndexBulkOperations,
) {
    const relations = extract.getRelations();

    for (const rel of relations) {
        const pathValue = await pathString(rel.path);
        const valueValue = await pathString(rel.value);

        await repository.handleRelation(rel, pathValue, valueValue, operations);
    }
}

async function setup_metadata(
    metadata: Stream<string | RDF.Quad[]>,
    repository: Repository,
    setTimestamp: (stream: string, value: string) => void,
    onClose: () => void,
) {
    let ingestMetadata = true;

    metadata.on("end", () => {
        ingestMetadata = false;
        return onClose();
    });

    const dbFragmentations: Member[] =
        await repository.findMetadataFragmentations();

    logger.debug(
        `[setup_metadata] Found ${dbFragmentations.length} fragmentations (${dbFragmentations.map(
            (x) => x.id.value,
        )})`,
    );

    const handleMetadata = async (meta: string | RDF.Quad[]) => {
        meta = maybe_parse(meta);
        if (!ingestMetadata) {
            logger.error(
                "[setup_metadata] Cannot handle metadata, repository is closed",
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

            await repository.ingestMetadata(SDS.Stream, streamId.value, ser);
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
    const repository = getRepository(database);
    await repository.open();

    const streamTimestampPaths: { [streamId: string]: string } = {};

    const ingestMetadata = true;
    let ingestData = true;
    let closed = false;

    const closeRepository = () => {
        if (!ingestMetadata && !ingestData && !closed) {
            logger.info("Closing repository client connection");
            closed = true;
            return repository.close();
        }
    };

    data.on("end", async () => {
        ingestData = false;
        return await closeRepository();
    });

    await setup_metadata(
        metadata,
        repository,
        (k, v) => (streamTimestampPaths[k] = v),
        closeRepository,
    );
    logger.debug("Attached metadata handler");

    await repository.createIndices();

    const extractor = new Extractor();

    data.data(async (input: RDF.Quad[] | string) => {
        const data = maybe_parse(input);
        if (!ingestData) {
            logger.error("Cannot handle data, repository is closed");
            return;
        }
        logger.debug(
            `Handling ingest for '${data.find((q) => q.predicate.equals(SDS.terms.payload))?.object?.value}'`,
        );

        const extract = extractor.extract_quads(data);
        const indexOperations: IndexBulkOperations =
            repository.prepareIndexBulk();

        await handleRecords(extract, repository, indexOperations);
        await handleRelations(extract, repository, indexOperations);
        await handleBuckets(extract, repository, indexOperations);

        await repository.ingestIndexBulk(indexOperations);
    });

    logger.debug("Attached data handler");
}
