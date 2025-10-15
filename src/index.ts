import type * as RDF from "@rdfjs/types";
import { Processor, type Reader, type Writer } from "@rdfc/js-runner";
import { LDES, Member, PROV, RDF as RDFT, SDS } from "@treecg/types";
import { DataFactory, Parser, Quad_Object, Writer as N3Writer } from "n3";
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
    const dataSer = new N3Writer().quadsToString(extract.getData());

    const records = extract.getRecords();
    // only set the data if the id does not exist
    const dataOperations = repository.prepareDataBulk();

    for (const rec of records) {
        if (!rec.dataless) {
            await repository.handleRecord(rec, dataSer, dataOperations);
        }
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
        if (!bucket.root) {
            delete bucket.root;
        }
        if (!bucket.immutable) {
            delete bucket.immutable;
        }
        await repository.handleBucket(bucket, operations);
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
    // Remove old relations
    const removeRelations = extract.getRemoveRelations();

    for (const rel of removeRelations) {
        const pathValue = await pathString(rel.path);
        const valueValue = await pathString(rel.value);

        await repository.removeRelation(rel, pathValue, valueValue, operations);
    }

    // Add new relations
    const relations = extract.getRelations();

    for (const rel of relations) {
        const pathValue = await pathString(rel.path);
        const valueValue = await pathString(rel.value);

        await repository.handleRelation(rel, pathValue, valueValue, operations);
    }
}

async function handleMetadata(
    metadata: string | RDF.Quad[],
    repository: Repository,
    ingestMetadata: boolean
) {
    const meta = maybe_parse(metadata);
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

        const ser = new N3Writer().quadsToString(streamMember);

        await repository.ingestMetadata(SDS.Stream, streamId.value, ser);
    }
}


export async function ingest(
    data: Reader,
    metadata: Reader,
    database: DBConfig,
) {
    const repository = await (ingestInit(data, metadata, database));
    await (ingestTransform(data, metadata, repository));
}

async function ingestInit(
    data: Reader,
    metadata: Reader,
    database: DBConfig,) 
{
    const repository = getRepository(database);
    await repository.open();

    const dbFragmentations: Promise<Member[]> = repository.findMetadataFragmentations();
    dbFragmentations.then(frag => 
        logger.debug(
            `Found ${frag.length} fragmentations (${frag.map(
                (x) => x.id.value,
            )})`,
        )
    );

    await repository.createIndices();
    return repository;
}

function ingestTransform(
    data: Reader,
    metadata: Reader,
    repository: Repository
) {

    let ingestMetadata = true;
    let ingestData = true;
    let closed = false;

    const closeRepository = () => {
        if (!ingestMetadata && !ingestData && !closed) {
            logger.info("Closing repository client connection");
            closed = true;
            return repository.close();
        }
    };

    const extractor = new Extractor();

    // Helper to process one iterator
    const processDataIterator = async (iter: AsyncIterable<string>) => {
        for await (const item of iter) {
            const data = maybe_parse(item);
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
        }
        ingestData = false;
        logger.info("[Ingest] Data stream closed.");
        return await closeRepository();
    };

    // Helper to process one iterator
    const processMetadataIterator = async (iter: AsyncIterable<string>) => {
        for await (const meta of iter) {
            logger.debug(`[Ingest] -- processing metadata entry\n${meta}`);
            await handleMetadata(meta, repository, ingestMetadata);
        }
        ingestMetadata = false;
        return await closeRepository();
        
    };

    processDataIterator(data.strings());
    processMetadataIterator(metadata.strings());
}




type Args = {
    dataInput: Reader,
    metadataInput: Reader,
    database: DBConfig,
};

export class Ingest extends Processor<Args> {
    
    repository: Repository;
    async init(this: Args & this): Promise<void> {
        this.repository = await ingestInit(
            this.dataInput, 
            this.metadataInput, 
            this.database
        );
        logger.debug("[Ingest] initialized the Ingest processor.");
    }
    async transform(this: Args & this): Promise<void> {
        ingestTransform(this.dataInput, this.metadataInput, this.repository);
        logger.debug("[Ingest] transform section initialized.");
    }
    async produce(this: Args & this): Promise<void> {
        logger.debug("[Ingest] produce function called.");
    }
}
