import { Processor, type Reader } from "@rdfc/js-runner";
import { Member, PROV, RDF as RDFT, SDS } from "@treecg/types";
import { Writer } from "n3";
import { Extract, Extractor } from "./extractor";
import { getRepository, IndexBulkOperations, Repository, } from "./repositories/Repository";
import { filterMember, maybe_parse, pathString } from "./utils";

export type IngestSDSArgs = {
    data: Reader;
    metadata: Reader;
    database: DBConfig;
};

export type DBConfig = {
    url: string;
    metadata: string;
    data: string;
    index: string;
};

export class IngestSDS extends Processor<IngestSDSArgs> {
    repository: Repository;
    recordCount: number = 0;

    async init(this: IngestSDSArgs & this): Promise<void> {
        this.repository = getRepository(this.database);
        await this.repository.open();

        const dbFragmentations: Member[] =
            await this.repository.findMetadataFragmentations();

        this.logger.debug(
            `[init] Found ${dbFragmentations.length} fragmentations (${dbFragmentations.map(
                (x) => x.id.value,
            )})`,
        );

        await this.repository.createIndices();
        this.logger.info("[init] IngestSDS initialized over a " + this.repository.getStoreType() + " data store");
    }

    async transform(this: IngestSDSArgs & this): Promise<void> {
        // Process data and metadata in parallel
        await Promise.all([
            this.processData(this.data.strings()),
            this.processMetadata(this.metadata.strings()),
        ]);

        // Close the repository after both data and metadata processing is done
        await this.repository.close();
    }

    async produce(this: IngestSDSArgs & this): Promise<void> {
        // No new data is produced by this processor.
    }

    async processData(iterable: AsyncIterable<string>): Promise<void> {
        const extractor = new Extractor();
        for await (const item of iterable) {
            const data = maybe_parse(item);

            this.logger.debug(`[processData] Handling ingest for record with payload id: <${data.find(
                (q) => q.predicate.equals(SDS.terms.payload)
            )?.object?.value}>`);

            const extract = extractor.extract_quads(data);
            const indexOperations: IndexBulkOperations = this.repository.prepareIndexBulk();
            await this.handleRecords(extract, indexOperations);
            await this.handleRelations(extract, indexOperations);
            await this.handleBuckets(extract, indexOperations);

            await this.repository.ingestIndexBulk(indexOperations);
            this.recordCount++;
        }
        this.logger.info(`[processData] Ingested ${this.recordCount} records`);
    }

    async processMetadata(iterable: AsyncIterable<string>): Promise<void> {
        for await (const item of iterable) {
            const meta = maybe_parse(item);

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
                        q.predicate.equals(PROV.terms.used) &&
                        q.object.equals(id),
                    (q, id) =>
                        q.predicate.equals(SDS.terms.dataset) &&
                        q.object.equals(id),
                ]);

                const ser = new Writer().quadsToString(streamMember);

                await this.repository.ingestMetadata(
                    SDS.Stream,
                    streamId.value,
                    ser,
                );
            }
        }
    }

    async handleRecords(
        extract: Extract,
        operations: IndexBulkOperations,
    ) {
        const dataSer = new Writer().quadsToString(extract.getData());

        const records = extract.getRecords();
        // only set the data if the id does not exist
        const dataOperations = this.repository.prepareDataBulk();

        for (const rec of records) {
            if (!rec.dataless) {
                await this.repository.handleRecord(rec, dataSer, dataOperations);
            }
        }

        await this.repository.ingestDataBulk(dataOperations);

        // Add this payload as a member to the correct buckets
        for (const rec of records) {
            for (const bucket of rec.buckets) {
                await this.repository.handleMember(rec, bucket, operations);
            }
        }
    }

    async handleBuckets(
        extract: Extract,
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
            await this.repository.handleBucket(bucket, operations);
        }
    }

    async handleRelations(
        extract: Extract,
        operations: IndexBulkOperations,
    ) {
        // Remove old relations
        const removeRelations = extract.getRemoveRelations();

        for (const rel of removeRelations) {
            const pathValue = await pathString(rel.path);
            const valueValue = await pathString(rel.value);

            await this.repository.removeRelation(
                rel,
                pathValue,
                valueValue,
                operations,
            );
        }

        // Add new relations
        const relations = extract.getRelations();

        for (const rel of relations) {
            const pathValue = await pathString(rel.path);
            const valueValue = await pathString(rel.value);

            await this.repository.handleRelation(
                rel,
                pathValue,
                valueValue,
                operations,
            );
        }
    }
}
