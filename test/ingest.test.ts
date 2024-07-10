import { describe, test, expect, beforeAll, beforeEach, afterAll } from "@jest/globals";
import { SimpleStream } from "@rdfc/js-runner";
import { MongoMemoryServer } from "mongodb-memory-server";
import { Collection, Db, MongoClient } from "mongodb";
import { ingest, DBConfig, DataRecord } from "../src/index";
import { RelationType, SDS } from "@treecg/types";
import { TREEFragment } from "../src/fragmentHelper";

describe("Functional tests for the ingest function", () => {
    /*
    const PREFIXES = `
        @prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix p-plan: <http://purl.org/net/p-plan#> .
        @prefix prov:   <http://www.w3.org/ns/prov#> .
        @prefix sds:    <https://w3id.org/sds#> .
        @prefix dcat:   <https://www.w3.org/ns/dcat#> .
        @prefix xsd:    <http://www.w3.org/2001/XMLSchema#>.
        @prefix sh:     <http://www.w3.org/ns/shacl#>.
        @prefix void:   <http://rdfs.org/ns/void#> .
        @prefix dct:    <http://purl.org/dc/terms/>.
        @prefix ldes:   <https://w3id.org/ldes#>.
        @prefix ex:     <http://example.org/ns#>.
    `;

    const METADATA = `
        ${PREFIXES}
        ex:somePlan a p-plan:Plan;
            rdfs:comment "An epic plan to publish an LDES".

        ex:streamVar0 a p-plan:Variable;
            p-plan:isVariableOfPlan ex:somePlan.

        ex:streamVar1 a p-plan:Variable;
            p-plan:isVariableOfPlan ex:somePlan.

        ex:step0 a p-plan:Step;
            p-plan:hasInputVar ex:someRMLMapping, ex:someOtherRMLMapping;
            p-plan:hasOutputVar ex:streamVar0;
            p-plan:isStepOfPlan <somePlan>;
            rdfs:comment "Map CSV rows to RML";
            p-plan:isStepOfPlan ex:somePlan.

        ex:step1 a p-plan:Step;
            p-plan:isPrecededBy ex:step0;
            p-plan:hasInputVar ex:streamVar0;
            p-plan:hasOutputVar ex:streamVar1;
            p-plan:isStepOfPlan ex:somePlan.

        ex:rmlStream a sds:Stream;
            p-plan:correspondsToVariable ex:streamVar0;
            p-plan:wasGeneratedBy [
                a p-plan:Activity;
                rdfs:comment "Convert an API response in a stream of RDF entities";
                p-plan:correspondsToStep ex:step0;
                prov:used [
                    a dcat:Dataset;
                    dcat:identifier <https://some.remote.api/data>
                ]
            ].

        ex:sdsStream a sds:Stream;
            p-plan:correspondsToVariable ex:streamVar1;
            p-plan:wasGeneratedBy [
                a p-plan:Activity;
                rdfs:comment "Wrap a stream of RDF entities in a SDS stream";
                p-plan:correspondsToStep ex:step1;
                prov:used ex:rmlStream
            ];
            sds:carries [ a sds:Member ];
            sds:dataset [
                a dcat:Dataset;
                dcat:title "Epic dataset";
                dcat:publisher <https://julianrojas.org/#me>;
                ldes:timestampPath dct:modified;
                dcat:identifier <http://some.web.resource/ldes>
            ].
    `;

    let mongod: MongoMemoryServer;
    let client: MongoClient;
    let db: Db;
    let metaColl: Collection;
    let dataColl: Collection<DataRecord>;
    let indexColl: Collection<TREEFragment>;

    beforeAll(async () => {
        // Initialize in-memory MongoDB
        mongod = await MongoMemoryServer.create();
        // Connect to MongoDB instance and create collection managers
        client = await new MongoClient(mongod.getUri()).connect();

        // Initialize database and collection managers
        db = client.db(mongod.instanceInfo!.dbName);

    });

    beforeEach(async () => {
        if (metaColl) {
            await metaColl.drop();
        }
        if (dataColl) {
            await dataColl.drop();
        }
        if (indexColl) {
            await indexColl.drop();
        }

        metaColl = db.collection("METADATA");
        dataColl = db.collection<DataRecord>("DATA");
        indexColl = db.collection<TREEFragment>("INDEX");
    });

    afterAll(async () => {
        if (client) {
            await client.close();
        }
        if (mongod) {
            await mongod.stop({ doCleanup: true, force: true });
        }
    });

    test("Writing a bucketless SDS into MongoDB using the default time-based fragmentation (k = 4, m = 10, n = 100)", async () => {
        const dataStream = new SimpleStream<string>();
        const metadataStream = new SimpleStream<string>();
        const config: DBConfig = {
            url: mongod.getUri(),
            metadata: "METADATA",
            data: "DATA",
            index: "INDEX"
        };

        // Max number of members allowed per fragment
        const m = 10;
        // Number of sub-fragments per level in the default B+Tree structure
        const k = 4;
        // Total number of members to be pushed
        const n = 100

        // Execute ingest function
        await ingest(dataStream, metadataStream, config, m, k);

        // Push metadata in
        await metadataStream.push(METADATA);

        // Push some SDS records in with 1h delta between each other
        for (const record of dataGenerator(n, 1 * 60 * 60 * 1000)) {
            await dataStream.push(record);
        }

        // Check that metadata was stored
        expect(await metaColl.countDocuments()).toBe(2);
        expect((await metaColl.findOne({ "id": "http://example.org/ns#rmlStream" }))!.type).toBe(SDS.Stream);
        expect((await metaColl.findOne({ "id": "http://example.org/ns#sdsStream" }))!.type).toBe(SDS.Stream);
        // Check all data records were persisted
        expect(await dataColl.countDocuments()).toBe(n);
        // Check that all fragments are correct and consistent
        const indexes = await indexColl.find().toArray();

        for (const bucket of indexes) {
            // Check fragment does not violate max members constraint
            expect(bucket.members!.length).toBeLessThanOrEqual(m);
            // Check local members do belong here
            for (const memId of bucket.members!) {
                const member = await dataColl.findOne({ "id": memId });
                expect(member).toBeDefined();
                expect(member!.timestamp.getTime()).toBeGreaterThanOrEqual(bucket.timeStamp!.getTime());
                expect(member!.timestamp.getTime()).toBeLessThan(bucket.timeStamp!.getTime() + bucket.span);
            }
            // Check it contains at most 2k relations (tree:LessThan & tree:GreaterThanOrEqualTo)
            expect(bucket.relations.length).toBeLessThanOrEqual(2 * k);
            // Check that all relations are telling the truth
            for (const rel of bucket.relations) {
                // Fetch related bucket
                const relBucket = (await indexColl.find({ id: rel.bucket }).toArray())[0];
                expect(relBucket).toBeDefined();
                expect(rel.timestampRelation).toBeTruthy();

                if (rel.type === RelationType.GreaterThanOrEqualTo) {
                    expect(new Date(rel.value!).getTime()).toBe(relBucket.timeStamp!.getTime());
                    for (const memRef of relBucket.members!) {
                        const member = await dataColl.findOne({ "id": memRef });
                        expect(member).toBeDefined();
                        expect(member!.timestamp.getTime()).toBeGreaterThanOrEqual(new Date(rel.value!).getTime());
                    }
                } else if (rel.type === RelationType.LessThan) {
                    expect(new Date(rel.value!).getTime())
                        .toBe(new Date(relBucket.timeStamp!.getTime() + relBucket.span).getTime());
                    for (const memRef of relBucket.members!) {
                        const member = await dataColl.findOne({ "id": memRef });
                        expect(member).toBeDefined();
                        expect(member!.timestamp.getTime()).toBeLessThan(new Date(rel.value!).getTime());
                    }
                }
            }
        }
    });

    test("Writing a bucketless SDS into MongoDB using the default time-based fragmentation (k = 3, m = 100, n = 1000)", async () => {
        const dataStream = new SimpleStream<string>();
        const metadataStream = new SimpleStream<string>();
        const config: DBConfig = {
            url: mongod.getUri(),
            metadata: "METADATA",
            data: "DATA",
            index: "INDEX"
        };

        // Max number of members allowed per fragment
        const m = 100;
        // Number of sub-fragments per level in the default B+Tree structure
        const k = 3;
        // Total number of members to be pushed
        const n = 1000;

        // Execute ingest function
        await ingest(dataStream, metadataStream, config, m, k);

        // Push metadata in
        await metadataStream.push(METADATA);

        // Push some SDS records in with 1h delta between each other
        for (const record of dataGenerator(n, 1 * 60 * 60 * 1000)) {
            await dataStream.push(record);
        }

        // Check that metadata was stored
        expect(await metaColl.countDocuments()).toBe(2);
        expect((await metaColl.findOne({ "id": "http://example.org/ns#rmlStream" }))!.type).toBe(SDS.Stream);
        expect((await metaColl.findOne({ "id": "http://example.org/ns#sdsStream" }))!.type).toBe(SDS.Stream);
        // Check all data records were persisted
        expect(await dataColl.countDocuments()).toBe(n);
        // Check that all fragments are correct and consistent
        const indexes = await indexColl.find().toArray();

        for (const bucket of indexes) {
            // Check fragment does not violate max members constraint
            expect(bucket.members!.length).toBeLessThanOrEqual(m);
            // Check local members do belong here
            for (const memId of bucket.members!) {
                const member = await dataColl.findOne({ "id": memId });
                expect(member).toBeDefined();
                expect(member!.timestamp.getTime()).toBeGreaterThanOrEqual(bucket.timeStamp!.getTime());
                expect(member!.timestamp.getTime()).toBeLessThan(bucket.timeStamp!.getTime() + bucket.span);
            }
            // Check it contains at most 2k relations (tree:LessThan & tree:GreaterThanOrEqualTo)
            expect(bucket.relations.length).toBeLessThanOrEqual(2 * k);
            // Check that all relations are telling the truth
            for (const rel of bucket.relations) {
                // Fetch related bucket
                const relBucket = (await indexColl.find({ id: rel.bucket }).toArray())[0];
                expect(relBucket).toBeDefined();
                expect(rel.timestampRelation).toBeTruthy();

                if (rel.type === RelationType.GreaterThanOrEqualTo) {
                    expect(new Date(rel.value!).getTime()).toBe(relBucket.timeStamp!.getTime());
                    for (const memRef of relBucket.members!) {
                        const member = await dataColl.findOne({ "id": memRef });
                        expect(member).toBeDefined();
                        expect(member!.timestamp.getTime()).toBeGreaterThanOrEqual(new Date(rel.value!).getTime());
                    }
                } else if (rel.type === RelationType.LessThan) {
                    expect(new Date(rel.value!).getTime())
                        .toBe(new Date(relBucket.timeStamp!.getTime() + relBucket.span).getTime());
                    for (const memRef of relBucket.members!) {
                        const member = await dataColl.findOne({ "id": memRef });
                        expect(member).toBeDefined();
                        expect(member!.timestamp.getTime()).toBeLessThan(new Date(rel.value!).getTime());
                    }
                }
            }
        }
    });

    test("Writing a bucketless SDS into MongoDB using the default time-based fragmentation (k = 4, m = 10, n = 100) and high temporal density", async () => {
        const dataStream = new SimpleStream<string>();
        const metadataStream = new SimpleStream<string>();
        const config: DBConfig = {
            url: mongod.getUri(),
            metadata: "METADATA",
            data: "DATA",
            index: "INDEX"
        };

        // Max number of members allowed per fragment
        const m = 10;
        // Number of sub-fragments per level in the default B+Tree structure
        const k = 4;
        // Total number of members to be pushed
        const n = 100

        // Execute ingest function
        await ingest(dataStream, metadataStream, config, m, k);

        // Push metadata in
        await metadataStream.push(METADATA);

        // Push some SDS records in with 10ms delta between each other
        for (const record of dataGenerator(n, 10)) {
            await dataStream.push(record);
        }

        // Check that metadata was stored
        expect(await metaColl.countDocuments()).toBe(2);
        expect((await metaColl.findOne({ "id": "http://example.org/ns#rmlStream" }))!.type).toBe(SDS.Stream);
        expect((await metaColl.findOne({ "id": "http://example.org/ns#sdsStream" }))!.type).toBe(SDS.Stream);
        // Check all data records were persisted
        expect(await dataColl.countDocuments()).toBe(n);
        // Check that all fragments are correct and consistent
        const indexes = await indexColl.find().toArray();

        for (const bucket of indexes) {
            // Check fragment does not violate max members constraint
            expect(bucket.members!.length).toBeLessThanOrEqual(m);
            // Check local members do belong here
            for (const memId of bucket.members!) {
                const member = await dataColl.findOne({ "id": memId });
                expect(member).toBeDefined();
                expect(member!.timestamp.getTime()).toBeGreaterThanOrEqual(bucket.timeStamp!.getTime());
                expect(member!.timestamp.getTime()).toBeLessThan(bucket.timeStamp!.getTime() + bucket.span);
            }
            // Check it contains at most 2k relations (tree:LessThan & tree:GreaterThanOrEqualTo)
            expect(bucket.relations.length).toBeLessThanOrEqual(2 * k);
            // Check that all relations are telling the truth
            for (const rel of bucket.relations) {
                // Fetch related bucket
                const relBucket = (await indexColl.find({ id: rel.bucket }).toArray())[0];
                expect(relBucket).toBeDefined();

                if (rel.type === RelationType.GreaterThanOrEqualTo) {
                    expect(rel.timestampRelation).toBeTruthy();
                    expect(new Date(rel.value!).getTime()).toBe(relBucket.timeStamp!.getTime());
                    for (const memRef of relBucket.members!) {
                        const member = await dataColl.findOne({ "id": memRef });
                        expect(member).toBeDefined();
                        expect(member!.timestamp.getTime()).toBeGreaterThanOrEqual(new Date(rel.value!).getTime());
                    }
                } else if (rel.type === RelationType.LessThan) {
                    expect(rel.timestampRelation).toBeTruthy();
                    expect(new Date(rel.value!).getTime())
                        .toBe(new Date(relBucket.timeStamp!.getTime() + relBucket.span).getTime());
                    for (const memRef of relBucket.members!) {
                        const member = await dataColl.findOne({ "id": memRef });
                        expect(member).toBeDefined();
                        expect(member!.timestamp.getTime()).toBeLessThan(new Date(rel.value!).getTime());
                    }
                } else if (rel.type === RelationType.Relation) {
                    // Check that related bucket increased pagination by 1
                    expect(relBucket.page).toBe(bucket.page + 1);
                    // Timestamps should be equal
                    expect(relBucket.timeStamp!.getTime()).toBe(bucket.timeStamp!.getTime());
                }
            }
        }
    });

    test("Writing a bucketless SDS into MongoDB using the default time-based fragmentation (k = 4, m = 10, n = 500, b = 3600) and older timestamps", async () => {
        const dataStream = new SimpleStream<string>();
        const metadataStream = new SimpleStream<string>();
        const config: DBConfig = {
            url: mongod.getUri(),
            metadata: "METADATA",
            data: "DATA",
            index: "INDEX"
        };

        // Max number of members allowed per fragment
        const m = 10;
        // Number of sub-fragments per level in the default B+Tree structure
        const k = 4;
        // Total number of members to be pushed
        const n = 500;
        // Minimum allowed bucket span
        const b = 3600;

        // Execute ingest function
        await ingest(dataStream, metadataStream, config, m, k, b);

        // Push metadata in
        await metadataStream.push(METADATA);

        // Push some SDS records in with 30 days delta between each other and past timestamps (1 year ago)
        for (const record of dataGenerator(n, 2592000000, new Date(new Date().getTime() - 31536000000))) {
            await dataStream.push(record);
        }

        // Check that metadata was stored
        expect(await metaColl.countDocuments()).toBe(2);
        expect((await metaColl.findOne({ "id": "http://example.org/ns#rmlStream" }))!.type).toBe(SDS.Stream);
        expect((await metaColl.findOne({ "id": "http://example.org/ns#sdsStream" }))!.type).toBe(SDS.Stream);
        // Check all data records were persisted
        expect(await dataColl.countDocuments()).toBe(n);
        // Check that all fragments are correct and consistent
        const indexes = await indexColl.find().toArray();

        for (const bucket of indexes) {
            // Check fragment does not violate max members constraint
            expect(bucket.members!.length).toBeLessThanOrEqual(m);
            // Check local members do belong here
            for (const memId of bucket.members!) {
                const member = await dataColl.findOne({ "id": memId });
                expect(member).toBeDefined();
                expect(member!.timestamp.getTime()).toBeGreaterThanOrEqual(bucket.timeStamp!.getTime());
                expect(member!.timestamp.getTime()).toBeLessThan(bucket.timeStamp!.getTime() + bucket.span);
            }
            // Check it contains at most 2k relations (tree:LessThan & tree:GreaterThanOrEqualTo)
            expect(bucket.relations.length).toBeLessThanOrEqual(2 * k);
            // Check that all relations are telling the truth
            for (const rel of bucket.relations) {
                // Fetch related bucket
                const relBucket = (await indexColl.find({ id: rel.bucket }).toArray())[0];
                expect(relBucket).toBeDefined();

                if (rel.type === RelationType.GreaterThanOrEqualTo) {
                    expect(rel.timestampRelation).toBeTruthy();
                    expect(new Date(rel.value!).getTime()).toBe(relBucket.timeStamp!.getTime());
                    for (const memRef of relBucket.members!) {
                        const member = await dataColl.findOne({ "id": memRef });
                        expect(member).toBeDefined();
                        expect(member!.timestamp.getTime()).toBeGreaterThanOrEqual(new Date(rel.value!).getTime());
                    }
                } else if (rel.type === RelationType.LessThan) {
                    expect(rel.timestampRelation).toBeTruthy();
                    expect(new Date(rel.value!).getTime())
                        .toBe(new Date(relBucket.timeStamp!.getTime() + relBucket.span).getTime());
                    for (const memRef of relBucket.members!) {
                        const member = await dataColl.findOne({ "id": memRef });
                        expect(member).toBeDefined();
                        expect(member!.timestamp.getTime()).toBeLessThan(new Date(rel.value!).getTime());
                    }
                } else if (rel.type === RelationType.Relation) {
                    // Check that related bucket increased pagination by 1
                    expect(relBucket.page).toBe(bucket.page + 1);
                    // Timestamps should be equal
                    expect(relBucket.timeStamp!.getTime()).toBe(bucket.timeStamp!.getTime());
                }
            }
        }
    });
     */
});

function* dataGenerator(n: number, inc: number, startDate?: Date): Generator<string> {
    const date = startDate? startDate : new Date();

    for (let i = 0; i < n; i++) {
        const record = `
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
            @prefix sds: <https://w3id.org/sds#>.
            @prefix ex:  <http://example.org/ns#>.
            @prefix dct: <http://purl.org/dc/terms/>.

            [] sds:stream ex:sdsStream;
                sds:payload <https://example.org/entity/Entity${i}>.

            <https://example.org/entity/Entity${i}> a ex:Entity;
                dct:modified "${date.toISOString()}"^^xsd:dateTime;
                ex:prop1 "some value";
                ex:prop2 [
                    a ex:NestedEntity;
                    ex:nestedProp "some other value"
                ];
                ex:prop3 ex:SomeNamedNode.
        `;
        // Increase timestamp by 1h
        date.setTime(date.getTime() + inc);
        yield record;
    }
}
