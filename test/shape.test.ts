import { afterEach, describe, expect, test } from "vitest";
import { setWarningHandler } from "rdf-lens";

import { Extractor } from "../lib/extractor";

const description = `
@prefix sds: <https://w3id.org/sds#>.
@prefix tree: <https://w3id.org/tree#>.

sds:DataDescription {
    _:record sds:payload <http://example.org/member>;
        sds:stream <http://example.org/stream>;
        sds:bucket <root/a>.

    <root/a> a sds:Bucket;
        sds:stream <http://example.org/stream>.

    <root> sds:relation _:relation.
    _:relation a sds:Relation;
        sds:relationType tree:GreaterThanRelation;
        sds:relationBucket <root/a>.
    <root> sds:stream <http://example.org/stream>.
}
`;

describe("SDS description shape", () => {
    let restore: ReturnType<typeof setWarningHandler> | undefined;
    afterEach(() => {
        if (restore) setWarningHandler(restore);
        restore = undefined;
    });

    test("reads the IRIs of records, buckets and relations as strings, without warnings", () => {
        const warnings: string[] = [];
        restore = setWarningHandler((message) => warnings.push(message));

        const extract = new Extractor().extract(description);

        expect(extract.getRecords()).toEqual([
            {
                stream: "http://example.org/stream",
                payload: "http://example.org/member",
                buckets: ["root/a"],
            },
        ]);
        expect(extract.getBuckets()).toEqual([
            { id: "root/a", streamId: "http://example.org/stream" },
        ]);
        expect(extract.getRelations()).toEqual([
            {
                type: "https://w3id.org/tree#GreaterThanRelation",
                stream: "http://example.org/stream",
                origin: "root",
                bucket: "root/a",
            },
        ]);
        expect(warnings).toEqual([]);
    });

    test("skips a record with a blank node where an IRI is expected", () => {
        const extract = new Extractor().extract(`
@prefix sds: <https://w3id.org/sds#>.
sds:DataDescription {
    _:record sds:payload _:member;
        sds:stream <http://example.org/stream>.
}
`);

        // Before, the payload became its blank node label, "b1_member"
        expect(extract.getRecords()).toEqual([]);
    });

    test("still reads a literal where an IRI is expected", () => {
        const extract = new Extractor().extract(`
@prefix sds: <https://w3id.org/sds#>.
sds:DataDescription {
    _:record sds:payload <http://example.org/member>;
        sds:stream "http://example.org/stream".
}
`);

        expect(extract.getRecords()).toEqual([
            {
                stream: "http://example.org/stream",
                payload: "http://example.org/member",
                buckets: [],
            },
        ]);
    });
});
