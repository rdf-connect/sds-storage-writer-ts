import { describe, expect, test } from "vitest";
import { ProcHelper } from "@rdfc/js-runner/lib/testUtils";
import { IngestSDS, IngestSDSArgs } from "../src";
import { resolve } from "path";
import { ReaderInstance } from "@rdfc/js-runner";

const pipeline = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix ex: <http://example.org/>.

ex:ingestor a rdfc:IngestSDS;
    rdfc:dataInput <data>;
    rdfc:metadataInput <metadata>;
    rdfc:database [
        rdfc:url "http://me.db";
        rdfc:metadata "meta";
        rdfc:data "data";
        rdfc:index "index";
    ].
`;

describe("processor", () => {
    test("IngestSDS definition", async () => {
        const helper = new ProcHelper<IngestSDS>();
        await helper.importFile(resolve("./processor.ttl"));
        await helper.importInline(resolve("./pipeline.ttl"), pipeline);

        const config = helper.getConfig("IngestSDS");

        expect(config.location).toBeDefined();
        expect(config.file).toBeDefined();
        expect(config.clazz).toBeDefined();

        const proc = <IngestSDS & IngestSDSArgs>(
            await helper.getProcessor("http://example.org/ingestor")
        );

        expect(proc.data).toBeInstanceOf(ReaderInstance);
        expect(proc.metadata.constructor.name).toBe("ReaderInstance");
        expect(proc.database.url).toBe("http://me.db");
        expect(proc.database.metadata).toBe("meta");
        expect(proc.database.data).toBe("data");
        expect(proc.database.index).toBe("index");
    });
});
