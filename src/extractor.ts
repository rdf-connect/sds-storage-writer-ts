import type * as RDF from "@rdfjs/types";
import { RDF as RDFT, RelationType, SDS } from "@treecg/types";
import { Parser } from "n3";
import { extractShapes, match, subject } from "rdf-lens";

import { $INLINE_FILE } from "@ajuvercr/ts-transformer-inline-file";

const Shapes = extractShapes(new Parser().parse($INLINE_FILE("./shape.ttl")));

export type Record = {
    stream: string;
    payload: string;
    buckets: string[];
};

export type Bucket = {
    id: string;
    stream: string;
    immutable?: boolean;
    root?: boolean;
    empty?: boolean;
};

export type RdfThing = {
    id: RDF.Term;
    quads: RDF.Quad[];
};

export type Relation = {
    type: RelationType;
    stream: string;
    origin: string;
    bucket: string;
    value?: RdfThing;
    path?: RdfThing;
};

const RecordLens = match(undefined, SDS.terms.payload, undefined)
    .thenAll(subject)
    .thenSome(Shapes.lenses["Record"]);

const BucketLens = match(undefined, RDFT.terms.type, SDS.terms.custom("Bucket"))
    .thenAll(subject)
    .thenSome(Shapes.lenses["Bucket"]);

const RelationLens = match(
    undefined,
    RDFT.terms.type,
    SDS.terms.custom("Relation"),
)
    .thenAll(subject)
    .thenSome(Shapes.lenses["Relation"]);

export class Extract {
    private data: RDF.Quad[] = [];
    private description: RDF.Quad[] = [];

    constructor(full: RDF.Quad[]) {
        full.forEach((q) => {
            if (q.graph.equals(SDS.terms.custom("DataDescription"))) {
                this.description.push(q);
            } else {
                this.data.push(q);
            }
        });
    }

    getData(): RDF.Quad[] {
        return this.data;
    }

    getRecords(): Record[] {
        return <Record[]>RecordLens.execute(this.description);
    }

    getBuckets(): Bucket[] {
        return <Bucket[]>BucketLens.execute(this.description);
    }

    getRelations(): Relation[] {
        return <Relation[]>RelationLens.execute(this.description);
    }
}

export class Extractor {
    constructor() {}

    extract_quads(quads: RDF.Quad[]): Extract {
        return new Extract(quads);
    }

    extract(inp: string): Extract {
        return new Extract(new Parser().parse(inp));
    }
}
