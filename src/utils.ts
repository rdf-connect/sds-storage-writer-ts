import { DataFactory, Parser, Quad_Object } from "n3";
import type * as RDF from "@rdfjs/types";
import { RdfThing } from "./extractor";
/* @ts-expect-error no type declaration available */
import canonize from "rdf-canonize";

type DenyQuad = (q: RDF.Quad, currentId: RDF.Term) => boolean;

const df = DataFactory;

export function maybe_parse(data: RDF.Quad[] | string): RDF.Quad[] {
    if (typeof data === "string" || data instanceof String) {
        const parse = new Parser();
        return parse.parse(<string>data);
    } else {
        return data;
    }
}

export function filterMember(
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

export async function pathString(
    thing?: RdfThing,
): Promise<string | undefined> {
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

    return await canonize.canonize(quads, { algorithm: "RDFC-1.0" });
}
