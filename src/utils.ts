
import type * as RDF from '@rdfjs/types';
import { LDES, Member, RDF as RDFT } from '@treecg/types';
import { Store } from "n3";

export function getMember(subject: RDF.Term, store: Store, done: Set<RDF.Term>): RDF.Quad[] {
    const newQuads = store.getQuads(subject, null, null, null);
    done.add(subject);

    const newSubjects = newQuads.map(q => q.object)
        .filter(q => q.termType === "BlankNode" || q.termType == "NamedNode")
        .filter(q => !done.has(q))

    return [...newQuads, ...newSubjects.flatMap(s => getMember(s, store, done))];
}

export function getMembersByType(type: RDF.Term, store: Store): Member[] {
    return store.getSubjects(RDFT.terms.type, type, null).map(sub => {
        if (sub.termType !== "NamedNode") throw "Memmbers can only be named nodes!";

        const quads = getMember(sub, store, new Set());
        return { id: sub, quads };
    });
}


export function extractBucketStrategies(meta: RDF.Quad[]): Member[] {
    const store = new Store(meta);
    return getMembersByType(LDES.terms.BucketizeStrategy, store);
}

