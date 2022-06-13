import { Stream } from "@treecg/connector-types";
import { MongoClient } from "mongodb";
import { DataFactory, DefaultGraph, Parser, Quad, Store, Term, Writer } from "n3";
import { env } from "process";

const { namedNode } = DataFactory;

type Member = {
    id: string,
    quads: Quad[],
}

type SR<T> = {
    [P in keyof T]: Stream<T[P]>;
}

type Data = {
    data: Quad[],
    metadata: Quad[],
}


type ExtractIndices = (member: Member) => any;

interface Fragment {
    id: string,
    f: ExtractIndices,
}

const dGraph = new DefaultGraph();
const ty = namedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");


function getMember(subject: Term, store: Store, done: Set<Term>): Quad[] {
    const newQuads = store.getQuads(subject, null, null, dGraph);
    done.add(subject);

    const newSubjects = newQuads.map(q => q.object)
        .filter(q => q.termType === "BlankNode" || q.termType == "NamedNode")
        .filter(q => !done.has(q))

    return [...newQuads, ...newSubjects.flatMap(s => getMember(s, store, done))];
}

export function getMembersByType(type: Term, store: Store): Member[] {
    return store.getSubjects(ty, type, null).map(sub => {
        if (sub.termType !== "NamedNode") throw "Memmbers can only be named nodes!";

        const quads = getMember(sub, store, new Set());
        return { id: sub.value, quads };
    });
}


function extractMetadata(meta: Quad[]): Member[] {
    const store = new Store(meta);
    return getMembersByType(namedNode("https://w3id.org/ldes#BucketizeStrategy"), store);
}


export function interpretFragmentation(fragmentation: Member): Fragment {
    const bucketProperty = fragmentation.quads.find(quad => quad.subject.value === fragmentation.id && quad.predicate.equals(namedNode("https://w3id.org/ldes#bucketProperty")))?.object || namedNode("https://w3id.org/ldes#bucket");
    const f = (a: Member) => a.quads.find(quad => quad.subject.value === a.id && quad.predicate.equals(bucketProperty))!.object.value;

    return {
        id: fragmentation.id, f
    };
}


export async function ingest(sr: SR<Data>, metacollection: string, dataCollection: string, indexCollectionName: string, mUrl?: string) {
    let state: Fragment[] = [];

    const url = mUrl || env.DB_CONN_STRING || "mongodb://localhost:27017/ldes";

    // Connect to database
    const mongo = await new MongoClient(url).connect();
    const db = mongo.db();
    const metaCollection = db.collection(metacollection);

    const dbFragmentations: Member[] = await metaCollection.find({ "type": "fragmentation" })
        .map(entry => { return { id: entry.id, quads: new Parser().parse(entry.value) } })
        .toArray();

    state = dbFragmentations.map(interpretFragmentation);


    const updateFragmentation = async (id: string, quads: Quad[]) => {
        const ser = new Writer().quadsToString(quads);
        await metaCollection.updateOne({ "type": "fragmentation", "id": id }, { $set: { value: ser } }, {upsert: true});
    };

    const handleMetadata = async (meta: Quad[]) => {
        const members = extractMetadata(meta);
        state = members.map(interpretFragmentation);

        await Promise.all(members.map(member => updateFragmentation(member.id, member.quads)));
    };

    sr.metadata.data(handleMetadata);
    if(sr.metadata.lastElement) {
        handleMetadata(sr.metadata.lastElement);
    }




    const memberCollection = db.collection(dataCollection);
    const indexCollection = db.collection(indexCollectionName);

    sr.data.data(async (data) => {
        const id = data[0].subject.value;
        const present = await memberCollection.count({ id: id }) > 0;

        if (!present) {
            const ser = new Writer().quadsToString(data);
            await memberCollection.insertOne({ id: id, data: ser });
        }

        const currentFragmentations = await indexCollection.find({ memberId: id }).map(entry => <string>entry.fragmentId).toArray();
        const fragmentIsNotPresent = (fragment: Fragment) => currentFragmentations.every(seen => seen != fragment.id);

        const newFragments = state.filter(fragmentIsNotPresent).map(fragment => {
            const indices = fragment.f({ id, quads: data });

            return {
                fragmentId: fragment.id,
                memberId: id,
                indices
            };
        });

        await indexCollection.insertMany(newFragments);
    });
}
