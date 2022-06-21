import { Member } from "@treecg/types";
import { Collection } from "mongodb";
import { Fragment, handleTimestampPath, MongoFragment } from ".";

import type * as RDF from '@rdfjs/types';

export class TimestampFragmentor implements Fragment {
    public readonly id: string;
    constructor(id: string) {
        this.id = id;
    }

    async extract(member: Member, mongo: Collection<MongoFragment>, timestampPath?: RDF.Term) {
        const timestampValue = timestampPath ? member.quads.find(
            quad => quad.subject.equals(member.id) && quad.predicate.equals(timestampPath)
        )?.object.value : undefined;

        if (timestampValue) {
            await handleTimestampPath([], this.id, timestampValue, member.id.value, mongo);
        }
    }
}