import type * as RDF from '@rdfjs/types';
import { Member, RelationType } from '@treecg/types';
import { Collection } from "mongodb";
import { Fragment, handleTimestampPath, MongoFragment } from '.';

export class SubjectFragmentor implements Fragment {
    public readonly id: string;
    private readonly bucketProperty: RDF.Term;

    constructor(id: RDF.Term, bucketProperty: RDF.Term) {
        this.id = id.value;
        this.bucketProperty = bucketProperty;
    }

    async extract(member: Member, mongo: Collection<MongoFragment>, timestampPath?: RDF.Term) {
        const value = member.quads.find(
            quad => quad.subject.equals(member.id)
                && quad.predicate.equals(this.bucketProperty)
        )?.object.value;

        if(!value) {
            console.error(`didn't find bucketProperty ${this.bucketProperty.value} on member ${member.id.value}`); 
            return;
        }

        const timestampValue = timestampPath ? member.quads.find(
            quad => quad.subject.equals(member.id) && quad.predicate.equals(timestampPath)
        )?.object.value : undefined;

        let currentFragment = await mongo.findOne({ fragmentId: this.id, ids: [value] });

        // Got to create the fragment
        if (!currentFragment) {
            const fragments = await mongo.find({ leaf: false, fragmentId: this.id }).toArray()
            const relations = fragments.map(id => { return { type: RelationType.EqualThan, values: id.ids, bucket: id.ids && id.ids[id.ids.length - 1] || "" } });

            const members = !!timestampValue ? undefined : [];

            await mongo.updateMany({ leaf: false, fragmentId: this.id }, { $push: { relations: { type: RelationType.EqualThan, values: [value], bucket: value } } });
            await mongo.insertOne({ leaf: false, fragmentId: this.id, ids: [value], count: 0, relations, members });
        }

        if (timestampValue) {
            await handleTimestampPath([value], this.id, timestampValue, member.id.value, mongo);
        } else {
            await mongo.updateOne({ leaf: false, fragmentId: this.id, ids: [value] }, { $inc: { count: 1 }, $push: { members: member.id.value } });
        }
    }
}
