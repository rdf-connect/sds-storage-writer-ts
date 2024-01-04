
import type * as RDF from '@rdfjs/types';
import { Member, RelationType } from '@treecg/types';
import { Collection } from "mongodb";
import type { Logger } from 'winston';
import { DataRecord } from '.';

export type TREEFragment = {
   id?: string,
   streamId: string,
   value?: string,
   relations: Array<{
      type: RelationType,
      value?: string,
      bucket: string,
      path?: string,
      timestampRelation?: boolean
   }>,
   members?: string[],
   count: number,
   timeStamp?: Date,
   span: number,
   immutable: boolean,
   root: boolean
};

export type ExtractIndices = (member: Member, mongo: Collection<TREEFragment>, timestampPath?: RDF.Term) => Promise<void>;

export interface Fragment {
   streamId: string,
   extract: ExtractIndices,
}

// TODO: Check if scheduling updates for each new fragment using setTimeout
// is the best way to handle immutable labeling. 
export async function handleTimestampPath(
   id: string | null,
   streamId: string,
   path: string,
   timestampValue: Date,
   memberId: string,
   indexColl: Collection<TREEFragment>,
   memberColl: Collection<DataRecord>,
   maxSize: number,
   k: number = 4,
   logger: Logger
) {
   logger.debug(`[ingest] handleTimeStamp: Processing record ${memberId} with timestamp ${timestampValue.toISOString()}`);

   // Candidate fragment where we want to put this member (most granular fragment)
   const candidateFragment: undefined | TREEFragment = (
      await indexColl.find({ streamId, timeStamp: { "$lte": timestampValue } })
         .sort({ timeStamp: -1, span: -1 }).limit(1).toArray()
   )[0];

   if (candidateFragment) {
      logger.debug(`[ingest] handleTimeStamp: Found this closest candidate fragment: ${candidateFragment.timeStamp?.toISOString()}`);

      // Check if this member belongs to a new top level fragment (i.e. a new year)
      if (timestampValue.getFullYear() > candidateFragment.timeStamp!.getFullYear()) {
         await indexColl.insertOne(createNewYearFragment(streamId, id, memberId, timestampValue.getFullYear()));
         logger.debug(`[ingest] handleTimeStamp: Created new top level fragment spanning 1 year: ${timestampValue.getFullYear()}`);
         return;
      }

      // Check if this fragment is still mutable
      if (!candidateFragment.immutable) {
         // We can still write this member to this fragment, unless it is full already
         if (candidateFragment.count < maxSize) {
            // There is still room in this fragment
            await indexColl.updateOne(
               candidateFragment,
               {
                  $inc: { count: 1 },
                  $push: { members: memberId }
               }
            );
            logger.debug(`[ingest] handleTimeStamp: Added new record to candidate fragment ${candidateFragment.timeStamp?.toISOString()}`);
            return;
         } else {
            // We need to further split this fragment in k sub-fragments (recursively),
            // while respecting the fragment max size
            await splitFragmentRecursively(
               k,
               maxSize,
               candidateFragment,
               memberId,
               streamId,
               path,
               memberColl,
               indexColl,
               logger
            );
         }
      } else {
         // TODO: implement optional strict mode that fails in this case
         logger.warn(`[ingest] handleTimeStamp: Received out of order member that cannot be added to the collection: ${memberId}`);
         return;
      }
   } else {
      // Check if there is no candidate fragment because this is the first fragment ever
      // or because this member is older than all existing fragments
      if (await indexColl.countDocuments() > 0) {
         // TODO: implement optional strict mode that fails in this case
         logger.warn(`[ingest] handleTimeStamp: Received out of order member that cannot be added to the collection: ${memberId}`);
      } else {
         // This is the first fragment ever. Let's create a fragment spanning 1 year
         const currYear = new Date().getFullYear();
         await indexColl.insertOne(createNewYearFragment(streamId, id, memberId, currYear));
         logger.debug(`[ingest] handleTimeStamp: Created initial fragment spanning 1 year: ${currYear}`);
      }
   }
}

function createNewYearFragment(
   streamId: string,
   id: string | null,
   memberId: string,
   year: number): TREEFragment {
   const timeStamp = new Date();
   timeStamp.setUTCFullYear(year);
   timeStamp.setUTCMonth(0);
   timeStamp.setUTCDate(1);
   timeStamp.setUTCHours(0, 0, 0, 0);

   return {
      streamId,
      id: id ? id : `${streamId}/${timeStamp.toISOString()}/31536000000`,
      timeStamp,
      relations: [],
      members: [memberId],
      count: 1,
      span: 31536000000,
      immutable: false,
      root: true
   }
}

async function splitFragmentRecursively(
   k: number,
   maxSize: number,
   candidateFragment: TREEFragment,
   newMember: string | null,
   streamId: string,
   path: string,
   memberColl: Collection<DataRecord>,
   indexColl: Collection<TREEFragment>,
   logger: Logger
) {
   // Gather all the members of the full fragment
   const membersRefs = candidateFragment.members!;
   if (newMember) {
      // Include the new member (if any)
      membersRefs.push(newMember);
   }
   const members = await memberColl.find({ id: { $in: membersRefs } }).toArray();
   // Sort members per timestamp
   members.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
   // Timespan for new sub-fragments
   const newSpan = candidateFragment.span / k;
   // New relations to be added
   const newRelations: TREEFragment["relations"] = []
   // New sub-fragments
   const subFragments: TREEFragment[] = [];

   for (let i = 0; i < k; i++) {
      const newTs = new Date(candidateFragment.timeStamp!.getTime() + (i * newSpan));
      const subFragment: TREEFragment = {
         id: `${streamId}/${newTs.toISOString()}/${newSpan}`,
         streamId,
         relations: [],
         members: [],
         count: 0,
         timeStamp: newTs,
         span: newSpan,
         immutable: false,
         root: false
      };

      for (const member of members) {
         // Check which members belong in this new sub-fragment
         if (member.timestamp.getTime() >= newTs.getTime()
            && member.timestamp.getTime() < (newTs.getTime() + newSpan)) {
            subFragment.members?.push(member.id);
            subFragment.count++;

         }
      }

      // These are the new relations that are added from the originally full
      // candidate fragment towards this new sub-fragment
      newRelations.push(...[
         {
            type: RelationType.GreaterThanOrEqualTo,
            value: newTs.toISOString(),
            bucket: subFragment.id!,
            path,
            timestampRelation: true
         },
         {
            type: RelationType.LessThan,
            value: new Date(newTs.getTime() + newSpan).toISOString(),
            bucket: subFragment.id!,
            path,
            timestampRelation: true
         }
      ]);

      // Check we if this new sub-fragment is violating the max size constraint
      if (subFragment.members!.length > maxSize) {
         // Further split this fragment that is currently too large
         logger.debug(`SPLITTING ONE LEVEL DEEPER for sub-fragment ${subFragment.timeStamp?.toISOString()}`);
         await splitFragmentRecursively(
            k,
            maxSize,
            subFragment,
            null,
            streamId,
            path,
            memberColl,
            indexColl,
            logger
         );
      } else {
         // Sub-fragment to be persisted in a bulk operation
         subFragments.push(subFragment);
      }
   }

   // Persist new sub-fragments and their relations
   await Promise.all([
      indexColl.insertMany(subFragments),
      indexColl.updateOne(
         { id: candidateFragment.id! },
         {
            $set: {
               streamId,
               members: [], 
               count: 0, 
               timeStamp: candidateFragment.timeStamp,
               span: candidateFragment.span,
               immutable: candidateFragment.immutable,
               root: candidateFragment.root
            },
            $push: { relations: { $each: newRelations } }
         },
         { upsert: true }
      )
   ]);
   logger.debug(`Added ${subFragments.length} new sub-fragments`);
}

