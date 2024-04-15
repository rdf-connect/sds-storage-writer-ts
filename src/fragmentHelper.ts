import type * as RDF from "@rdfjs/types";
import { Member, RelationType } from "@treecg/types";
import { Collection } from "mongodb";
import type { Logger } from "winston";
import {
  DataRecord,
  FragmentExtension,
  Relation,
  SDSRecord,
  TREEFragment,
} from "./types";
import { serializeRdfThing } from "./utils";
import { Literal, NamedNode } from "n3";
import { createRequire } from "module";

type Fragment = TREEFragment & FragmentExtension;

export type ExtractIndices = (
  member: Member,
  mongo: Collection<Fragment>,
  timestampPath?: RDF.Term,
) => Promise<void>;

// export interface Fragment {
//   streamId: string;
//   extract: ExtractIndices;
// }

// TODO: Check if scheduling updates for each new fragment using setTimeout
// is the best way to handle immutable labeling.
export async function handleTimestampPath(
  record: SDSRecord<true>,
  path: string,
  indexColl: Collection<Fragment>,
  memberColl: Collection<DataRecord>,
  relationColl: Collection<Relation>,
  maxSize: number,
  k: number = 4,
  minBucketSpan: number,
  logger: Logger,
) {
  logger.debug(
    "-----------------------------------------------------------------------------------",
  );
  logger.debug(
    `[ingest] handleTimeStamp: Processing record ${
      record.payload.value
    } with timestamp ${record.timestampValue.toISOString()}`,
  );

  // Candidate fragment where we want to put this member (most granular fragment)
  const candidateFragment: undefined | Fragment = (
    await indexColl
      .find({
        streamId: record.stream,
        timeStamp: { $lte: record.timestampValue },
      })
      .sort({ timeStamp: -1, span: -1, page: -1 })
      .limit(1)
      .toArray()
  )[0];

  if (candidateFragment) {
    logger.debug(
      `[ingest] handleTimeStamp: Found this closest candidate fragment: ${candidateFragment.timeStamp?.toISOString()} (page ${
        candidateFragment.page
      })`,
    );

    // Check if this member belongs to a new top level fragment (i.e. a new year)
    if (
      record.timestampValue.getFullYear() >
      candidateFragment.timeStamp!.getFullYear()
    ) {
      await indexColl.insertOne(
        createNewYearFragment(
          record.stream,
          null,
          record.payload.value,
          record.timestampValue.getFullYear(),
        ),
      );
      logger.debug(
        `[ingest] handleTimeStamp: Created new top level fragment spanning 1 year: ${record.timestampValue.getFullYear()}`,
      );
      return;
    }

    // Check if this fragment is still mutable
    if (candidateFragment.immutable) {
      // TODO: implement optional strict mode that fails in this case
      logger.warn(
        `[ingest] handleTimeStamp: Received out of order member (${record.payload.value}) or current bucket has already expired (${candidateFragment.id})`,
      );
      return;
    }

    // We can still write this member to this fragment, unless it is full already
    if (candidateFragment.count < maxSize) {
      // There is still room in this fragment
      await indexColl.updateOne(candidateFragment, {
        $inc: { count: 1 },
        $push: { members: record.payload.value },
      });
      logger.debug(
        `[ingest] handleTimeStamp: Added new record to candidate fragment ${candidateFragment.timeStamp?.toISOString()}`,
      );
      return;
    } else {
      // We need to further split this fragment in k sub-fragments (recursively),
      // while respecting the fragment max size
      await splitFragmentRecursively(
        k,
        maxSize,
        candidateFragment,
        record.payload.value,
        record.stream,
        path,
        memberColl,
        indexColl,
        relationColl,
        minBucketSpan,
        logger,
      );
    }
  } else {
    // Check if there is no candidate fragment because this is the first fragment ever
    // or because this member is older than all existing fragments
    if ((await indexColl.countDocuments()) > 0) {
      // TODO: implement optional strict mode that fails in this case
      logger.warn(
        `[ingest] handleTimeStamp: Received out of order member that cannot be added to the collection: ${record.payload.value}`,
      );
    } else {
      // This is the first fragment ever. Let's create a fragment spanning 1 year based on the member's timestamp
      const currYear = record.timestampValue.getFullYear();
      await indexColl.insertOne(
        createNewYearFragment(
          record.stream,
          null,
          record.payload.value,
          currYear,
        ),
      );
      logger.debug(
        `[ingest] handleTimeStamp: Created initial fragment spanning 1 year: ${currYear}`,
      );
    }
  }
}

function createNewYearFragment(
  streamId: string,
  id: string | null,
  memberId: string,
  year: number,
): Fragment {
  const timeStamp = new Date();
  timeStamp.setUTCFullYear(year);
  timeStamp.setUTCMonth(0);
  timeStamp.setUTCDate(1);
  timeStamp.setUTCHours(0, 0, 0, 0);

  return {
    streamId,
    id: id ? id : `${timeStamp.toISOString()}/31536000000/0`,
    timeStamp,
    members: [memberId],
    count: 1,
    span: 31536000000,
    immutable: false,
    root: true,
    page: 0,
  };
}

async function splitFragmentRecursively(
  k: number,
  maxSize: number,
  candidateFragment: Fragment,
  newMember: string | null,
  streamId: string,
  path: string,
  memberColl: Collection<DataRecord>,
  indexColl: Collection<Fragment>,
  relationColl: Collection<Relation>,
  minBucketSpan: number,
  logger: Logger,
) {
  // Time span for new sub-fragment(s)
  const newSpan = candidateFragment.span / k;
  // Gather all the members of the full fragment
  const membersRefs = candidateFragment.members!;
  if (newMember) {
    // Include the new member (if any)
    membersRefs.push(newMember);
  }
  const newRelations: Relation[] = [];
  const members = await memberColl.find({ id: { $in: membersRefs } }).toArray();
  // Sort members per timestamp
  members.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());

  if (newSpan < minBucketSpan * 1000) {
    // We don't want to split temporal fragments under a given time resolution.
    // Instead we opt for a 1-dimensional pagination when the amount of members
    // is too high for a very short time span.

    const newFragment = handlePagedFragment(
      candidateFragment,
      streamId,
      members,
      maxSize,
      newRelations,
      logger,
    );

    // Persist new paginated fragment and its relation
    await Promise.all([
      relationColl.insertMany(newRelations),
      indexColl.insertOne(newFragment),
      indexColl.updateOne(
        { id: candidateFragment.id! },
        {
          $set: {
            streamId,
            members: members.slice(0, maxSize).map((m) => m.id),
            count: maxSize,
            timeStamp: candidateFragment.timeStamp,
            span: candidateFragment.span,
            immutable: candidateFragment.immutable,
            root: candidateFragment.root,
            page: candidateFragment.page,
          },
        },
        { upsert: true },
      ),
    ]);
  } else {
    // New sub-fragments
    const subFragments: Fragment[] = await handleSubfragmentCreation(
      k,
      candidateFragment,
      newSpan,
      streamId,
      members,
      newRelations,
      path,
      maxSize,
      logger,
      memberColl,
      indexColl,
      relationColl,
      minBucketSpan,
    );

    // Persist new sub-fragments and their relations
    await Promise.all([
      relationColl.insertMany(newRelations),
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
            root: candidateFragment.root,
            page: candidateFragment.page,
          },
        },
        { upsert: true },
      ),
    ]);

    logger.debug(
      `Added ${subFragments.length} new sub-fragments (${subFragments.map(
        (sf) => ` ${sf.id}`,
      )})`,
    );
  }
}

async function handleSubfragmentCreation(
  k: number,
  candidateFragment: Fragment,
  newSpan: number,
  streamId: string,
  members: DataRecord[],
  newRelations: Relation[],
  path: string,
  maxSize: number,
  logger: Logger,
  memberColl: Collection<DataRecord>,
  indexColl: Collection<Fragment>,
  relationColl: Collection<Relation>,
  minBucketSpan: number,
) {
  const subFragments: Fragment[] = [];

  for (let i = 0; i < k; i++) {
    const newTs = new Date(
      candidateFragment.timeStamp!.getTime() + i * newSpan,
    );
    const subFragment: Fragment = {
      id: `${newTs.toISOString()}/${newSpan}/0`,
      streamId,
      members: [],
      count: 0,
      timeStamp: newTs,
      span: newSpan,
      immutable: false,
      root: false,
      page: 0,
    };

    for (const member of members) {
      // Check which members belong in this new sub-fragment
      if (
        member.timestamp.getTime() >= newTs.getTime() &&
        member.timestamp.getTime() < newTs.getTime() + newSpan
      ) {
        subFragment.members?.push(member.id);
        subFragment.count++;
      }
    }

    const createRelation = (type: RelationType, time: Date) => {
      return {
        from: candidateFragment.id,
        bucket: subFragment.id!,
        type,
        value: serializeRdfThing({
          id: new Literal(time.toISOString()),
          quads: [],
        }),
        path: serializeRdfThing({ id: new NamedNode(path), quads: [] }),
      };
    };

    // These are the new relations that are added from the originally full
    // candidate fragment towards this new sub-fragment
    newRelations.push(
      createRelation(RelationType.GreaterThanOrEqualTo, newTs),
      createRelation(
        RelationType.LessThan,
        new Date(newTs.getTime() + newSpan),
      ),
    );

    // Check we if this new sub-fragment is violating the max size constraint
    if (subFragment.members!.length > maxSize) {
      // Further split this fragment that is currently too large
      logger.debug(
        `Splitting one level deeper for sub-fragment ${subFragment.timeStamp?.toISOString()} (span: ${
          subFragment.span
        })`,
      );
      await splitFragmentRecursively(
        k,
        maxSize,
        subFragment,
        null,
        streamId,
        path,
        memberColl,
        indexColl,
        relationColl,
        minBucketSpan,
        logger,
      );
    } else {
      // Sub-fragment to be persisted in a bulk operation
      subFragments.push(subFragment);
    }
  }

  return subFragments;
}

function handlePagedFragment(
  candidateFragment: Fragment,
  streamId: string,
  members: DataRecord[],
  maxSize: number,
  newRelations: Relation[],
  logger: Logger,
): Fragment {
  const baseBucketId = `${candidateFragment.timeStamp!.toISOString()}/${
    candidateFragment.span
  }`;

  const newFragment: Fragment = {
    id: `${baseBucketId}/${candidateFragment.page + 1}`,
    streamId,
    members: members.slice(maxSize).map((m) => m.id),
    count: members.slice(maxSize).map((m) => m.id).length,
    timeStamp: candidateFragment.timeStamp!,
    span: candidateFragment.span,
    immutable: false,
    root: false,
    page: candidateFragment.page + 1,
  };

  newRelations.push({
    from: baseBucketId,
    type: RelationType.Relation,
    bucket: `${baseBucketId}/${candidateFragment.page + 1}`,
  });

  logger.debug(
    `Added paginated (page ${
      newFragment.page
    }) new sub-fragment ${newFragment.timeStamp?.toISOString()}`,
  );

  return newFragment;
}
