# A storage writer for mongo

Given an [SDS stream](https://w3id.org/sds/specification) and a stream of members, will write everything to a MongoDB instance.
This stream is divided into 2 streams, a metadata stream and a data stream.

Items on the metadata stream are stored in the metadata collection so the LDES server can find this metadata back. If a timestamp path as part of the dataset is found, the storage writer will dynamically start up a timestamp fragmentation. 

Items on the data stream have the following shape:
```turtle
# Member ex:sample1 exists
ex:sample1 a ex:Object;
  ex:x "2";
  ex:y "5".


# <bucketizedStream> contains this member and this member is part of bucket <bucket2>
[] sds:stream <bucketizedStream>;
   sds:payload ex:sample1;
   sds:bucket <bucket2>.

# <bucket1> has a relation to <bucket2>
<bucket1> sds:relation [
  sds:relationType tree:GreaterThanRelation ;
  sds:relationBucket <bucket2> ;
  sds:relationValue 1;
  sds:relationPath ex:x 
] .
```

With this information, the data of the member is stored in the MongoDB collection, and the required relations are also stored in the database.


## Usage

The library exposes one function `ingest`, which handles everything.

```typescript
async function ingest(streamReader: {data: Stream<Quad[]>, metadata: Stream<Quad[]>}, metacollection: string, dataCollection: string, indexCollection: string, timestampFragmentation?: string, mongoUrl?: string) { /* snip */ }
```

arguments:
- `streamReader`: a stream reader that carries data (as Quad[]) and metadata (also Quad[]).
- `metaCollection`: the name of the used metadata collection of the mongo database. Used for storing all metadata.
- `dataCollection`: the name of the used data collection of the mongo database. Used for storing the actual member data.
- `indexCollection`: the name of the used index collection of the mongo database. Used for storing all information about the available fragmentations.
- `mongoUrl`: optional parameter to specify the mongo connection url. When undefined, the evn parameter `DB_CONN_STRING` is used. Default value is `mongodb://localhost:27017/ldes`;

### But what is a Stream?

If you are not familiar with the connector architecture, this example can get you started. (setting up a pipeline with an orchestrator like [nautirust](https://github.com/ajuvercr/nautirust) and using the [js-runner](https://github.com/ajuvercr/js-runner))

```typescript
import { SimpleStream, Stream } from "@treecg/connector-types";
import type * as RDF from '@rdfjs/types';
import { ingest } from ".";

type SR<T> = {
    [P in keyof T]: Stream<T[P]>;
}

type Data = {
    data: RDF.Quad[],
    metadata: RDF.Quad[],
}

async function main() {
    const dataStream = new SimpleStream<RDF.Quad[]>();
    const metadataStream = new SimpleStream<RDF.Quad[]>();
    const streamReader: SR<Data> = {data: dataStream, metadata: metadataStream};

    await ingest(streamReader, "meta", "data", "index", "https://w3id.org/ldes#TimestampFragmentation", "mongodb://localhost:27017/ldes");

    // Push some quads
    dataStream.push([

    ]);
}
```


## Authors and License

Arthur Vercruysse <arthur.vercruysse@ugent.be>

© Ghent University - IMEC. MIT licensed
