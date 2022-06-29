# A storage writer for mongo

Given an [SDS description stream](https://w3id.org/sds/specification) and a stream of members, will write everything to a MongoDB instance.

A lot of what the store writer does, is configured through metadata.
The mongoDB keeps the last instance of the metadata. This includes fragmentation strategies.

Note: a timestamp fragmentation is added when a timestampPath is found in the metadata about the dataset.

## Metadata

Note: This is the previous sds specification, when software catches up, this README will be updated.

Incoming metadata looks something like this:
```turtle
@prefix p-plan: <http://purl.org/net/p-plan#> .
@prefix sds: <https://w3id.org/sds#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix void: <http://rdfs.org/ns/void#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dcat: <https://www.w3.org/ns/dcat#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix ldes: <https://w3id.org/ldes#> .

# There is a sds:Stream which represents a sds:dataset. 
<http://me#csvStream>
  a sds:Stream;
  p-plan:wasGeneratedBy <http://me#readCsv> ;
  sds:carries <http://me#csvShape> ;
  sds:dataset <http://me#dataset> .

<http://me#dataset>
  a <https://www.w3.org/ns/dcat#Dataset> ;
  dcat:title "Epic dataset" ;
  dcat:publisher [ foaf:name "Arthur Vercruysse" ] ;
  ldes:timestampPath <http://example.org/ns#time> ; # important: timestampPath is defined
  dcat:identifier <http://localhost:3000/ldes> .

# A readCsv activity happened
# Only used for lineage
<http://me#readCsv>
  a p-plan:Activity ;
  rdfs:comment "Reads csv file and converts to rdf members" ;
  prov:used [
    a void:Dataset ;
    void:dataDump <file:///data/input.csv>
  ] .

# The stream carries sds:Member
<http://me#csvShape>
  a sds:Member ;
  sds:shape <http://example.org/ns#PointShape.ttl> .
```

This metadata doesn't give the stream writer that much information, only that there is a sds:Stream with a sds:dataset which has a ldes:timestampPath.
This information is however enough to write a basic timestamp fragmentation to the mongo.

When a Activity used a BucketizeStrategy like this:

```turtle
@prefix ex: <http://example.org/ns#> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix tree: <https://w3id.org/tree#> .

ex:BucketizeStrategy a ldes:BucketizeStrategy;
    ldes:bucketType ldes:subject;
    ldes:bucketProperty ldes:bucket2;
    tree:path ex:x;
    ldes:pageSize 50.
```

the store writer will add a new fragmentation accordingly in the mongo (and save this bucketization to the mongo as well).


## Usage

The library exposes one function `ingest`, this handles everything.

```typescript
async function ingest(streamReader: {data: Stream<Quad[]>, metadata: Stream<Quad[]>}, metacollection: string, dataCollection: string, indexCollection: string, timestampFragmentation?: string, mongoUrl?: string) { /* snip */ }
```

arguments:
- `streamReader`: a stream reader that carries data (as Quad[]) and metadata (also Quad[]).
- `metaCollection`: the name of the used metadata collection of the mongo database. Used for storing all metadata.
- `dataCollection`: the name of the used data collection of the mongo database. Used for storing the actual member data.
- `indexCollection`: the name of the used index collection of the mongo database. Used for storing all information about the available fragmentations.
- `timestampFragmentation`: optional parameter to give the timestampFragmentation an ID (this ID is used in the ldes-server). 
- `mongoUrl`: optional parameter to specify the mongo connection url. When undefined, the evn parameter `DB_CONN_STRING` is used. Default value is `mongodb://localhost:27017/ldes`;

### But what is a Stream?

If you are not familiar with the connector architecture, this example can get you started. (setting up a pipeline with an orchestrator like [nautirust](https://github.com/ajuvercr/nautirust) and using the a [js-runner](https://github.com/ajuvercr/js-runner))

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

© Ghnet University - IMEC. MIT licensed
