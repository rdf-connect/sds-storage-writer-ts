# A SDS storage writer for MongoDB

[![Bun CI](https://github.com/TREEcg/sds-storage-writer-mongo/actions/workflows/build-test.yml/badge.svg)](https://github.com/TREEcg/sds-storage-writer-mongo/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@treecg/sds-storage-writer-mongo.svg?style=popout)](https://npmjs.com/package/@treecg/sds-storage-writer-mongo)

Given an [SDS stream](https://w3id.org/sds/specification) and its correspondent stream of members, this processor will write everything into a MongoDB instance.

SDS stream updates are stored into MongoDB collections for the LDES server to find this information when serving requests. If a `ldes:timestampPath` property is given as part of the dataset metadata, the storage writer will automatically start up a timestamp fragmentation, based on a B+ Tree strategy.

An example of a SDS data stream with a predefined fragmentation strategy is shown next:

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

### As a [Connector Architecture](https://the-connector-architecture.github.io/site/docs/1_Home) processor

This repository exposes the Connector Architecture processor [`js:Ingest`](https://github.com/TREEcg/sds-storage-writer-mongo/blob/master/configs/processor.ttl#L41), which can be used within data processing pipelines to write a SDS streams into a MongoDB instance. The processor can be configured as follows:

```turtle
@prefix : <https://w3id.org/conn#>.
@prefix js: <https://w3id.org/conn/js#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.

[ ] a js:Ingest;
  js:dataInput <inputDataReader>;
  js:metadataInput <inputMetadataReader>;
  js:database [
    js:url <http://myLDESView.org>;
    js:metadata "METADATA";
    js:data "DATA";
    js:index "INDEX";
  ];
  js:pageSize 500;
  js:branchSize 4.
```

### As a library

The library exposes one function `ingest`, which handles everything.

```typescript
async function ingest(
  data: Stream<string | Quad[]>, 
  metadata: Stream<string | RDF.Quad[]>, 
  database: DBConfig,
  maxsize: number = 100,
  k: number = 4
) { /* snip */ }
```

arguments:

- `data`: a stream reader that carries data (as `string` or `Quad[]`).
- `metadata`: a stream reader that carries SDS metadata (as `string` or `Quad[]`).
- `database`: connection parameters for a reachable MongoDB instance.
- `maxsize`: max number of members per fragment.
- `k`: max number of child nodes in the default time-based B+ Tree fragmentation.

## Authors and License

Arthur Vercruysse <arthur.vercruysse@ugent.be>
Julián Rojas <julianandres.rojasmelendez@ugent.be>

© Ghent University - IMEC. MIT licensed
