# A RDF-Connect SDS storage writer

[![Bun CI](https://github.com/rdf-connect/sds-storage-writer-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/sds-storage-writer-ts/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/sds-storage-writer-ts.svg?style=popout)](https://npmjs.com/package/@rdfc/sds-storage-writer-ts)

Given an [SDS stream](https://w3id.org/sds/specification) and its correspondent stream of members, this processor will write everything into a supported data storage system. So far, it only supports MongoDB instances.

SDS stream updates are stored into MongoDB collections for the LDES server to find this information when serving requests.

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

### As a [RDF-Connect](https://rdf-connect.github.io/rdfc.github.io/) processor

This repository exposes the following RDF-Connect processors:

#### [`js:Ingest`](https://github.com/rdf-connect/sds-storage-writer-mongo/blob/master/configs/processor.ttl#L41)

This processor can be used within data processing pipelines to write a SDS streams into a MongoDB instance. The processor can be configured as follows:

```turtle
@prefix : <https://w3id.org/conn#>.
@prefix js: <https://w3id.org/conn/js#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.


<> a rdfc:Pipeline;
   rdfc:consistsOf [
       rdfc:instantiates rdfc:NodeRunner;
       rdfc:processor ... , <ingest>, ... ; 
   ].

<ingest> a rdfc:Ingest;
  rdfc:dataInput <inputDataReader>;
  rdfc:metadataInput <inputMetadataReader>;
  rdfc:database [
    rdfc:url <http://myLDESView.org>;
    rdfc:metadata "METADATA";
    rdfc:data "DATA";
    rdfc:index "INDEX";
  ].
```

### As a library

The library exposes one function `ingest`, which handles everything.

```typescript
async function ingest(
  data: Stream<string | Quad[]>, 
  metadata: Stream<string | RDF.Quad[]>, 
  database: DBConfig,
) { /* snip */ }
```

arguments:

- `data`: a stream reader that carries data (as `string` or `Quad[]`).
- `metadata`: a stream reader that carries SDS metadata (as `string` or `Quad[]`).
- `database`: connection parameters for a reachable MongoDB instance.

## Authors and License

Arthur Vercruysse <arthur.vercruysse@ugent.be>
Julián Rojas <julianandres.rojasmelendez@ugent.be>

© Ghent University - IMEC. MIT licensed
