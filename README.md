# sds-storage-writer-ts

[![Build and tests with Node.js](https://github.com/rdf-connect/sds-storage-writer-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/sds-storage-writer-ts/actions/workflows/build-test.yml)  
[![npm](https://img.shields.io/npm/v/@rdfc/sds-storage-writer-ts.svg?style=popout)](https://npmjs.com/package/@rdfc/sds-storage-writer-ts)

TypeScript [RDF-Connect](https://rdf-connect.github.io/rdfc.github.io/) processor for ingesting [SDS streams](https://treecg.github.io/SmartDataStreams-Spec/) and generating persistent [Linked Data Event Stream (LDES)](https://w3id.org/ldes/specification) or in general [TREE fragmentations](https://w3id.org/tree/specification).  
The processor consumes an SDS stream and stores all data, metadata, and fragmentation structures into a supported storage backend.

Currently supported storage systems:
- **MongoDB**
- **Redis**

This repository exposes one processor:

---

## [`rdfc:IngestSDS`](https://github.com/rdf-connect/sds-storage-writer-ts/blob/main/processor.ttl)

Processor that ingests an SDS stream to generate a Linked Data Event Stream.  
Uses `sds:Bucket` to describe fragmentation (see `rdfc:Bucketize`).

This processor stores:
- SDS members (data)
- SDS metadata
- SDS buckets and relations
- SDS fragmentation structure

in a persistent database backend, making the stream available for **LDES servers** to serve and traverse.

---

## Import

```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#>.

<> owl:imports <./node_modules/@rdfc/sds-storage-writer-ts/processor.ttl>.
```

---

## Example Configuration

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl:  <http://www.w3.org/2002/07/owl#>.

### Import processor definitions
<> owl:imports <./node_modules/@rdfc/sds-storage-writer-ts/processor.ttl>.

### Define readers
<dataInput> a rdfc:Reader.
<metadataInput> a rdfc:Reader.

### Configure the processor
<ingestor> a rdfc:IngestSDS;
    rdfc:dataInput <dataInput>;
    rdfc:metadataInput <metadataInput>;
    rdfc:database [
        rdfc:url "mongodb://localhost:27017/ldes";
        rdfc:metadata "meta";
        rdfc:data "data";
        rdfc:index "index";
    ].
```

---

## Processor Semantics

Given an SDS stream with a predefined fragmentation:

```turtle
# Member exists
ex:sample1 a ex:Object;
  ex:x "2";
  ex:y "5".

sds:DataDescription {
  [] sds:stream <bucketizedStream>;
    sds:payload ex:sample1; # Stream membership
    sds:bucket <bucket2>.

  # Bucket relations
  <bucket1> sds:relation [
    sds:relationType tree:GreaterThanRelation;
    sds:relationBucket <bucket2>;
    sds:relationValue 1;
    sds:relationPath ex:x 
  ].
}

```

The processor will:
- Store the **member data** `ex:sample1`
- Store **SDS metadata** `sds:DataDescription` and `sds:RemoveDataDescription`
- Store **bucket definitions** `sds:bucket` and `sds:relationBucket` values  
- Store **bucket relations** `sds:relation` values
- Persist the full fragmentation structure in the configured storage system.

---

## Database Backends

### MongoDB
Data is stored in collections defined by:
- `rdfc:metadata`
- `rdfc:data`
- `rdfc:index`

### Redis
Redis is supported as an alternative backend with the same logical structure, enabling:
- fast ingestion
- scalable storage

---

## Configuration Reference

### `rdfc:IngestSDS`

| Property             | Type                | Required | Description                   |
|----------------------|---------------------|----------|-------------------------------|
| `rdfc:dataInput`     | rdfc:Reader         | yes      | SDS member stream             |
| `rdfc:metadataInput` | rdfc:Reader         | yes      | SDS metadata stream           |
| `rdfc:database`      | rdfc:DatabaseConfig | yes      | Storage backend configuration |

---

### `rdfc:DatabaseConfig`

| Property        | Type   | Required | Description              |
|-----------------|--------|----------|--------------------------|
| `rdfc:metadata` | string | yes      | Metadata collection name |
| `rdfc:data`     | string | yes      | Data collection name     |
| `rdfc:index`    | string | yes      | Index collection name    |
| `rdfc:url`      | string | no       | Database connection URL  |

---

## Role in RDF-Connect Pipelines

Typical architecture:

```
[SDS Stream] + [Members Stream]
↓
rdfc:IngestSDS
↓
MongoDB / Redis
↓
LDES Server
```

This processor acts as the **persistent storage layer** between streaming RDF-Connect pipelines and TREE/LDES serving infrastructure.

---

© Ghent University – IMEC  
MIT License

Authors:
- Arthur Vercruysse
- Julián Rojas
- Ieben Smessaert
