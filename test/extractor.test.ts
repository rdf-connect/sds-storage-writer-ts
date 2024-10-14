import { describe, expect, test } from "vitest";
import { Extractor } from "../lib/extractor";

describe("SDS storage writer tests", () => {
    test("ingest configuration", async () => {
        const value = `
@prefix uk: <http://mumo.be/data/unknown/>.
@prefix sds: <https://w3id.org/sds#>.
@prefix data: <http://mumo.be/data/>.
@prefix ob: <http://def.isotc211.org/iso19156/2011/Observation#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sosa: <http://www.w3.org/ns/sosa/>.

_:b5_b4_n3-32 a <http://qudt.org/1.1/schema/qudt#QuantityValue>;
    <http://qudt.org/1.1/schema/qudt#unit> <http://qudt.org/1.1/vocab/unit#DegreeCelsius>;
    <http://qudt.org/1.1/schema/qudt#numericValue> "19.02265739440918"^^xsd:float.
<http://mumo.be/data/2024-01-08T13:34:58.277205944Z/temperature> a ob:OM_Observation;
    <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation.resultTime> "2024-01-08T13:34:58.277205944Z"^^xsd:dateTime;
    <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation.result> _:b5_b4_n3-32;
    sosa:madeBySensor <http://mumo.be/data/unknown/sensor/temperature>.
uk:location a <http://purl.org/dc/terms/Location>;
    <http://purl.org/dc/terms/title> "UNKNOWN - Location".
uk:node sosa:hosts <http://mumo.be/data/unknown/sensor/temperature>, <http://mumo.be/data/unknown/sensor/pressure>, <http://mumo.be/data/unknown/sensor/battery>, <http://mumo.be/data/unknown/sensor/humidity>;
    a sosa:Platform;
    <http://purl.org/dc/terms/description> "Unconfigured node";
    <http://purl.org/dc/terms/title> "UNCONFIGURED";
    <http://www.cidoc-crm.org/cidoc-crm/P55_has_current_location> uk:location;
    <http://www.loc.gov/mods/rdf/v1#relatedItem> "SomeID".
<http://mumo.be/data/unknown/sensor/battery> a sosa:Sensor;
    <http://purl.org/dc/terms/title> "UNKNOWN - Battery";
    sosa:observes "battery".
<http://mumo.be/data/unknown/sensor/humidity> a sosa:Sensor;
    <http://purl.org/dc/terms/title> "UNKNOWN - Humidity";
    sosa:observes "humidity".
<http://mumo.be/data/unknown/sensor/pressure> a sosa:Sensor;
    <http://purl.org/dc/terms/title> "UNKNOWN - Pressure";
    sosa:observes "pressure".
<http://mumo.be/data/unknown/sensor/temperature> a sosa:Sensor;
    <http://purl.org/dc/terms/title> "UNKNOWN - Temperature";
    sosa:observes "temperature".
sds:DataDescription {
_:df_13_1 sds:payload <http://mumo.be/data/2024-01-08T13:34:58.277205944Z/temperature>;
    sds:stream <https://example.org/ns#>;
    sds:bucket <root/UNKNOWN%20-%20Location/UNCONFIGURED/temperature>.
_:df_13_2 a sds:Relation;
    sds:relationType <https://w3id.org/tree#EqualToRelation>;
    sds:relationBucket <root/UNKNOWN%20-%20Location>;
    sds:relationPath _:n3-12;
    sds:relationValue uk:location.
_:df_13_3 a sds:Relation;
    sds:relationType <https://w3id.org/tree#EqualToRelation>;
    sds:relationBucket <root/UNKNOWN%20-%20Location/UNCONFIGURED>;
    sds:relationPath _:n3-20;
    sds:relationValue uk:node.
_:df_13_4 a sds:Relation;
    sds:relationType <https://w3id.org/tree#EqualToRelation>;
    sds:relationBucket <root/UNKNOWN%20-%20Location/UNCONFIGURED/temperature>;
    sds:relationPath _:n3-25;
    sds:relationValue <http://qudt.org/1.1/vocab/unit#DegreeCelsius>.
_:n3-12 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> sosa:madeBySensor;
    <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n3-13.
_:n3-13 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:n3-14;
    <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n3-15.
_:n3-14 <http://www.w3.org/ns/shacl#inversePath> sosa:hosts.
_:n3-15 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://www.cidoc-crm.org/cidoc-crm/P55_has_current_location>;
    <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n3-16.
_:n3-16 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:n3-17;
    <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>.
_:n3-17 <http://www.w3.org/ns/shacl#zeroOrMorePath> <http://purl.org/dc/terms/isPartOf>.
_:n3-20 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> sosa:madeBySensor;
    <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n3-21.
_:n3-21 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:n3-22;
    <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>.
_:n3-22 <http://www.w3.org/ns/shacl#inversePath> sosa:hosts.
_:n3-25 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation.result>;
    <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:n3-26.
_:n3-26 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://qudt.org/1.1/schema/qudt#unit>;
    <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>.
<root> a sds:Bucket;
    sds:immutable "false";
    sds:isRoot "true";
    sds:stream <https://example.org/ns#>;
    sds:relation _:df_13_2.
<root/UNKNOWN%20-%20Location> a sds:Bucket;
    sds:immutable "false";
    sds:stream <https://example.org/ns#>;
    sds:relation _:df_13_3.
<root/UNKNOWN%20-%20Location/UNCONFIGURED> a sds:Bucket;
    sds:immutable "false";
    sds:stream <https://example.org/ns#>;
    sds:relation _:df_13_4.
<root/UNKNOWN%20-%20Location/UNCONFIGURED/temperature> a sds:Bucket;
    sds:immutable "false";
    sds:stream <https://example.org/ns#>
}
  `;

        const extractor = new Extractor();
        const extract = extractor.extract(value);

        expect(extract.getData().length).toBe(30);
        const records = extract.getRecords();
        expect(records.length).toBe(1);
        expect(records[0].payload).toBe(
            "http://mumo.be/data/2024-01-08T13:34:58.277205944Z/temperature",
        );

        const buckets = extract.getBuckets();
        expect(buckets.length).toBe(4);
        expect(buckets.map((x) => x.streamId)).toEqual([
            "https://example.org/ns#",
            "https://example.org/ns#",
            "https://example.org/ns#",
            "https://example.org/ns#",
        ]);

        const relations = extract.getRelations();
        expect(relations.length).toBe(3);
    });
});
