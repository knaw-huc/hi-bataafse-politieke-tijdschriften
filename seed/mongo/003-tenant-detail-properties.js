const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

tenantDb.detail_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.detail_properties.insertMany([
    { dataset_name: DATASET_NAME, name: "tijdschrift", type: "json", path: "$", order: 1, config: {
      "id": {
        "hidden": true
      },
      "tijdschriftId": {
        "hidden": true
      },
      "uitgeverId": {
        "hidden": true
      },
      "uitgever": {
        "type": "link",
        "url": "/politieke-tijdschriften-uitgever_drukker/details/$uitgeverId"
      },
      "plaatsId": {
        "hidden": true
      },
      "plaats": {
        "type": "link",
        "url": "/politieke-tijdschriften-plaatsnaam/details/$plaatsId"
      },
      "aanvullendeTitels": {
        "type": "list"
      },
      "korteOmschrijvingInhoud": {
        "type": "markdown"
      },
      "toelichtingRedacteurAuteur": {
        "type": "markdown"
      }
    }}
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-plaatsnaam` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-plaatsnaam`, name: "plaats", type: "json", path: "$", order: 1, config: {
        "id": { "hidden": true },
        "plaatsId": { "hidden": true },
        "geoNameURI": { "type": "external-link" }
    }}
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-personen` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-personen`, name: "persoon", type: "json", path: "$", order: 1, config: {
            "id": {"hidden": true},
            "persoonId": {"hidden": true},
            "bioPortaal": {"type": "external-link"},
            "bioNummer": {"hidden": true}
        }}
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-uitgever_drukker` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-uitgever_drukker`, name: "uitgever_drukker", type: "json", path: "$", order: 1, config: {
      "id": {
        "hidden": true
      },
      "uitgeverId": {
        "hidden": true
      },
      "eersteGeneratieId": {
        "hidden": true
      },
      "tweedeGeneratieId": {
        "hidden": true
      },
      "eerstePlaatsId": {
        "hidden": true
      },
      "tweedePlaatsId": {
        "hidden": true
      },
      "derdePlaatsId": {
        "hidden": true
      },
      "eersteGeneratieNaam": {
        "type": "link",
        "url": "/politieke-tijdschriften-personen/details/$eersteGeneratieId"
      },
      "eerstePlaatsNaam": {
        "type": "link",
        "url": "/politieke-tijdschriften-plaatsnaam/details/$eerstePlaatsId"
      },
      "tweedePlaatsNaam": {
        "type": "link",
        "url": "/politieke-tijdschriften-plaatsnaam/details/$tweedePlaatsId"
      },
      "derdePlaatsNaam": {
        "type": "link",
        "url": "/politieke-tijdschriften-plaatsnaam/details/$derdePlaatsId"
      }
    }}
]);

