const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

tenantDb.schema_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.schema_properties.insertMany([
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "titel",
      "schema": {
        "type": "string"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "korteOmschrijvingInhoud",
      "schema": {
        "type": "string",
        "format": "markdown-html"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "uitgever",
      "schema": {
        "type": "string",
        "format": "link",
        "x-link-to": "/politieke-tijdschriften-uitgever_drukker/details/$uitgeverId"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "plaats",
      "schema": {
        "type": "string",
        "format": "link",
        "x-link-to": "/politieke-tijdschriften-plaatsnaam/details/$plaatsId"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "aanvullendeTitels",
      "schema": {
        "type": "array"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "vormTijdschrift",
      "schema": {
        "type": "string"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "typeTijdschrift",
      "schema": {
        "type": "string"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "politiekePositie",
      "schema": {
        "type": "string"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "verantwoordingSelectie",
      "schema": {
        "type": "string"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "toelichtingRedacteurAuteur",
      "schema": {
        "type": "string"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "plaatsId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "uitgeverId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "tijdschriftId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften",
      "order": 0,
      "property": "id",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-plaatsnaam",
      "order": 0,
      "property": "plaatsId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-plaatsnaam",
      "order": 0,
      "property": "id",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "id",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "uitgeverId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-personen",
      "order": 0,
      "property": "eersteGeneratieId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-personen",
      "order": 0,
      "property": "id",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-plaatsnaam",
      "order": 0,
      "property": "geoNameURI",
      "schema": {
        "type": "string",
        "format": "external-link"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "eerstePlaatsNaam",
      "schema": {
        "type": "string",
        "format": "link",
        "x-link-to": "/politieke-tijdschriften-plaatsnaam/details/$eerstePlaatsId"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "eerstePlaatsId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "tweedePlaatsId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "derdePlaatsId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-personen",
      "order": 0,
      "property": "persoonId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-personen",
      "order": 0,
      "property": "bioPortaal",
      "schema": {
        "type": "string",
        "format": "external-link"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "eersteGeneratieNaam",
      "schema": {
        "type": "string",
        "format": "link",
        "x-link-to": "/politieke-tijdschriften-personen/details/$eersteGeneratieId"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "eersteGeneratieId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "tweedeGeneratieId",
      "schema": {
        "x-omit": true
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "tweedeGeneratieNaam",
      "schema": {
        "type": "string",
        "format": "link",
        "x-link-to": "/politieke-tijdschriften-personen/details/$tweedeGeneratieId"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "tweedePlaatsNaam",
      "schema": {
        "type": "string",
        "format": "link",
        "x-link-to": "/politieke-tijdschriften-plaatsnaam/details/$tweedePlaatsId"
      }
    },
    {
      "dataset_name": "politieke-tijdschriften-uitgever_drukker",
      "order": 0,
      "property": "derdePlaatsNaam",
      "schema": {
        "type": "string",
        "format": "link",
        "x-link-to": "/politieke-tijdschriften-plaatsnaam/details/$derdePlaatsId"
      }
    }
]);

