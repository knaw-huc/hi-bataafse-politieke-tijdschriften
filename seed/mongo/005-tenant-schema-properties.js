const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

tenantDb.schema_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.schema_properties.insertMany([
    {
        dataset_name: DATASET_NAME,
        property: 'titel',
        order: 0,
        schema: { "type": "string" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'korteOmschrijvingInhoud',
        order: 0,
        schema: { "type": "string", "format": "markdown-html" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'uitgever',
        order: 0,
        schema: { "type": "string", "format": "link", "x-link-to": "/politieke-tijdschriften-uitgever_drukker/details/$uitgeverId" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'plaats',
        order: 0,
        schema: { "type": "string", "format": "link", "x-link-to": "/politieke-tijdschriften-plaatsnaam/details/$plaatsId" }
    },
     {
        dataset_name: DATASET_NAME,
        property: 'aanvullendeTitels',
        order: 0,
        schema: { "type": "array" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'vormTijdschrift',
        order: 0,
        schema: { "type": "string" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'typeTijdschrift',
        order: 0,
        schema: { "type": "string" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'politiekePositie',
        order: 0,
        schema: { "type": "string" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'verantwoordingSelectie',
        order: 0,
        schema: { "type": "string" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'toelichtingRedacteurAuteur',
        order: 0,
        schema: { "type": "string" }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'plaatsId',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'uitgeverId',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'tijdschriftId',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: DATASET_NAME,
        property: 'id',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: `${DATASET_NAME}-plaatsnaam`,
        property: 'plaatsId',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: `${DATASET_NAME}-plaatsnaam`,
        property: 'id',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: `${DATASET_NAME}-uitgever_drukker`,
        property: 'id',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: `${DATASET_NAME}-uitgever_drukker`,
        property: 'uitgeverId',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: `${DATASET_NAME}-personen`,
        property: 'eersteGeneratieId',
        order: 0,
        schema: { "x-omit": true }
    },
    {
        dataset_name: `${DATASET_NAME}-personen`,
        property: 'id',
        order: 0,
        schema: { "x-omit": true }
    },

]);
