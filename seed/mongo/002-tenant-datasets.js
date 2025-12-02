const DATA_TYPE_ELASTIC = "elasticsearch";
const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";
const ES_INDEX = "hi-ga-tijdschriften";
const DATA_TYPE = DATA_TYPE_ELASTIC;
const ID_PROPERTY_FOR_ELASTIC = "";
const BASE_URL_FOR_ELASTIC = "";

const tenantDb = db.getSiblingDB(TENANT_DB);

const dataConfiguration = { id_property: ID_PROPERTY_FOR_ELASTIC, base_url: BASE_URL_FOR_ELASTIC };

tenantDb.datasets.updateOne(
    { tenant_name: TENANT_DB, name: DATASET_NAME },
    {
        $set: {
            tenant_name: TENANT_DB,
            name: DATASET_NAME,
            es_index: `${ES_INDEX}-tijdschriften`,
            data_type: DATA_TYPE,
            data_configuration: dataConfiguration,
            detail_id: "id"
        }
    },
    { upsert: true }
);
tenantDb.datasets.updateOne(
    { tenant_name: TENANT_DB, name: `${DATASET_NAME}-plaatsnaam` },
    {
        $set:{
            tenant_name: TENANT_DB,
            name: `${DATASET_NAME}-plaatsnaam`,
            es_index: `${ES_INDEX}-plaatsnaam`,
            data_type: DATA_TYPE,
            data_configuration: dataConfiguration,
            detail_id: "id"
        }
    },
    { upsert: true }
);
tenantDb.datasets.updateOne(
    { tenant_name: TENANT_DB, name: `${DATASET_NAME}-personen` },
    {
        $set:{
            tenant_name: TENANT_DB,
            name: `${DATASET_NAME}-personen`,
            es_index: `${ES_INDEX}-personen`,
            data_type: DATA_TYPE,
            data_configuration: dataConfiguration,
            detail_id: "id"
        }
    },
    { upsert: true }
);
tenantDb.datasets.updateOne(
    { tenant_name: TENANT_DB, name: `${DATASET_NAME}-uitgever_drukker` },
    {
        $set:{
            tenant_name: TENANT_DB,
            name: `${DATASET_NAME}-uitgever_drukker`,
            es_index: `${ES_INDEX}-uitgever_drukker`,
            data_type: DATA_TYPE,
            data_configuration: dataConfiguration,
            detail_id: "id"
        }
    },
    { upsert: true }
);

