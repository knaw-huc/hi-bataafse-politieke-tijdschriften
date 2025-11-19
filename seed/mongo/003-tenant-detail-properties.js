const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

tenantDb.detail_properties.createIndex({ dataset_name: 1, order: 1 }, { unique: true });

tenantDb.detail_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.detail_properties.insertMany([
    { dataset_name: DATASET_NAME, name: "description",    type: "list", path: "$",   order: 0 },
]);
