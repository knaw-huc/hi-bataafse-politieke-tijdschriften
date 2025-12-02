const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

tenantDb.detail_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.detail_properties.insertMany([
    { dataset_name: DATASET_NAME, name: "description",    type: "list", path: "$",   order: 1 }
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-plaatsnaam` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-plaatsnaam`, name: "description", type: "list", path: "$", order: 1 }
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-personen` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-personen`, name: "description", type: "list", path: "$", order: 1 }
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-uitgever_drukker` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-uitgever_drukker`, name: "description", type: "list", path: "$", order: 1 }
]);

