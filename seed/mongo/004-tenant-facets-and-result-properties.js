const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

// ---------- FACETS ----------
tenantDb.facets.createIndex({ dataset_name: 1, name: 1 }, { unique: true });

// type oneOf { "text", "tree", "range" }
tenantDb.facets.deleteMany({ dataset_name: DATASET_NAME });
// tenantDb.facets.insertMany([
//    { dataset_name: DATASET_NAME, name: "Tijdschrift-titel",   property: "title1.keyword", type: "text" },
// ]);

// ---------- RESULT PROPERTIES ----------
tenantDb.result_properties.createIndex({ dataset_name: 1, order: 1 }, { unique: true });
tenantDb.result_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.result_properties.insertMany([
    { dataset_name: DATASET_NAME, name: "id",          path: "$.id",                type: 'number', order: 0 },
    { dataset_name: DATASET_NAME, name: "title",       path: "$.title1",            type: 'text', order: 1 },
    { dataset_name: DATASET_NAME, name: "description", path: "$.korteOmschrijving", type: 'text', order: 2 },
]);
