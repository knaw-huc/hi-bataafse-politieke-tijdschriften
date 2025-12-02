const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

// ---------- FACETS ----------
tenantDb.facets.deleteMany({ dataset_name: DATASET_NAME });
 tenantDb.facets.insertMany([
     { dataset_name: DATASET_NAME, name: "Vorm tijdschrift", property: "vormTijdschrift.keyword", type: "text", order: 4 },
     { dataset_name: DATASET_NAME, name: "Type tijdschrift", property: "typeTijdschrift.keyword", type: "text", order: 5 },
     { dataset_name: DATASET_NAME, name: "Politieke positie", property: "politiekePositie.keyword", type: "text", order: 6 },
     { dataset_name: DATASET_NAME, name: "Jaar eerste nummer", property: "eersteNummerJaar.keyword", type: "text", order: 7 },
     //{ dataset_name: DATASET_NAME, name: "Titel", property: "titel.keyword", type: "text", order: 0 },
     { dataset_name: DATASET_NAME, name: "Uitgever", property: "uitgever.keyword", type: "text", order: 1 },
     { dataset_name: DATASET_NAME, name: "Drukker", property: "drukker.keyword", type: "text", order: 2 },
     { dataset_name: DATASET_NAME, name: "Plaats", property: "plaats.keyword", type: "text", order: 3 }
]);

// ---------- RESULT PROPERTIES ----------
tenantDb.result_properties.createIndex({ dataset_name: 1, order: 1 }, { unique: true });
tenantDb.result_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.result_properties.insertMany([
    { dataset_name: DATASET_NAME, name: "id", path: "$.id", type: 'number', order: 0 },
    { dataset_name: DATASET_NAME, name: "title", path: "$.titel", type: 'text', order: 1 },
]);
tenantDb.result_properties.deleteMany({ dataset_name: `${DATASET_NAME}-plaatsnaam` });
tenantDb.result_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-plaatsnaam`, name: "id", path: "$.id", type: 'number', order: 0 },
    { dataset_name: `${DATASET_NAME}-plaatsnaam`, name: "title", path: "$.plaatsnaam", type: 'text', order: 1 },
]);
tenantDb.result_properties.deleteMany({ dataset_name: `${DATASET_NAME}-personen` });
tenantDb.result_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-personen`, name: "id", path: "$.id", type: 'number', order: 0 },
    { dataset_name: `${DATASET_NAME}-personen`, name: "title", path: "$.persoonsNaam", type: 'text', order: 1 },
]);
tenantDb.result_properties.deleteMany({ dataset_name: `${DATASET_NAME}-uitgever_drukker` });
tenantDb.result_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-uitgever_drukker`, name: "id", path: "$.id", type: 'number', order: 0 },
    { dataset_name: `${DATASET_NAME}-uitgever_drukker`, name: "title", path: "$.uitgever", type: 'text', order: 1 },
]);