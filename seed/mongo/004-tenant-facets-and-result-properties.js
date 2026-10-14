const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

// ---------- FACETS ----------
tenantDb.facets.deleteMany({ dataset_name: DATASET_NAME });
 tenantDb.facets.insertMany([
     { dataset_name: DATASET_NAME, name: "Tijdschrift-titel", property: "titel", type: "text", order: 0, start_open: true },
     { dataset_name: DATASET_NAME, name: "Jaar eerste nummer", property: "eersteNummerJaar", type: "range", order: 1 },
     { dataset_name: DATASET_NAME, name: "Jaar laatste nummer", property: "laatsteNummerJaar", type: "range", order: 2 },
     { dataset_name: DATASET_NAME, name: "Jaren in omloop", property: "jarenInOmloop", type: "range", order: 3 },
     { dataset_name: DATASET_NAME, name: "Uitgever", property: "uitgever", type: "text", order: 4, start_open: true },
     { dataset_name: DATASET_NAME, name: "Personen", property: "personen", type: "text", order: 5, start_open: true },
     { dataset_name: DATASET_NAME, name: "Plaats van uitgave", property: "plaatsVanUitgave", type: "text", order: 6, start_open: true },
     { dataset_name: DATASET_NAME, name: "Vorm tijdschrift", property: "vormTijdschrift", type: "text", order: 7, start_open: true },
     { dataset_name: DATASET_NAME, name: "Type tijdschrift", property: "typeTijdschrift", type: "text", order: 8, start_open: true },
     { dataset_name: DATASET_NAME, name: "Type artikel", property: "artikelType", type: "text", order: 9, start_open: true },
     { dataset_name: DATASET_NAME, name: "Frequentie", property: "frequentie", type: "text", order:  10, start_open: true },
     { dataset_name: DATASET_NAME, name: "Politieke positie", property: "politiekePositie", type: "text", order: 11, start_open: true }
]);

// ---------- RESULT PROPERTIES ----------
tenantDb.result_properties.createIndex({ dataset_name: 1, order: 1 }, { unique: true });
tenantDb.result_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.result_properties.insertMany([
    { dataset_name: DATASET_NAME, name: "id", path: "$.id", type: 'number', order: 0 },
    { dataset_name: DATASET_NAME, name: "title", path: "$.titelVanTijdschrift", type: 'text', order: 1 },
    { dataset_name: DATASET_NAME, name: "tags", path: "$.artikelType", type: 'text', order: 2 },
    { dataset_name: DATASET_NAME, name: "numPublications", path: "$.afleveringen", type: 'number', order: 3 },
    { dataset_name: DATASET_NAME, name: "eersteNummerJaar", path: "$.eersteNummerJaar", type: 'number', order: 4 },
    { dataset_name: DATASET_NAME, name: "laatsteNummerJaar", path: "$.laatsteNummerJaar", type: 'number', order: 5 },
    { dataset_name: DATASET_NAME, name: "politiekePositie", path: "$.politiekePositie", type: 'text', order: 6 },
    { dataset_name: DATASET_NAME, name: "jarenInOmloop", path: "$.jarenInOmloop", type: 'number', order: 7 },
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