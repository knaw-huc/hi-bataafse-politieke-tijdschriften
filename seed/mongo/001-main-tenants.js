const TENANT_NAME = "tenant-a";
const TENANT_DOMAIN_LOCAL = "localhost";
const TENANT_DOMAIN_DEV = "bataafse-politieke-tijdschriften.dev.diginfra";
const TENANT_DOMAIN_SD = "bataafse-politieke-tijdschriften.sd.di.huc.knaw.nl";

const mainDb = db.getSiblingDB("main");

mainDb.tenants.createIndex({ domain: 1 }, { unique: true });

mainDb.tenants.updateOne(
    { domain: TENANT_DOMAIN_LOCAL },
    { $set: { name: TENANT_NAME, domain: TENANT_DOMAIN_LOCAL } },
    { upsert: true }
);

mainDb.tenants.updateOne(
    { domain: TENANT_DOMAIN_DEV },
    { $set: { name: TENANT_NAME, domain: `${TENANT_DOMAIN_DEV}.net` } },
    { upsert: true }
);

mainDb.tenants.updateOne(
    { domain: TENANT_DOMAIN_DEV },
    { $set: { name: TENANT_NAME, domain: `${TENANT_DOMAIN_DEV}.org` } },
    { upsert: true }
);

mainDb.tenants.updateOne(
    { domain: TENANT_DOMAIN_SD },
    { $set: { name: TENANT_NAME, domain: `${TENANT_DOMAIN_SD}` } },
    { upsert: true }
);