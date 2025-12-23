const TENANT_DB = "tenant-a";

const tenantDb = db.getSiblingDB(TENANT_DB);

tenantDb.translations.insertMany([
    {
        label_key: 'politieke-tijdschriften',
        label_value: 'Tijdschriften',
        locale: 'NL',
    },
    {
        label_key: 'politieke-tijdschriften-plaatsnaam',
        label_value: 'Plaatsnamen',
        locale: 'NL',
    },
    {
        label_key: 'politieke-tijdschriften-personen',
        label_value: 'Personen',
        locale: 'NL',
    },
    {
        label_key: 'politieke-tijdschriften-uitgever_drukker',
        label_value: 'Uitgever / Drukker',
        locale: 'NL',
    },
    {
        label_key: 'navigation-menu-home',
        label_value: 'Home',
        locale: 'NL',
    },
    {
        label_key: 'navigation-menu',
        label_value: 'Navigatiebalk',
        locale: 'NL',
    },
    {
        label_key: 'search_facet_title',
        label_value: 'Zoeken',
        locale: 'NL',
    },
    {
        label_key: 'results_list_title',
        label_value: 'Resultaten',
        locale: 'NL',
    },
    {
        label_key: 'home',
        label_value: 'Home',
        locale: 'NL',
    }
])
