const TENANT_DB = "tenant-a";
const DATASET_NAME = "politieke-tijdschriften";

const tenantDb = db.getSiblingDB(TENANT_DB);

tenantDb.detail_properties.deleteMany({ dataset_name: DATASET_NAME });
tenantDb.detail_properties.insertMany([
    { dataset_name: DATASET_NAME, name: "tijdschrift", type: "screen", path: "$", order: 1, config: {
        "id": "tijdschrift-detail",
        "screenType": "normal",
        "globals": {},
        "form": {
          "rows": [
            {
              "displayType": "group",
              "groupId": "titel",
              "rows": [
                {
                  "elements": [
                    {
                      "value": "$data#$.lidwoordTitel",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.titelVanTijdschrift",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.onderTitel",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.motto",
                      "type": "label"
                    }
                  ]
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "periode",
              "rows": [
                {
                  "columns": [
                    {
                      "elements": [
                        {
                          "value": "$data#$.eersteNummer",
                          "type": "label"
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.laatsteNummer",
                          "type": "label"
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.prijsDuiten",
                          "type": "label"
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.afleveringen",
                          "type": "label"
                        }
                      ]
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.frequentie",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.formaat",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.dagen-info",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.datering",
                      "type": "markdown"
                    }
                  ]
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "publicatie",
              "rows": [
                {
                  "elements": [
                    {
                      "value": "$data#$.uitgever",
                      "type": "link",
                      "config": {
                        "url": "/politieke-tijdschriften-uitgever_drukker/details/$uitgeverId"
                      }
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.drukker",
                      "type": "link",
                      "config": {
                        "url": "/politieke-tijdschriften-uitgever_drukker/details/$drukkerId"
                      }
                    }
                  ]
                },
                {
                  "columns": [
                    {
                      "elements": [
                        {
                          "value": "$data#$.plaats",
                          "type": "search-link",
                          "config": {
                            "url": "/politieke-tijdschriften/search?plaatsVanUitgave=$plaats"
                          }
                        },
                        {
                          "value": {
                            "latitude": "$data#$.plaatsBreedtegraad",
                            "longitude": "$data#$.plaatsLengtegraad"
                          },
                          "type": "map",
                          "config": {
                            "zoom": new NumberInt("8")
                          }
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.plaats2",
                          "type": "search-link",
                          "config": {
                            "url": "/politieke-tijdschriften/search?plaatsVanUitgave=$plaats2"
                          }
                        },
                        {
                          "value": {
                            "latitude": "$data#$.plaats2Breedtegraad",
                            "longitude": "$data#$.plaats2Lengtegraad"
                          },
                          "type": "map",
                          "config": {
                            "zoom": new NumberInt("8")
                          }
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.plaats3",
                          "type": "search-link",
                          "config": {
                            "url": "/politieke-tijdschriften/search?plaatsVanUitgave=$plaats3"
                          }
                        },
                        {
                          "value": {
                            "latitude": "$data#$.plaats3Breedtegraad",
                            "longitude": "$data#$.plaats3Lengtegraad"
                          },
                          "type": "map",
                          "config": {
                            "zoom": new NumberInt("8")
                          }
                        }
                      ]
                    }
                  ]
                },
                {
                  "columns": [
                    {
                      "elements": [
                        {
                          "value": "$data#$.uitgeverZeker",
                          "type": "toggle"
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.drukkerZeker",
                          "type": "toggle"
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.nietBewaard",
                          "type": "toggle"
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.vrijheidGelijkheidBroederschap",
                          "type": "toggle"
                        }
                      ]
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.bewaarplaats",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.vervolgTitelVan",
                      "type": "link",
                      "config": {
                        "url": "/politieke-tijdschriften/details/$vervolgTitelVanId"
                      }
                    }
                  ]
                },
                {
                  "columns": [
                    {
                      "elements": [
                        {
                          "value": "$data#$.zieOok1",
                          "type": "link",
                          "config": {
                            "url": "/politieke-tijdschriften/details/$zieOok1Id"
                          }
                        }
                      ]
                    },
                    {
                      "elements": [
                        {
                          "value": "$data#$.zieOok2",
                          "type": "link",
                          "config": {
                            "url": "/politieke-tijdschriften/details/$zieOok2Id"
                          }
                        }
                      ]
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.auteurs-redacteurs",
                      "type": "counted-list"
                    }
                  ]
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "classificatie",
              "rows": [
                {
                  "elements": [
                    {
                      "value": "$data#$.vormTijdschrift",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.typeTijdschrift",
                      "type": "label"
                    }
                  ]
                },
                {
                  "elements": [
                    {
                      "value": "$data#$.politiekePositie",
                      "type": "label"
                    }
                  ]
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "inhoud",
              "elements": [
                {
                  "value": "$data#$.korteOmschrijvingInhoud",
                  "type": "markdown",
                  "config": {}
                },
                {
                  "value": "$data#$.verantwoordingSelectie",
                  "type": "markdown",
                  "config": {}
                },
                {
                  "value": "$data#$.toelichtingRedacteurAuteur",
                  "type": "markdown",
                  "config": {}
                },
                {
                  "value": "$data#$.duidingTitel",
                  "type": "markdown"
                },
                {
                  "value": "$data#$.advertenties_en_andere_verwijsplaatsen",
                  "type": "markdown"
                },
                {
                  "value": "$data#$.opmaak_en_spelling",
                  "type": "label"
                },
                {
                  "value": "$data#$.apparaat_Hanou",
                  "type": "label"
                },
                {
                  "value": "$data#$.apparaat_Kluit",
                  "type": "label"
                },
                {
                  "value": "$data#$.politiekePositieToelichting",
                  "type": "markdown"
                },
                {
                  "value": "$data#$.doelTijdschrift",
                  "type": "markdown"
                },
                {
                  "value": "$data#$.reactie_en_of_justitie",
                  "type": "markdown"
                },
                {
                  "value": "$data#$.oplage_en_bereik",
                  "type": "markdown"
                },
                {
                  "value": "$data#$.opmerkingen",
                  "type": "markdown"
                },
                {
                  "value": "$data#$.digitaalExemplaar",
                  "type": "markdown"
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "aanvullende-titels",
              "elements": [
                {
                  "value": "$data#$.aanvullendeTitels",
                  "type": "counted-list"
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "artikel-types",
              "elements": [
                {
                  "value": "$data#$.artikelType",
                  "type": "markdown-list"
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "saakes",
              "elements": [
                {
                  "value": "$data#$.linkNaarSaakes",
                  "type": "markdown-list"
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "stcn",
              "elements": [
                {
                  "value": "$data#$.STCN",
                  "type": "markdown-list"
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "ent",
              "elements": [
                {
                  "value": "$data#$.ENT",
                  "type": "markdown-list"
                }
              ]
            },
            {
              "displayType": "group",
              "groupId": "seclit",
              "elements": [
                {
                  "value": "$data#$.secundaireLiteratuur",
                  "type": "markdown-list"
                }
              ]
            }
          ]
        }
      }
    }
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-plaatsnaam` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-plaatsnaam`, name: "plaats", type: "screen", path: "$", order: 1, config: {
      "id": "plaats-detail",
      "screenType": "normal",
      "globals": {
      },
      "form": {
        "rows": [
          {
            "displayType": "group",
            "groupId": "plaats",
            "rows": [
              {
                "elements": [
                  {
                    "value": "$data#$.plaatsnaam",
                    "type": "label"
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": {
                      "latitude": "$data#$.coordinaten.breedteGraad",
                      "longitude": "$data#$.coordinaten.lengteGraad"
                    },
                    "type": "map",
                    "config": {
                      "zoom": 6
                    }
                  }
                ]
              },
              {
                "columns": [
                  {
                    "elements": [
                      {
                        "value": "$data#$.coordinaten.breedteGraad",
                        "type": "label"
                      }
                    ]
                  },
                  {
                    "elements": [
                      {
                        "value": "$data#$.coordinaten.lengteGraad",
                        "type": "label"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }
    }}
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-personen` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-personen`, name: "persoon", type: "screen", path: "$", order: 1, config: {
      "id": "personen-detail",
      "screenType": "normal",
      "globals": {
      },
      "form": {
        "rows": [
          {
            "displayType": "group",
            "groupId": "persoon",
            "rows": [
              {
                "elements": [
                  {
                    "value": "$data#$.persoonsNaam",
                    "type": "label"
                  }
                ]
              },
              {
                "columns": [
                  {
                    "elements": [
                      {
                        "value": "$data#$.geboorteJaar",
                        "type": "label"
                      }
                    ]
                  },
                  {
                    "elements": [
                      {
                        "value": "$data#$.sterfJaar",
                        "type": "label"
                      }
                    ]
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": "$data#$.bioPortaal",
                    "type": "label"
                  }
                ]
              },
              {
                "columns": [
                  {
                    "elements": [
                      {
                        "value": "$data#$.eersteBeroep",
                        "type": "label"
                      }
                    ]
                  },
                  {
                    "elements": [
                      {
                        "value": "$data#$.eersteBeroepOpmerking",
                        "type": "label"
                      }
                    ]
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": "$data#$.tweedeBeroep",
                    "type": "label"
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": "$data#$.derdeBeroep",
                    "type": "label"
                  }
                ]
              }
            ]
          }
        ]
      }
  }}
]);
tenantDb.detail_properties.deleteMany({ dataset_name: `${DATASET_NAME}-uitgever_drukker` });
tenantDb.detail_properties.insertMany([
    { dataset_name: `${DATASET_NAME}-uitgever_drukker`, name: "uitgever_drukker", type: "screen", path: "$", order: 1, config: {
      "id": "uitgever-drukker-detail",
      "screenType": "normal",
      "globals": {
      },
      "form": {
        "rows": [
          {
            "displayType": "group",
            "groupId": "uitgever-drukker",
            "rows": [
              {
                "elements": [
                  {
                    "value": "$data#$.uitgever",
                    "type": "label"
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": "$data#$.eersteGeneratieNaam",
                    "type": "link",
                    "config": {
                      "url": "/politieke-tijdschriften-personen/details/$eersteGeneratieId"
                    }
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": "$data#$.tweedeGeneratieNaam",
                    "type": "link",
                    "config": {
                      "url": "/politieke-tijdschriften-personen/details/$tweedeGeneratieId"
                    }
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": "$data#$.derdeGeneratieNaam",
                    "type": "link",
                    "config": {
                      "url": "/politieke-tijdschriften-personen/details/$derdeGeneratieId"
                    }
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": "$data#$.politiekSignatuur",
                    "type": "label"
                  }
                ]
              },
              {
                "elements": [
                  {
                    "value": "$data#$.bestaan",
                    "type": "label"
                  }
                ]
              }
            ]
          }
        ]
      }
    }}
]);