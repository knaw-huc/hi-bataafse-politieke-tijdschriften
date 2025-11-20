
# Bataafse politieke tijdschriften

## Procrustus, Panoptes API and Panoptes Browser

The penultimate triple of projects to get you started with a generic browser.

## Main flow

In a nutshell, the main flow is as follows:

- Take the input data and transform this in some way to a suitable input format: for now, this will be JSON.
- Create a configuration for the Procrustus indexer to read and index your converted data into ElasticSearch indexes (mapping A).
- Read and index your converted data in ElasticSearch.
- Create configuration for Panoptes to map ElasticSearch indexes to a suitable format for the Panoptes API to serve to the generic browser (mapping B).
- Read this configuration into MongoDB and have Panoptes serve the data to the Panoptes generic browser.

# Procrustus indexer configuration — Mapping A

Procrustus attempts to convert input data into a suitable format for (ElasticSearch) indexing, by converting input JSON files into a collection of output JSON files, 
where properties in the input are mapped onto desired properties in the output. This allows flattening of complex, nested objects into more
manageable data structures. After this conversion, the indexer reads the created output JSON files and imports them into ElasticSearch.

The configuration for Procustus read and index is specified in a TOML file where the mapping from input to output is defined. This file also steers the needed
elements for proper ElasticSearch index creation.

See https://github.com/knaw-huc/procrustus-indexer?tab=readme-ov-file#toml-configuration for more information on read and index configuration.


# Panoptes MongoDB configuration - Mapping B

Panoptes acts as a backend for a generic collection browser and contains configuration for the mapping of ElasticSearch indexes to Panoptes API structures.

Panoptes is setup to act in a multi-tenant setup with a single Panoptes API instance serving multiple browsers for accessing multiple datasets.

The configuration for the tenants and datasets in Panoptes is managed in MongoDB. In order to configure this, you need to have at least two databases: one named main and one with the name of your tenant.

# Docker Compose

The current Docker Compose setup is mainly aimed at development. This spins up an ElasticSearch, a MongoDB and the Panoptes API. If you want to seed ElasticSearch and MongoDB, uncomment the 'es-init' container in the Docker Compose file.

You can verify the existence of indexes in ElasticSearch by visiting: http://localhost:9200/_cat/indices?format=json