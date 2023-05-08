# pg2agol

A tool to upload a PostGIS table to ArcGIS Online (AGOL).

The intention of this script is to be run regularly via a cron job, though it
can be run independently as well.

## Setup

At the time of writing, the ArcGIS API for Python (`arcgis`) is at `2.1.0` and
[requires][arcgis-py-reqs] Python `3.7.0` to `3.9.x`.

[arcgis-py-reqs]: https://developers.arcgis.com/python/guide/system-requirements/
