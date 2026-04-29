# Bike-Map app
This module contains the frontend assets for the Bike-Map app.

## Setting up config jsons

In the `project_root/apps/bike-map/config/<env>/<city>.json`, add config jsons like the one below. If you've already stood up the infrastructure, you can get the `log_endpoint` and `routing_api_url` values for an env via this `tofu output` command.

```console
$ cd <project_root>/infra
$ AWS_PROFILE=<env_profile_name> tofu output --var-file=<env>.tfvars
```

```json
{
  "city_name": "Chicago",  # The city name as it should be displayed in the site
  "log_endpoint": "<dev env route-logging API url>",
  "routing_api_url": "<dev env routing-api url>",
  "routing_api_key": "<dev routing-api key; should match the one for this city in dev.tfvars>",
  "map_center": [-87.6298, 41.8781],  # Use a point in the center of the city
  "map_zoom": 11,
  "layers": ["crashes", "thefts", "parking"],  # Adjust this for the city's available data
  "about_paragraphs": [
    "Chicago Bike Map helps you explore cycling conditions around the city. The map layers show where crashes, thefts, and bike parking are concentrated based on recent city data, so you can get a sense of patterns in the areas you ride.",
    "Routes prefer streets with better bike infrastructure, lower speed limits, and less severe recent crash history, even if that means a slightly longer ride. We recommend always wearing a helmet while riding.",
    "Routes are generated using OpenStreetMap and City of Chicago data that may be incomplete or outdated. Route suggestions are informational and are no substitute for your own judgment while riding."
  ],
  "terms_what_is": "Chicago Bike Map is a personal project that displays cycling-related data for the Chicago area and generates route suggestions based on publicly available data. It is not a commercial product, and it is not offered by a licensed transportation, safety, or engineering professional.",
  "terms_data_sources": "Map data comes from OpenStreetMap contributors and is subject to the <a href=\"https://opendatacommons.org/licenses/odbl/\" target=\"_blank\" rel=\"noopener\">ODbL license</a>. Crash, theft, and infrastructure data come from the City of Chicago open data portal and other public sources. This project does not independently verify the accuracy or completeness of any of these sources.",
  "terms_liability": "This project is provided \"as is\" without warranties of any kind, express or implied. To the fullest extent permitted by law, the creator of Chicago Bike Map shall not be liable for any damages arising from your use of or reliance on this application, including but not limited to personal injury, property damage, or data loss."
}
```