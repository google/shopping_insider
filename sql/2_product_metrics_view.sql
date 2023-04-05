# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Creates a snapshot of product_metrics_view.
#
# The ads_ShoppingProductStats_<External Customer Id> table has shopping performance metrics.
# This view will get latest metrics data and create derived columns useful for further processing of
# data.

CREATE OR REPLACE VIEW `{project_id}.{dataset}.product_metrics_view_{external_customer_id}`
AS (
  WITH
    GeoTargets AS (
      SELECT DISTINCT
        parent_id,
        country_code
      FROM
        `{project_id}.{dataset}.geo_targets`
    ),
    LanguageCodes AS (
      SELECT DISTINCT
        criterion_id,
        language_code
      FROM
        `{project_id}.{dataset}.language_codes`
    ),
    ShoppingProductStats AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        customer_id,
        segments_product_merchant_id AS merchant_id,
        segments_product_channel AS channel,
        segments_product_item_id AS offer_id,
        CAST(SPLIT(segments_product_country, '/')[SAFE_OFFSET(1)] AS INT64)
          AS country_criterion_id,
        CAST(SPLIT(segments_product_language, '/')[SAFE_OFFSET(1)] AS INT64)
          AS language_criterion_id,
        metrics_impressions AS impressions,
        metrics_clicks AS clicks,
        metrics_cost_micros AS cost,
        metrics_conversions AS conversions,
        metrics_conversions_value AS conversions_value
      FROM
        `{project_id}.{dataset}.ads_ShoppingProductStats_{external_customer_id}`
    )
  SELECT
    ShoppingProductStats._DATA_DATE,
    ShoppingProductStats._LATEST_DATE,
    ShoppingProductStats.customer_id,
    ShoppingProductStats.merchant_id,
    ShoppingProductStats.channel,
    ShoppingProductStats.offer_id,
    LanguageCodes.language_code,
    GeoTargets.country_code AS target_country,
    SUM(ShoppingProductStats.impressions) AS impressions,
    SUM(ShoppingProductStats.clicks) AS clicks,
    SUM(ShoppingProductStats.cost) AS cost,
    SUM(ShoppingProductStats.conversions) AS conversions,
    SUM(ShoppingProductStats.conversions_value) AS conversions_value
  FROM
    ShoppingProductStats
  INNER JOIN
    GeoTargets
    ON
      GeoTargets.parent_id = ShoppingProductStats.country_criterion_id
  INNER JOIN
    LanguageCodes
    ON LanguageCodes.criterion_id = ShoppingProductStats.language_criterion_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);
