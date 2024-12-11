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

CREATE OR REPLACE TABLE `{project_id}.{dataset}.market_insights_best_sellers_materialized` AS (
  SELECT
    _PARTITIONDATE AS data_date,
    NULL AS rank_id,
    rank,
    previous_rank,
    country_code AS ranking_country,
    report_category_id AS ranking_category,
    NULL AS ranking_category_path,
    category_l1 AS ranking_category_name_l1,
    category_l2 AS ranking_category_name_l2,
    category_l3 AS ranking_category_name_l3,
    title AS product_title,
    SPLIT(variant_gtins, ' ') AS gtins,
    brand,
    CASE
      WHEN
        category_l5 != ""
      THEN
        category_l1 || " > " || category_l2 || " > " || category_l3 || " > " || category_l4 || " > " || category_l5
      WHEN
        category_l4 != ""
      THEN
        category_l1 || " > " || category_l2 || " > " || category_l3 || " > " || category_l4
      WHEN
        category_l3 != ""
      THEN
        category_l1 || " > " || category_l2 || " > " || category_l3
      WHEN
        category_l2 != ""
      THEN
        category_l1 || " > " || category_l2
      WHEN
        category_l1 != ""
      THEN
        category_l1
      ELSE
        NULL
    END AS google_product_category_path,
    report_category_id AS google_product_category,
    NULL AS min,
    NULL AS max,
    NULL AS currency,
    IF(product_inventory_status = "in_inventory", TRUE, FALSE) AS is_in_inventory
  FROM
    `{project_id}.{dataset}.BestSellersProductClusterWeekly_{merchant_id}`
  WHERE
    _PARTITIONDATE = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
);
