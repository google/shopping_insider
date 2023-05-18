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

# Creates a latest snapshot view with Best Sellers & Price Benchmarks

CREATE OR REPLACE VIEW `{project_id}.{dataset}.market_insights_snapshot_view`
AS (
  WITH
    BestSellers AS (
      SELECT DISTINCT
        _PARTITIONDATE AS _DATA_DATE,
        CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
        SPLIT(rank_id, ':')[SAFE_ORDINAL(2)] AS target_country,
        TRUE AS is_best_seller,
      FROM
        `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{merchant_id}`
          AS BestSellers
    ),
    Products AS (
      SELECT
        *,
        IF(
          sale_price_effective_start_date <= CURRENT_TIMESTAMP()
            AND sale_price_effective_end_date > CURRENT_TIMESTAMP(),
          sale_price.value,
          price.value) AS effective_price
      FROM
        `{project_id}.{dataset}.product_detailed_materialized`
    ),
    PriceBenchmarks AS (
      SELECT
        _PARTITIONDATE AS _DATA_DATE,
        CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
        country_of_sale AS target_country,
        price_benchmark_value,
        price_benchmark_currency,
        price_benchmark_timestamp
      FROM
        `{project_id}.{dataset}.Products_PriceBenchmarks_{merchant_id}`
    )
  SELECT
    Products.*,
    IFNULL(BestSellers.is_best_seller, FALSE) AS is_best_seller,
    PriceBenchmarks.price_benchmark_value,
    PriceBenchmarks.price_benchmark_currency,
    PriceBenchmarks.price_benchmark_timestamp,
    CASE
      WHEN PriceBenchmarks.price_benchmark_value IS NULL THEN ''
      WHEN (PriceBenchmarks.price_benchmark_value - Products.effective_price) < 0
        THEN 'Less than price benchmark'
      WHEN (PriceBenchmarks.price_benchmark_value - Products.effective_price) > 0
        THEN 'More than price benchmark'
      ELSE 'Equal to price benchmark'
      END AS price_competitiveness_band,
    SAFE_DIVIDE(Products.effective_price, PriceBenchmarks.price_benchmark_value) - 1
      AS price_vs_benchmark,
  FROM Products
  LEFT JOIN BestSellers
    USING (_DATA_DATE, unique_product_id, target_country)
  LEFT JOIN PriceBenchmarks
    USING (_DATA_DATE, unique_product_id, target_country)
);
