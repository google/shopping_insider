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
        _PARTITIONDATE AS data_date,
        entity_id,
        country_code AS target_country,
        TRUE AS is_best_seller,
      FROM
        `{project_id}.{dataset}.BestSellersProductClusterWeekly_{merchant_id}`
          AS BestSellers
    ),
    Products AS (
      SELECT
        * EXCEPT (_DATA_DATE, _LATEST_DATE),
        _DATA_DATE AS data_date,
        _LATEST_DATE AS latest_date,
        IF(
          sale_price_effective_start_date <= CURRENT_TIMESTAMP()
            AND sale_price_effective_end_date > CURRENT_TIMESTAMP(),
          sale_price.value,
          price.value) AS effective_price,
        SPLIT(unique_product_id, '|')[1] AS product_id
      FROM
        `{project_id}.{dataset}.product_detailed_materialized`
    ),
    PriceBenchmarks AS (
      SELECT
        _PARTITIONDATE AS data_date,
        CONCAT(CAST(merchant_id AS STRING), '|', id) AS unique_product_id,
        report_country_code AS target_country,
        benchmark_price.amount_micros / 1000000 AS price_benchmark_value,
        benchmark_price.currency_code AS price_benchmark_currency,
        NULL AS price_benchmark_timestamp
      FROM
        `{project_id}.{dataset}.PriceCompetitiveness_{merchant_id}`
    )
  SELECT
    Products AS product,
    BestSellers AS best_sellers,
    STRUCT(
      PriceBenchmarks.data_date,
      PriceBenchmarks.unique_product_id,
      PriceBenchmarks.target_country,
      PriceBenchmarks.price_benchmark_value,
      PriceBenchmarks.price_benchmark_currency,
      PriceBenchmarks.price_benchmark_timestamp,
      CASE
        WHEN PriceBenchmarks.price_benchmark_value IS NULL THEN ''
        WHEN (SAFE_DIVIDE(Products.effective_price, PriceBenchmarks.price_benchmark_value) - 1) < -0.01
          THEN 'Less than PB'
        WHEN (SAFE_DIVIDE(Products.effective_price, PriceBenchmarks.price_benchmark_value) - 1) > 0.01
          THEN 'More than PB'
        ELSE 'Equal to PB'
        END AS price_competitiveness_band,
      SAFE_DIVIDE(Products.effective_price, PriceBenchmarks.price_benchmark_value) - 1
        AS price_vs_benchmark
    ) AS price_benchmarks
  FROM Products
  LEFT JOIN `{project_id}.{dataset}.BestSellersEntityProductMapping_{merchant_id}`
    USING (product_id)
  LEFT JOIN BestSellers
    USING (data_date, entity_id, target_country)
  LEFT JOIN PriceBenchmarks
    USING (data_date, unique_product_id, target_country)
);
