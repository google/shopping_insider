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

# Creates a snapshot view of products combined with performance metrics.

CREATE OR REPLACE VIEW `{project_id}.{dataset}.product_detailed_view_{external_customer_id}`
AS
WITH
  ProductMetrics AS (
    SELECT
      ProductView._DATA_DATE,
      ProductView.unique_product_id,
      ProductMetricsView.customer_id,
      ProductView.target_country,
      IFNULL(SUM(ProductMetricsView.impressions), 0) AS impressions_30_days,
      IFNULL(SUM(ProductMetricsView.clicks), 0) AS clicks_30_days,
      IFNULL(SUM(ProductMetricsView.cost), 0) AS cost_30_days,
      IFNULL(SUM(ProductMetricsView.conversions), 0) AS conversions_30_days,
      IFNULL(SUM(ProductMetricsView.conversions_value), 0) AS conversions_value_30_days,
      COUNTIF(ProductMetricsView.impressions > 0) AS days_has_impressions,
      COUNTIF(ProductMetricsView.clicks > 0) AS days_has_clicks,
      SAFE_DIVIDE(
        SUM(ProductMetricsView.cost),
        SUM(ProductMetricsView.clicks)) AS cpc_30_days,
      SAFE_DIVIDE(
        SUM(ProductMetricsView.cost),
        SUM(ProductMetricsView.impressions) * 1000) AS cpm_30_days,
      SAFE_DIVIDE(
        SUM(ProductMetricsView.clicks),
        SUM(ProductMetricsView.impressions)) AS ctr_30_days
    FROM
      `{project_id}.{dataset}.product_metrics_view_{external_customer_id}` AS ProductMetricsView
    INNER JOIN
      `{project_id}.{dataset}.product_view_{merchant_id}` AS ProductView
      ON
        ProductMetricsView.merchant_id = ProductView.merchant_id
        AND LOWER(ProductMetricsView.channel) = LOWER(ProductView.channel)
        AND LOWER(ProductMetricsView.language_code) = LOWER(ProductView.content_language)
        AND LOWER(ProductMetricsView.target_country) = LOWER(ProductView.target_country)
        AND LOWER(ProductMetricsView.offer_id) = LOWER(ProductView.offer_id)
        AND ProductMetricsView._DATA_DATE
          BETWEEN DATE_SUB(ProductView._DATA_DATE, INTERVAL 30 DAY)
          AND ProductView._DATA_DATE
    GROUP BY 1, 2, 3, 4
  ),
  ProductData AS (
    SELECT
      ProductView._DATA_DATE,
      ProductView._LATEST_DATE,
      COALESCE(ProductView.aggregator_id, ProductView.merchant_id) AS account_id,
      MAX(customer_view.customer_descriptive_name) AS account_display_name,
      ProductView.merchant_id AS sub_account_id,
      ProductView.unique_product_id,
      ProductView.target_country,
      MAX(ProductView.offer_id) AS offer_id,
      MAX(ProductView.channel) AS channel,
      MAX(ProductView.in_stock) AS in_stock,
      # An offer is labeled as approved when able to serve on all destinations
      MAX(ProductView.is_approved) AS is_approved,
      # Aggregated Issues & Servability Statuses
      MAX(ProductView.disapproval_issues) AS disapproval_issues,
      MAX(ProductView.demotion_issues) AS demotion_issues,
      MAX(ProductView.warning_issues) AS warning_issues,
      MIN(IF(TargetedProduct.product_id IS NULL, 0, 1)) AS is_targeted,
      MAX(ProductView.title) AS title,
      MAX(ProductView.link) AS item_url,
      MAX(ProductView.product_type_l1) AS product_type_l1,
      MAX(ProductView.product_type_l2) AS product_type_l2,
      MAX(ProductView.product_type_l3) AS product_type_l3,
      MAX(ProductView.product_type_l4) AS product_type_l4,
      MAX(ProductView.product_type_l5) AS product_type_l5,
      MAX(ProductView.google_product_category_l1) AS google_product_category_l1,
      MAX(ProductView.google_product_category_l2) AS google_product_category_l2,
      MAX(ProductView.google_product_category_l3) AS google_product_category_l3,
      MAX(ProductView.google_product_category_l4) AS google_product_category_l4,
      MAX(ProductView.google_product_category_l5) AS google_product_category_l5,
      MAX(ProductView.custom_labels.label_0) AS custom_label_0,
      MAX(ProductView.custom_labels.label_1) AS custom_label_1,
      MAX(ProductView.custom_labels.label_2) AS custom_label_2,
      MAX(ProductView.custom_labels.label_3) AS custom_label_3,
      MAX(ProductView.custom_labels.label_4) AS custom_label_4,
      MAX(ProductView.brand) AS brand,
      MAX(ProductMetrics.impressions_30_days) AS impressions_30_days,
      MAX(ProductMetrics.clicks_30_days) AS clicks_30_days,
      MAX(ProductMetrics.cost_30_days) AS cost_30_days,
      MAX(ProductMetrics.conversions_30_days) AS conversions_30_days,
      MAX(ProductMetrics.conversions_value_30_days) AS conversions_value_30_days,
      MAX(ProductMetrics.cpc_30_days) AS cpc_30_days,
      MAX(ProductMetrics.cpm_30_days) AS cpm_30_days,
      MAX(ProductMetrics.ctr_30_days) AS ctr_30_days,
      MAX(ProductMetrics.days_has_impressions) AS days_has_impressions,
      MAX(ProductMetrics.days_has_clicks) AS days_has_clicks,
      MAX(ProductView.description) AS description,
      MAX(ProductView.mobile_link) AS mobile_link,
      MAX(ProductView.image_link) AS image_link,
      ANY_VALUE(ProductView.additional_image_links) AS additional_image_links,
      MAX(ProductView.content_language) AS content_language,
      MAX(ProductView.expiration_date) AS expiration_date,
      MAX(ProductView.google_expiration_date) AS google_expiration_date,
      MAX(ProductView.adult) AS adult,
      MAX(ProductView.age_group) AS age_group,
      MAX(ProductView.availability) AS availability,
      MAX(ProductView.availability_date) AS availability_date,
      MAX(ProductView.color) AS color,
      MAX(ProductView.condition) AS condition,
      MAX(ProductView.gender) AS gender,
      MAX(ProductView.gtin) AS gtin,
      MAX(ProductView.item_group_id) AS item_group_id,
      MAX(ProductView.material) AS material,
      MAX(ProductView.mpn) AS mpn,
      MAX(ProductView.pattern) AS pattern,
      ANY_VALUE(ProductView.price) AS price,
      ANY_VALUE(ProductView.sale_price) AS sale_price,
      MAX(ProductView.sale_price_effective_start_date) AS sale_price_effective_start_date,
      MAX(ProductView.sale_price_effective_end_date) AS sale_price_effective_end_date,
      ANY_VALUE(ProductView.additional_product_types) AS additional_product_types,
      DATE_DIFF(
        DATE(
          LEAST(
            MAX(COALESCE(ProductView.expiration_date, ProductView.google_expiration_date)),
            MAX(ProductView.google_expiration_date))),
        ProductView._DATA_DATE,
        DAY) AS days_until_expiration
    FROM
      `{project_id}.{dataset}.product_view_{merchant_id}` AS ProductView
    LEFT JOIN
      ProductMetrics
      ON
        ProductMetrics._DATA_DATE = ProductView._DATA_DATE
        AND ProductMetrics.unique_product_id = ProductView.unique_product_id
        AND ProductMetrics.target_country = ProductView.target_country
    LEFT JOIN
      `{project_id}.{dataset}.customer_view_{external_customer_id}` AS customer_view
      ON
        customer_view.customer_id = ProductMetrics.customer_id
        AND customer_view._DATA_DATE = ProductMetrics._DATA_DATE
    LEFT JOIN
      `{project_id}.{dataset}.targeted_products_view_{external_customer_id}` AS TargetedProduct
      ON
        TargetedProduct.merchant_id = ProductView.merchant_id
        AND TargetedProduct.product_id = ProductView.product_id
        AND TargetedProduct._DATA_DATE = ProductView._DATA_DATE
        AND TargetedProduct.target_country = ProductView.target_country
    GROUP BY
      _DATA_DATE,
      _LATEST_DATE,
      account_id,
      sub_account_id,
      unique_product_id,
      target_country
  )
SELECT
  * EXCEPT (
    impressions_30_days,
    clicks_30_days,
    cost_30_days,
    conversions_30_days,
    conversions_value_30_days,
    ctr_30_days,
    cpc_30_days,
    cpm_30_days,
    days_has_impressions,
    days_has_clicks),
  IFNULL(impressions_30_days, 0) AS impressions_30_days,
  IFNULL(clicks_30_days, 0) AS clicks_30_days,
  IFNULL(cost_30_days, 0) AS cost_30_days,
  IFNULL(conversions_30_days, 0) AS conversions_30_days,
  IFNULL(conversions_value_30_days, 0) AS conversions_value_30_days,
  IFNULL(cpc_30_days, 0) AS cpc_30_days,
  IFNULL(cpm_30_days, 0) AS cpm_30_days,
  IFNULL(ctr_30_days, 0) AS ctr_30_days,
  IFNULL(days_has_impressions, 0) AS days_has_impressions,
  IFNULL(days_has_clicks, 0) AS days_has_clicks,
  CASE
    WHEN is_approved = 1 AND in_stock = 1
      THEN 1
    ELSE 0
    END AS funnel_in_stock,
  CASE
    WHEN is_approved = 1 AND in_stock = 1 AND is_targeted = 1
      THEN 1
    ELSE 0
    END AS funnel_targeted,
  CASE
    WHEN
      is_approved = 1
      AND in_stock = 1
      AND is_targeted = 1
      AND impressions_30_days > 0
      THEN 1
    ELSE 0
    END AS funnel_has_impression,
  CASE
    WHEN
      is_approved = 1
      AND in_stock = 1
      AND is_targeted = 1
      AND impressions_30_days > 0
      AND clicks_30_days > 0
      THEN 1
    ELSE 0
    END AS funnel_has_clicks
FROM
  ProductData;
