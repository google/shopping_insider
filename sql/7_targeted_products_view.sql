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

# Creates a view with targeted products.

CREATE OR REPLACE VIEW `{project_id}.{dataset}.targeted_products_view_{external_customer_id}`
AS (
  WITH
    IdTargetedOffer AS (
      SELECT DISTINCT
        _DATA_DATE,
        _LATEST_DATE,
        merchant_id,
        target_country,
        offer_id
      FROM
        `{project_id}.{dataset}.pmax_criteria_view_{external_customer_id}` AS Criteria
      WHERE
        offer_id IS NOT NULL
    ),
    IdTargeted AS (
      SELECT
        ProductView._DATA_DATE,
        ProductView._LATEST_DATE,
        ProductView.product_id,
        ProductView.merchant_id,
        ProductView.target_country
      FROM
        `{project_id}.{dataset}.product_view_{merchant_id}` AS ProductView
      INNER JOIN IdTargetedOffer
        ON
          IdTargetedOffer.merchant_id = ProductView.merchant_id
          AND IdTargetedOffer.target_country = ProductView.target_country
          AND TRIM(LOWER(IdTargetedOffer.offer_id)) = TRIM(LOWER(ProductView.offer_id))
          AND IdTargetedOffer._DATA_DATE = ProductView._DATA_DATE
    ),
    NonIdTargeted AS (
      SELECT
        ProductView._DATA_DATE,
        ProductView._LATEST_DATE,
        ProductView.product_id,
        ProductView.merchant_id,
        ProductView.target_country
      FROM
        `{project_id}.{dataset}.product_view_{merchant_id}` AS ProductView
      INNER JOIN `{project_id}.{dataset}.criteria_view_{external_customer_id}` AS Criteria
        ON
          Criteria.merchant_id = ProductView.merchant_id
          AND Criteria.target_country = ProductView.target_country
          AND Criteria._DATA_DATE = ProductView._DATA_DATE
          AND (
            Criteria.custom_label0 IS NULL
            OR Criteria.custom_label0 = TRIM(LOWER(ProductView.custom_labels.label_0)))
          AND (
            Criteria.custom_label1 IS NULL
            OR Criteria.custom_label1 = TRIM(LOWER(ProductView.custom_labels.label_1)))
          AND (
            Criteria.custom_label2 IS NULL
            OR Criteria.custom_label2 = TRIM(LOWER(ProductView.custom_labels.label_2)))
          AND (
            Criteria.custom_label3 IS NULL
            OR Criteria.custom_label3 = TRIM(LOWER(ProductView.custom_labels.label_3)))
          AND (
            Criteria.custom_label4 IS NULL
            OR Criteria.custom_label4 = TRIM(LOWER(ProductView.custom_labels.label_4)))
          AND (
            Criteria.product_type_l1 IS NULL
            OR Criteria.product_type_l1 = TRIM(LOWER(ProductView.product_type_l1)))
          AND (
            Criteria.product_type_l2 IS NULL
            OR Criteria.product_type_l2 = TRIM(LOWER(ProductView.product_type_l2)))
          AND (
            Criteria.product_type_l3 IS NULL
            OR Criteria.product_type_l3 = TRIM(LOWER(ProductView.product_type_l3)))
          AND (
            Criteria.product_type_l4 IS NULL
            OR Criteria.product_type_l4 = TRIM(LOWER(ProductView.product_type_l4)))
          AND (
            Criteria.product_type_l5 IS NULL
            OR Criteria.product_type_l5 = TRIM(LOWER(ProductView.product_type_l5)))
          AND (
            Criteria.google_product_category_l1 IS NULL
            OR Criteria.google_product_category_l1
              = TRIM(LOWER(ProductView.google_product_category_l1)))
          AND (
            Criteria.google_product_category_l2 IS NULL
            OR Criteria.google_product_category_l2
              = TRIM(LOWER(ProductView.google_product_category_l2)))
          AND (
            Criteria.google_product_category_l3 IS NULL
            OR Criteria.google_product_category_l3
              = TRIM(LOWER(ProductView.google_product_category_l3)))
          AND (
            Criteria.google_product_category_l4 IS NULL
            OR Criteria.google_product_category_l4
              = TRIM(LOWER(ProductView.google_product_category_l4)))
          AND (
            Criteria.google_product_category_l5 IS NULL
            OR Criteria.google_product_category_l5
              = TRIM(LOWER(ProductView.google_product_category_l5)))
          AND (
            Criteria.brand IS NULL
            OR Criteria.brand = TRIM(LOWER(ProductView.brand)))
          AND (
            Criteria.channel IS NULL
            OR Criteria.channel = TRIM(LOWER(ProductView.channel)))
          AND (
            Criteria.channel_exclusivity IS NULL
            OR Criteria.channel_exclusivity = TRIM(LOWER(ProductView.channel_exclusivity)))
          AND (
            Criteria.condition IS NULL
            OR Criteria.condition = TRIM(LOWER(ProductView.condition)))
          AND TRIM(LOWER(ProductView.custom_labels.label_0)) NOT IN UNNEST(neg_custom_label0)
          AND TRIM(LOWER(ProductView.custom_labels.label_1)) NOT IN UNNEST(neg_custom_label1)
          AND TRIM(LOWER(ProductView.custom_labels.label_2)) NOT IN UNNEST(neg_custom_label2)
          AND TRIM(LOWER(ProductView.custom_labels.label_3)) NOT IN UNNEST(neg_custom_label3)
          AND TRIM(LOWER(ProductView.custom_labels.label_4)) NOT IN UNNEST(neg_custom_label4)
          AND TRIM(LOWER(ProductView.product_type_l1)) NOT IN UNNEST(neg_product_type_l1)
          AND TRIM(LOWER(ProductView.product_type_l2)) NOT IN UNNEST(neg_product_type_l2)
          AND TRIM(LOWER(ProductView.product_type_l3)) NOT IN UNNEST(neg_product_type_l3)
          AND TRIM(LOWER(ProductView.product_type_l4)) NOT IN UNNEST(neg_product_type_l4)
          AND TRIM(LOWER(ProductView.product_type_l5)) NOT IN UNNEST(neg_product_type_l5)
          AND TRIM(LOWER(ProductView.google_product_category_l1))
            NOT IN UNNEST(neg_google_product_category_l1)
          AND TRIM(LOWER(ProductView.google_product_category_l2))
            NOT IN UNNEST(neg_google_product_category_l2)
          AND TRIM(LOWER(ProductView.google_product_category_l3))
            NOT IN UNNEST(neg_google_product_category_l3)
          AND TRIM(LOWER(ProductView.google_product_category_l4))
            NOT IN UNNEST(neg_google_product_category_l4)
          AND TRIM(LOWER(ProductView.google_product_category_l5))
            NOT IN UNNEST(neg_google_product_category_l5)
          AND TRIM(LOWER(ProductView.brand)) NOT IN UNNEST(neg_brand)
          AND TRIM(LOWER(ProductView.channel)) NOT IN UNNEST(neg_channel)
          AND TRIM(LOWER(ProductView.channel_exclusivity)) NOT IN UNNEST(neg_channel_exclusivity)
          AND TRIM(LOWER(ProductView.condition)) NOT IN UNNEST(neg_condition)
      WHERE
        Criteria.offer_id IS NULL
    )
  SELECT
    _DATA_DATE,
    _LATEST_DATE,
    product_id,
    merchant_id,
    target_country
  FROM
    IdTargeted
  UNION ALL
  SELECT
    _DATA_DATE,
    _LATEST_DATE,
    product_id,
    merchant_id,
    target_country
  FROM
    NonIdTargeted
);
