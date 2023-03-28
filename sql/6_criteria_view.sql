# Copyright 2023 Google LLC..
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

# Creates a snapshot criteria view for both stardard & pmax campaigns.

CREATE OR REPLACE VIEW `{project_id}.{dataset}.criteria_view_{external_customer_id}`
AS (
  SELECT
    _DATA_DATE,
    _LATEST_DATE,
    'AdGroup' AS source,
    merchant_id,
    target_country,
    custom_label0,
    custom_label1,
    custom_label2,
    custom_label3,
    custom_label4,
    product_type_l1,
    product_type_l2,
    product_type_l3,
    product_type_l4,
    product_type_l5,
    google_product_category_l1,
    google_product_category_l2,
    google_product_category_l3,
    google_product_category_l4,
    google_product_category_l5,
    channel,
    channel_exclusivity,
    condition,
    brand,
    offer_id
  FROM
    `{project_id}.{dataset}.adgroup_criteria_view_{external_customer_id}`
  UNION ALL
  SELECT
    _DATA_DATE,
    _LATEST_DATE,
    'pMax' AS source,
    merchant_id,
    target_country,
    custom_label0,
    custom_label1,
    custom_label2,
    custom_label3,
    custom_label4,
    product_type_l1,
    product_type_l2,
    product_type_l3,
    product_type_l4,
    product_type_l5,
    google_product_category_l1,
    google_product_category_l2,
    google_product_category_l3,
    google_product_category_l4,
    google_product_category_l5,
    channel,
    NULL AS channel_exclusivity,
    condition,
    brand,
    offer_id
  FROM
    `{project_id}.{dataset}.pmax_criteria_view_{external_customer_id}`
);