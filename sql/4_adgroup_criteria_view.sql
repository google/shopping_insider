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

# Creates a snapshot of standard shopping criteria view.
#
# The view parse the adgroup criteria into multiple columns that will used to join with the GMC data
# to find the targeted products.

CREATE OR REPLACE VIEW `{project_id}.{dataset}.adgroup_criteria_view_{external_customer_id}`
AS (
  WITH
    Criteria AS (
      SELECT
        Campaigns._DATA_DATE,
        Campaigns._LATEST_DATE,
        Campaigns.campaign_id,
        AdGroups.ad_group_id,
        ROW_NUMBER()
          OVER (PARTITION BY AdGroups.ad_group_id, Campaigns._DATA_DATE) AS criterion_row,
        AdGroupCriteria.ad_group_criterion_negative AS is_negative,
        AdGroupCriteria.ad_group_criterion_display_name AS display_name,
        SPLIT(AdGroupCriteria.ad_group_criterion_display_name, '&+') AS sub_criteria,
      FROM
        `{project_id}.{dataset}.ads_Campaign_{external_customer_id}` AS Campaigns
      INNER JOIN
        `{project_id}.{dataset}.ads_AdGroup_{external_customer_id}` AS AdGroups
        USING (campaign_id, _DATA_DATE, _LATEST_DATE)
      INNER JOIN
        `{project_id}.{dataset}.ads_AdGroupCriterion_{external_customer_id}` AS AdGroupCriteria
        USING (ad_group_id, _DATA_DATE, _LATEST_DATE)
      WHERE
        Campaigns.campaign_status = 'ENABLED'
        AND AdGroups.ad_group_status = 'ENABLED'
        AND AdGroups.ad_group_type IN ('SHOPPING_PRODUCT_ADS', 'SHOPPING_SMART_ADS')
        AND AdGroupCriteria.ad_group_criterion_status = 'ENABLED'
        AND AdGroupCriteria.ad_group_criterion_negative = FALSE
    ),
    FlattenCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id,
        ad_group_id,
        criterion_row,
        is_negative,
        display_name,
        sub_criterion,
        SPLIT(sub_criterion, '==')[SAFE_OFFSET(0)] AS sub_criterion_type,
        SPLIT(sub_criterion, '==')[SAFE_OFFSET(1)] AS sub_criterion_value
      FROM
        Criteria, UNNEST(sub_criteria) AS sub_criterion
    ),
    PivotedCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id,
        ad_group_id,
        criterion_row,
        is_negative,
        display_name,
        sub_criterion,
        sub_criterion_type,
        sub_criterion_value,
        MAX(
          IF(
            sub_criterion_type = 'custom0' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS custom_label0,
        MAX(
          IF(
            sub_criterion_type = 'custom1' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS custom_label1,
        MAX(
          IF(
            sub_criterion_type = 'custom2' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS custom_label2,
        MAX(
          IF(
            sub_criterion_type = 'custom3' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS custom_label3,
        MAX(
          IF(
            sub_criterion_type = 'custom4' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS custom_label4,
        MAX(
          IF(
            sub_criterion_type = 'product_type_l1' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS product_type_l1,
        MAX(
          IF(
            sub_criterion_type = 'product_type_l2' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS product_type_l2,
        MAX(
          IF(
            sub_criterion_type = 'product_type_l3' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS product_type_l3,
        MAX(
          IF(
            sub_criterion_type = 'product_type_l4' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS product_type_l4,
        MAX(
          IF(
            sub_criterion_type = 'product_type_l5' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS product_type_l5,
        MAX(
          IF(
            sub_criterion_type = 'category_l1' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS google_product_category_l1,
        MAX(
          IF(
            sub_criterion_type = 'category_l2' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS google_product_category_l2,
        MAX(
          IF(
            sub_criterion_type = 'category_l3' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS google_product_category_l3,
        MAX(
          IF(
            sub_criterion_type = 'category_l4' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS google_product_category_l4,
        MAX(
          IF(
            sub_criterion_type = 'category_l5' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS google_product_category_l5,
        MAX(
          IF(
            sub_criterion_type = 'channel' AND sub_criterion_value <> '*',
            SPLIT(sub_criterion_value, ':')[SAFE_OFFSET(1)],
            NULL)) AS channel,
        MAX(
          IF(
            sub_criterion_type = 'channel_exclusivity' AND sub_criterion_value <> '*',
            SPLIT(sub_criterion_value, ':')[SAFE_OFFSET(1)],
            NULL)) AS channel_exclusivity,
        MAX(
          IF(
            sub_criterion_type = 'c_condition' AND sub_criterion_value <> '*',
            SPLIT(sub_criterion_value, ':')[SAFE_OFFSET(1)],
            NULL)) AS condition,
        MAX(
          IF(
            sub_criterion_type = 'brand' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS brand,
        MAX(
          IF(
            sub_criterion_type = 'id' AND sub_criterion_value <> '*',
            sub_criterion_value,
            NULL)) AS offer_id
      FROM
        FlattenCriteria
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    Merchants AS (
      SELECT DISTINCT
        ShoppingProductStats.campaign_id,
        ShoppingProductStats.segments_product_merchant_id AS merchant_id,
        GeoTargets.country_code AS target_country
      FROM
        `{project_id}.{dataset}.ads_ShoppingProductStats_{external_customer_id}`
          AS ShoppingProductStats
      INNER JOIN
        `{project_id}.{dataset}.geo_targets` AS GeoTargets
        ON
          CAST(
            SPLIT(
              ShoppingProductStats.segments_product_country,
              '/')[
              SAFE_OFFSET(1)]
            AS INT64)
          = GeoTargets.parent_id
    )
  SELECT
    *
  FROM PivotedCriteria
  INNER JOIN Merchants
    USING (campaign_id)
);
