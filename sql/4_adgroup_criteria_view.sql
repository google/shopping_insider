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
        IF(AdGroupCriteria.ad_group_criterion_status = 'ENABLED', TRUE, FALSE)
          AS is_criterion_enabled,
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
    ),
    FlattenCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id,
        ad_group_id,
        criterion_row,
        is_negative,
        is_criterion_enabled,
        display_name,
        sub_criterion,
        SPLIT(sub_criterion, '==')[SAFE_OFFSET(0)] AS sub_criterion_type,
        TRIM(LOWER(SPLIT(sub_criterion, '==')[SAFE_OFFSET(1)])) AS sub_criterion_value
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
        is_criterion_enabled,
        display_name,
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
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ),
    -- This aggregate the criteria to be used for exclusion logic
    AggregatedCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id,
        ad_group_id,
        ARRAY_AGG(DISTINCT custom_label0 IGNORE NULLS) AS agg_custom_label0,
        ARRAY_AGG(DISTINCT custom_label1 IGNORE NULLS) AS agg_custom_label1,
        ARRAY_AGG(DISTINCT custom_label2 IGNORE NULLS) AS agg_custom_label2,
        ARRAY_AGG(DISTINCT custom_label3 IGNORE NULLS) AS agg_custom_label3,
        ARRAY_AGG(DISTINCT custom_label4 IGNORE NULLS) AS agg_custom_label4,
        ARRAY_AGG(DISTINCT product_type_l1 IGNORE NULLS) AS agg_product_type_l1,
        ARRAY_AGG(DISTINCT product_type_l2 IGNORE NULLS) AS agg_product_type_l2,
        ARRAY_AGG(DISTINCT product_type_l3 IGNORE NULLS) AS agg_product_type_l3,
        ARRAY_AGG(DISTINCT product_type_l4 IGNORE NULLS) AS agg_product_type_l4,
        ARRAY_AGG(DISTINCT product_type_l5 IGNORE NULLS) AS agg_product_type_l5,
        ARRAY_AGG(DISTINCT google_product_category_l1 IGNORE NULLS)
          AS agg_google_product_category_l1,
        ARRAY_AGG(DISTINCT google_product_category_l2 IGNORE NULLS)
          AS agg_google_product_category_l2,
        ARRAY_AGG(DISTINCT google_product_category_l3 IGNORE NULLS)
          AS agg_google_product_category_l3,
        ARRAY_AGG(DISTINCT google_product_category_l4 IGNORE NULLS)
          AS agg_google_product_category_l4,
        ARRAY_AGG(DISTINCT google_product_category_l5 IGNORE NULLS)
          AS agg_google_product_category_l5,
        ARRAY_AGG(DISTINCT channel IGNORE NULLS) AS agg_channel,
        ARRAY_AGG(DISTINCT channel_exclusivity IGNORE NULLS) AS agg_channel_exclusivity,
        ARRAY_AGG(DISTINCT condition IGNORE NULLS) AS agg_condition,
        ARRAY_AGG(DISTINCT brand IGNORE NULLS) AS agg_brand,
        ARRAY_AGG(DISTINCT offer_id IGNORE NULLS) AS agg_offer_id
      FROM
        PivotedCriteria
      GROUP BY
        1, 2, 3, 4
    ),
    FinalCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id,
        ad_group_id,
        criterion_row,
        is_negative,
        is_criterion_enabled,
        display_name,
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
        offer_id,
        IF(CONTAINS_SUBSTR(display_name, 'custom0==*'), agg_custom_label0, NULL)
          AS neg_custom_label0,
        IF(CONTAINS_SUBSTR(display_name, 'custom1==*'), agg_custom_label1, NULL)
          AS neg_custom_label1,
        IF(CONTAINS_SUBSTR(display_name, 'custom2==*'), agg_custom_label2, NULL)
          AS neg_custom_label2,
        IF(CONTAINS_SUBSTR(display_name, 'custom3==*'), agg_custom_label3, NULL)
          AS neg_custom_label3,
        IF(CONTAINS_SUBSTR(display_name, 'custom4==*'), agg_custom_label4, NULL)
          AS neg_custom_label4,
        IF(CONTAINS_SUBSTR(display_name, 'product_type_l1==*'), agg_product_type_l1, NULL)
          AS neg_product_type_l1,
        IF(CONTAINS_SUBSTR(display_name, 'product_type_l2==*'), agg_product_type_l2, NULL)
          AS neg_product_type_l2,
        IF(CONTAINS_SUBSTR(display_name, 'product_type_l3==*'), agg_product_type_l3, NULL)
          AS neg_product_type_l3,
        IF(CONTAINS_SUBSTR(display_name, 'product_type_l4==*'), agg_product_type_l4, NULL)
          AS neg_product_type_l4,
        IF(CONTAINS_SUBSTR(display_name, 'product_type_l5==*'), agg_product_type_l5, NULL)
          AS neg_product_type_l5,
        IF(CONTAINS_SUBSTR(display_name, 'category_l1==*'), agg_google_product_category_l1, NULL)
          AS neg_google_product_category_l1,
        IF(CONTAINS_SUBSTR(display_name, 'category_l2==*'), agg_google_product_category_l2, NULL)
          AS neg_google_product_category_l2,
        IF(CONTAINS_SUBSTR(display_name, 'category_l3==*'), agg_google_product_category_l3, NULL)
          AS neg_google_product_category_l3,
        IF(CONTAINS_SUBSTR(display_name, 'category_l4==*'), agg_google_product_category_l4, NULL)
          AS neg_google_product_category_l4,
        IF(CONTAINS_SUBSTR(display_name, 'category_l5==*'), agg_google_product_category_l5, NULL)
          AS neg_google_product_category_l5,
        IF(CONTAINS_SUBSTR(display_name, 'channel==*'), agg_channel, NULL) AS neg_channel,
        IF(CONTAINS_SUBSTR(display_name, 'channel_exclusivity==*'), agg_channel_exclusivity, NULL)
          AS neg_channel_exclusivity,
        IF(CONTAINS_SUBSTR(display_name, 'c_condition==*'), agg_condition, NULL) AS neg_condition,
        IF(CONTAINS_SUBSTR(display_name, 'brand==*'), agg_brand, NULL) AS neg_brand,
        IF(CONTAINS_SUBSTR(display_name, 'id==*'), agg_offer_id, NULL) AS neg_offer_id
      FROM
        AggregatedCriteria
      INNER JOIN
        PivotedCriteria
        USING (campaign_id, ad_group_id, _DATA_DATE, _LATEST_DATE)
      WHERE
        is_negative = FALSE
        AND is_criterion_enabled = TRUE
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
  FROM FinalCriteria
  INNER JOIN Merchants
    USING (campaign_id)
);
