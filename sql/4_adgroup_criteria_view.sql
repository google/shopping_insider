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
    # Retrieves the 'ENABLED' & 'SHOPPING' criteria.
    Criteria AS (
      SELECT
        Campaigns._DATA_DATE,
        Campaigns._LATEST_DATE,
        Campaigns.campaign_id,
        AdGroups.ad_group_id,
        # This column used for troubleshooting only, it act like a unique id for row.
        ROW_NUMBER()
          OVER (PARTITION BY AdGroups.ad_group_id, Campaigns._DATA_DATE) AS criterion_row,
        AdGroupCriteria.ad_group_criterion_negative AS is_negative,
        IF(AdGroupCriteria.ad_group_criterion_status = 'ENABLED', TRUE, FALSE)
          AS is_criterion_enabled,
        AdGroupCriteria.ad_group_criterion_display_name AS display_name,
        # Split the individual criterion
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
    # Unnest the criterion into each row.
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
    # Assigns the each criterion into the respective column.
    PivotedCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id,
        ad_group_id,
        criterion_row,
        is_negative,
        is_criterion_enabled,
        display_name AS criteria,
        # Get the parent criteria by removing the last criterion.
        # Logic: find the position of the last '&+', then keep the LEFT of it.
        LEFT(
          display_name,
          IF(INSTR(display_name, '&+', -1, 1) > 0, INSTR(display_name, '&+', -1, 1) - 1, 0))
          AS parent_criteria,
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
    # Aggregates the criteria to be used for "Everything else" by grouping the same parent. At this
    # point, we only know the sibling (same parent), not the grandparent criteria.
    ParentCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id,
        ad_group_id,
        parent_criteria,
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
        1, 2, 3, 4, 5
    ),
    # In order to know all ancestors criteria, we do the expansive join by joining all criteria that
    # matched the parent criteria.
    JoinedExclusionCriteria AS (
      SELECT
        PivotedCriteria._DATA_DATE,
        PivotedCriteria._LATEST_DATE,
        PivotedCriteria.campaign_id,
        PivotedCriteria.ad_group_id,
        PivotedCriteria.criterion_row,
        PivotedCriteria.is_negative,
        PivotedCriteria.is_criterion_enabled,
        PivotedCriteria.criteria,
        PivotedCriteria.parent_criteria,
        PivotedCriteria.custom_label0,
        PivotedCriteria.custom_label1,
        PivotedCriteria.custom_label2,
        PivotedCriteria.custom_label3,
        PivotedCriteria.custom_label4,
        PivotedCriteria.product_type_l1,
        PivotedCriteria.product_type_l2,
        PivotedCriteria.product_type_l3,
        PivotedCriteria.product_type_l4,
        PivotedCriteria.product_type_l5,
        PivotedCriteria.google_product_category_l1,
        PivotedCriteria.google_product_category_l2,
        PivotedCriteria.google_product_category_l3,
        PivotedCriteria.google_product_category_l4,
        PivotedCriteria.google_product_category_l5,
        PivotedCriteria.channel,
        PivotedCriteria.channel_exclusivity,
        PivotedCriteria.condition,
        PivotedCriteria.brand,
        PivotedCriteria.offer_id,
        # If 'criterion==*' also imply the 'everything else'. We only add the negative criteria if
        # 'criterion==*' is found.
        IF(CONTAINS_SUBSTR(criteria, 'custom0==*'), ParentCriteria.agg_custom_label0, NULL)
          AS neg_custom_label0,
        IF(CONTAINS_SUBSTR(criteria, 'custom1==*'), ParentCriteria.agg_custom_label1, NULL)
          AS neg_custom_label1,
        IF(CONTAINS_SUBSTR(criteria, 'custom2==*'), ParentCriteria.agg_custom_label2, NULL)
          AS neg_custom_label2,
        IF(CONTAINS_SUBSTR(criteria, 'custom3==*'), ParentCriteria.agg_custom_label3, NULL)
          AS neg_custom_label3,
        IF(CONTAINS_SUBSTR(criteria, 'custom4==*'), ParentCriteria.agg_custom_label4, NULL)
          AS neg_custom_label4,
        IF(
          CONTAINS_SUBSTR(criteria, 'product_type_l1==*'), ParentCriteria.agg_product_type_l1, NULL)
          AS neg_product_type_l1,
        IF(
          CONTAINS_SUBSTR(criteria, 'product_type_l2==*'), ParentCriteria.agg_product_type_l2, NULL)
          AS neg_product_type_l2,
        IF(
          CONTAINS_SUBSTR(criteria, 'product_type_l3==*'), ParentCriteria.agg_product_type_l3, NULL)
          AS neg_product_type_l3,
        IF(
          CONTAINS_SUBSTR(criteria, 'product_type_l4==*'), ParentCriteria.agg_product_type_l4, NULL)
          AS neg_product_type_l4,
        IF(
          CONTAINS_SUBSTR(criteria, 'product_type_l5==*'), ParentCriteria.agg_product_type_l5, NULL)
          AS neg_product_type_l5,
        IF(
          CONTAINS_SUBSTR(criteria, 'category_l1==*'),
          ParentCriteria.agg_google_product_category_l1,
          NULL)
          AS neg_google_product_category_l1,
        IF(
          CONTAINS_SUBSTR(criteria, 'category_l2==*'),
          ParentCriteria.agg_google_product_category_l2,
          NULL)
          AS neg_google_product_category_l2,
        IF(
          CONTAINS_SUBSTR(criteria, 'category_l3==*'),
          ParentCriteria.agg_google_product_category_l3,
          NULL)
          AS neg_google_product_category_l3,
        IF(
          CONTAINS_SUBSTR(criteria, 'category_l4==*'),
          ParentCriteria.agg_google_product_category_l4,
          NULL)
          AS neg_google_product_category_l4,
        IF(
          CONTAINS_SUBSTR(criteria, 'category_l5==*'),
          ParentCriteria.agg_google_product_category_l5,
          NULL)
          AS neg_google_product_category_l5,
        IF(CONTAINS_SUBSTR(criteria, 'channel==*'), ParentCriteria.agg_channel, NULL)
          AS neg_channel,
        IF(
          CONTAINS_SUBSTR(criteria, 'channel_exclusivity==*'),
          ParentCriteria.agg_channel_exclusivity,
          NULL)
          AS neg_channel_exclusivity,
        IF(CONTAINS_SUBSTR(criteria, 'c_condition==*'), ParentCriteria.agg_condition, NULL)
          AS neg_condition,
        IF(CONTAINS_SUBSTR(criteria, 'brand==*'), ParentCriteria.agg_brand, NULL) AS neg_brand,
        IF(CONTAINS_SUBSTR(criteria, 'id==*'), ParentCriteria.agg_offer_id, NULL) AS neg_offer_id
      FROM
        ParentCriteria
      INNER JOIN
        PivotedCriteria
        ON
          ParentCriteria.campaign_id = PivotedCriteria.campaign_id
          AND ParentCriteria.ad_group_id = PivotedCriteria.ad_group_id
          AND ParentCriteria._DATA_DATE = PivotedCriteria._DATA_DATE
          # Find all the ancestors criteria
          AND INSTR(PivotedCriteria.criteria, ParentCriteria.parent_criteria) > 0
    ),
    # Aggregates and removes the duplicates caused by the expansive join early.
    AggregatedCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id,
        ad_group_id,
        criterion_row,
        is_negative,
        is_criterion_enabled,
        criteria,
        parent_criteria,
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
        ARRAY_CONCAT_AGG(neg_custom_label0) AS neg_custom_label0,
        ARRAY_CONCAT_AGG(neg_custom_label1) AS neg_custom_label1,
        ARRAY_CONCAT_AGG(neg_custom_label2) AS neg_custom_label2,
        ARRAY_CONCAT_AGG(neg_custom_label3) AS neg_custom_label3,
        ARRAY_CONCAT_AGG(neg_custom_label4) AS neg_custom_label4,
        ARRAY_CONCAT_AGG(neg_product_type_l1) AS neg_product_type_l1,
        ARRAY_CONCAT_AGG(neg_product_type_l2) AS neg_product_type_l2,
        ARRAY_CONCAT_AGG(neg_product_type_l3) AS neg_product_type_l3,
        ARRAY_CONCAT_AGG(neg_product_type_l4) AS neg_product_type_l4,
        ARRAY_CONCAT_AGG(neg_product_type_l5) AS neg_product_type_l5,
        ARRAY_CONCAT_AGG(neg_google_product_category_l1) AS neg_google_product_category_l1,
        ARRAY_CONCAT_AGG(neg_google_product_category_l2) AS neg_google_product_category_l2,
        ARRAY_CONCAT_AGG(neg_google_product_category_l3) AS neg_google_product_category_l3,
        ARRAY_CONCAT_AGG(neg_google_product_category_l4) AS neg_google_product_category_l4,
        ARRAY_CONCAT_AGG(neg_google_product_category_l5) AS neg_google_product_category_l5,
        ARRAY_CONCAT_AGG(neg_channel) AS neg_channel,
        ARRAY_CONCAT_AGG(neg_channel_exclusivity) AS neg_channel_exclusivity,
        ARRAY_CONCAT_AGG(neg_condition) AS neg_condition,
        ARRAY_CONCAT_AGG(neg_brand) AS neg_brand,
        ARRAY_CONCAT_AGG(neg_offer_id) AS neg_offer_id
      FROM
        JoinedExclusionCriteria
      GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29
    ),
    # Removes the criteria that is not effectively serving.
    FinalCriteria AS (
      SELECT
        *
      FROM
        AggregatedCriteria
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
