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

# Creates a snapshot of PMax shopping criteria view.
#
# The view parse the asset_group_listing_group_filter into multiple columns that will used to join
# with the GMC data to find the targeted products.

CREATE OR REPLACE VIEW `{project_id}.{dataset}.pmax_criteria_view_{external_customer_id}`
AS (
  # Use recursive join to find all the ancestors.
  WITH RECURSIVE
    # Get all the criteria.
    AssetGroupListingGroupFilters AS (
      SELECT DISTINCT
        _DATA_DATE,
        _LATEST_DATE,
        CAST(asset_group_listing_group_filter_id AS INT64) AS listing_group_filter_id,
        CAST(
          SPLIT(
            asset_group_listing_group_filter_parent_listing_group_filter,
            '~')[
            SAFE_OFFSET(1)]
          AS INT64) AS parent_listing_group_filter_id,
        CAST(
          SPLIT(
            asset_group_listing_group_filter_asset_group,
            '/')[
            SAFE_OFFSET(3)]
          AS INT64) AS asset_group_id,
        asset_group_listing_group_filter_type,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX0',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_custom_attribute_value)),
          NULL) AS custom_label0,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX1',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_custom_attribute_value)),
          NULL) AS custom_label1,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX2',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_custom_attribute_value)),
          NULL) AS custom_label2,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX3',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_custom_attribute_value)),
          NULL) AS custom_label3,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX4',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_custom_attribute_value)),
          NULL) AS custom_label4,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL1',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_type_value)),
          NULL) AS product_type_l1,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL2',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_type_value)),
          NULL) AS product_type_l2,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL3',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_type_value)),
          NULL) AS product_type_l3,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL4',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_type_value)),
          NULL) AS product_type_l4,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL5',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_type_value)),
          NULL) AS product_type_l5,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL1',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_bidding_category_id)),
          NULL) AS google_product_category_l1,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL2',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_bidding_category_id)),
          NULL) AS google_product_category_l2,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL3',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_bidding_category_id)),
          NULL) AS google_product_category_l3,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL4',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_bidding_category_id)),
          NULL) AS google_product_category_l4,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL5',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_bidding_category_id)),
          NULL) AS google_product_category_l5,
        IF(
          asset_group_listing_group_filter_case_value_product_channel_channel != 'UNSPECIFIED',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_channel_channel)),
          NULL) AS channel,
        IF(
          asset_group_listing_group_filter_case_value_product_condition_condition != 'UNSPECIFIED',
          TRIM(LOWER(asset_group_listing_group_filter_case_value_product_condition_condition)),
          NULL) AS condition,
        TRIM(LOWER(asset_group_listing_group_filter_case_value_product_brand_value)) AS brand,
        TRIM(LOWER(asset_group_listing_group_filter_case_value_product_item_id_value)) AS offer_id
      FROM
        `{project_id}.{dataset}.ads_AssetGroupListingGroupFilter_{external_customer_id}`
    ),
    # Aggregates the criteria to be used for "Everything else" by grouping the same parent. At this
    # point, we only know the sibling (same parent), not the grandparent criteria.
    AggregatedCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        asset_group_id,
        parent_listing_group_filter_id,
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
        ARRAY_AGG(DISTINCT condition IGNORE NULLS) AS agg_condition,
        ARRAY_AGG(DISTINCT brand IGNORE NULLS) AS agg_brand,
        ARRAY_AGG(DISTINCT offer_id IGNORE NULLS) AS agg_offer_id
      FROM
        AssetGroupListingGroupFilters
      GROUP BY
        1, 2, 3, 4
    ),
    # Find the active asset group
    AssetGroups AS (
      SELECT DISTINCT
        _DATA_DATE,
        _LATEST_DATE,
        CAST(asset_group_id AS INT64) AS asset_group_id,
        CAST(SPLIT(asset_group_campaign, '/')[OFFSET(3)] AS INT64) AS campaign_id
      FROM
        `{project_id}.{dataset}.ads_AssetGroup_{external_customer_id}`
      WHERE
        asset_group_status = 'ENABLED'
    ),
    # Find the active campaign.
    Campaigns AS (
      SELECT DISTINCT
        _DATA_DATE,
        _LATEST_DATE,
        campaign_id
      FROM
        `{project_id}.{dataset}.ads_Campaign_{external_customer_id}`
      WHERE
        campaign_status = 'ENABLED'
    ),
    # Get merchant id.
    Merchants AS (
      SELECT DISTINCT
        _DATA_DATE,
        _LATEST_DATE,
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
    ),
    # Get the active criteria only.
    FilteredData AS (
      SELECT
        *
      FROM Campaigns
      INNER JOIN AssetGroups
        USING (_DATA_DATE, _LATEST_DATE, campaign_id)
      INNER JOIN AssetGroupListingGroupFilters
        USING (_DATA_DATE, _LATEST_DATE, asset_group_id)
    ),
    # Join recursively to traverse from the leaf node(the last criterion) to the root(all products).
    JoinedData AS (
      # Get the leaf node of the listing group
      SELECT
        # This used for troubleshooting purpose only. It is unique id for each row.
        0 AS index,
        # It is common id for each branch(route).
        listing_group_filter_id AS leaf_node_listing_group_filter_id,
        *
      FROM FilteredData
      WHERE
        # This will get the leaf node only. The middle layer is 'SUBDIVISION'.
        asset_group_listing_group_filter_type = 'UNIT_INCLUDED'
      UNION ALL
      # Traverse through all rules via parent_listing_group_filter_id
      SELECT
        # Increases the number by 1, so each row is unique.
        Child.index + 1 AS index,
        # Retains the leaf node id, so each route is unique(traceable).
        Child.leaf_node_listing_group_filter_id,
        Parent.*
      FROM FilteredData AS Parent
      INNER JOIN JoinedData AS Child
        ON
          Child.parent_listing_group_filter_id = Parent.listing_group_filter_id
          AND Child._DATA_DATE = Parent._DATA_DATE
    ),
    # Aggregates the inclusive criteria.
    InclusiveCriteria AS (
      SELECT
        _DATA_DATE,
        _LATEST_DATE,
        asset_group_id,
        campaign_id,
        # Each branch is a criteria, aggregates to get all effective criterion.
        leaf_node_listing_group_filter_id,
        # Aggregates the parent ids to find the family tree
        ARRAY_AGG(IFNULL(parent_listing_group_filter_id, 0)) AS parent_listing_group_filter_ids,
        MAX(custom_label0) AS custom_label0,
        MAX(custom_label1) AS custom_label1,
        MAX(custom_label2) AS custom_label2,
        MAX(custom_label3) AS custom_label3,
        MAX(custom_label4) AS custom_label4,
        MAX(product_type_l1) AS product_type_l1,
        MAX(product_type_l2) AS product_type_l2,
        MAX(product_type_l3) AS product_type_l3,
        MAX(product_type_l4) AS product_type_l4,
        MAX(product_type_l5) AS product_type_l5,
        MAX(google_product_category_l1) AS google_product_category_l1,
        MAX(google_product_category_l2) AS google_product_category_l2,
        MAX(google_product_category_l3) AS google_product_category_l3,
        MAX(google_product_category_l4) AS google_product_category_l4,
        MAX(google_product_category_l5) AS google_product_category_l5,
        MAX(channel) AS channel,
        MAX(condition) AS condition,
        MAX(brand) AS brand,
        MAX(offer_id) AS offer_id
      FROM
        JoinedData
      GROUP BY 1, 2, 3, 4, 5
    ),
    # Lastly, add the 'Everything else' array to the table by matching all the parent(SUBDIVISION).
    Criteria AS (
      SELECT
        InclusiveCriteria._DATA_DATE,
        InclusiveCriteria._LATEST_DATE,
        InclusiveCriteria.asset_group_id,
        InclusiveCriteria.campaign_id,
        InclusiveCriteria.leaf_node_listing_group_filter_id,
        InclusiveCriteria.custom_label0,
        InclusiveCriteria.custom_label1,
        InclusiveCriteria.custom_label2,
        InclusiveCriteria.custom_label3,
        InclusiveCriteria.custom_label4,
        InclusiveCriteria.product_type_l1,
        InclusiveCriteria.product_type_l2,
        InclusiveCriteria.product_type_l3,
        InclusiveCriteria.product_type_l4,
        InclusiveCriteria.product_type_l5,
        InclusiveCriteria.google_product_category_l1,
        InclusiveCriteria.google_product_category_l2,
        InclusiveCriteria.google_product_category_l3,
        InclusiveCriteria.google_product_category_l4,
        InclusiveCriteria.google_product_category_l5,
        InclusiveCriteria.channel,
        InclusiveCriteria.condition,
        InclusiveCriteria.brand,
        InclusiveCriteria.offer_id,
        ANY_VALUE(IF(custom_label0 IS NULL, AggregatedCriteria.agg_custom_label0, []))
          AS neg_custom_label0,
        ANY_VALUE(IF(custom_label1 IS NULL, AggregatedCriteria.agg_custom_label1, []))
          AS neg_custom_label1,
        ANY_VALUE(IF(custom_label2 IS NULL, AggregatedCriteria.agg_custom_label2, []))
          AS neg_custom_label2,
        ANY_VALUE(IF(custom_label3 IS NULL, AggregatedCriteria.agg_custom_label3, []))
          AS neg_custom_label3,
        ANY_VALUE(IF(custom_label4 IS NULL, AggregatedCriteria.agg_custom_label4, []))
          AS neg_custom_label4,
        ANY_VALUE(IF(product_type_l1 IS NULL, AggregatedCriteria.agg_product_type_l1, []))
          AS neg_product_type_l1,
        ANY_VALUE(IF(product_type_l2 IS NULL, AggregatedCriteria.agg_product_type_l2, []))
          AS neg_product_type_l2,
        ANY_VALUE(IF(product_type_l3 IS NULL, AggregatedCriteria.agg_product_type_l3, []))
          AS neg_product_type_l3,
        ANY_VALUE(IF(product_type_l4 IS NULL, AggregatedCriteria.agg_product_type_l4, []))
          AS neg_product_type_l4,
        ANY_VALUE(IF(product_type_l5 IS NULL, AggregatedCriteria.agg_product_type_l5, []))
          AS neg_product_type_l5,
        ANY_VALUE(
          IF(
            google_product_category_l1 IS NULL,
            AggregatedCriteria.agg_google_product_category_l1, []))
          AS neg_google_product_category_l1,
        ANY_VALUE(
          IF(
            google_product_category_l2 IS NULL,
            AggregatedCriteria.agg_google_product_category_l2, []))
          AS neg_google_product_category_l2,
        ANY_VALUE(
          IF(
            google_product_category_l3 IS NULL,
            AggregatedCriteria.agg_google_product_category_l3, []))
          AS neg_google_product_category_l3,
        ANY_VALUE(
          IF(
            google_product_category_l4 IS NULL,
            AggregatedCriteria.agg_google_product_category_l4, []))
          AS neg_google_product_category_l4,
        ANY_VALUE(
          IF(
            google_product_category_l5 IS NULL,
            AggregatedCriteria.agg_google_product_category_l5, []))
          AS neg_google_product_category_l5,
        ANY_VALUE(IF(channel IS NULL, AggregatedCriteria.agg_channel, [])) AS neg_channel,
        ANY_VALUE(IF(condition IS NULL, AggregatedCriteria.agg_condition, [])) AS neg_condition,
        ANY_VALUE(IF(brand IS NULL, AggregatedCriteria.agg_brand, [])) AS neg_brand,
        ANY_VALUE(IF(offer_id IS NULL, AggregatedCriteria.agg_offer_id, [])) AS neg_offer_id
      FROM
        InclusiveCriteria
      LEFT JOIN
        AggregatedCriteria
        ON
          AggregatedCriteria.asset_group_id = InclusiveCriteria.asset_group_id
          AND AggregatedCriteria._DATA_DATE = InclusiveCriteria._DATA_DATE
          AND AggregatedCriteria.parent_listing_group_filter_id
            IN UNNEST(InclusiveCriteria.parent_listing_group_filter_ids)
      GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
    )
  SELECT
    *
  FROM Criteria
  INNER JOIN Merchants
    USING (_DATA_DATE, _LATEST_DATE, campaign_id)
);
