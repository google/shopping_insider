# Copyright 2023 Google LLC.
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
  WITH RECURSIVE
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
          asset_group_listing_group_filter_case_value_product_custom_attribute_value,
          NULL) AS custom_label0,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX1',
          asset_group_listing_group_filter_case_value_product_custom_attribute_value,
          NULL) AS custom_label1,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX2',
          asset_group_listing_group_filter_case_value_product_custom_attribute_value,
          NULL) AS custom_label2,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX3',
          asset_group_listing_group_filter_case_value_product_custom_attribute_value,
          NULL) AS custom_label3,
        IF(
          asset_group_listing_group_filter_case_value_product_custom_attribute_index = 'INDEX4',
          asset_group_listing_group_filter_case_value_product_custom_attribute_value,
          NULL) AS custom_label4,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL1',
          asset_group_listing_group_filter_case_value_product_type_value,
          NULL) AS product_type_l1,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL2',
          asset_group_listing_group_filter_case_value_product_type_value,
          NULL) AS product_type_l2,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL3',
          asset_group_listing_group_filter_case_value_product_type_value,
          NULL) AS product_type_l3,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL4',
          asset_group_listing_group_filter_case_value_product_type_value,
          NULL) AS product_type_l4,
        IF(
          asset_group_listing_group_filter_case_value_product_type_level = 'LEVEL5',
          asset_group_listing_group_filter_case_value_product_type_value,
          NULL) AS product_type_l5,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL1',
          asset_group_listing_group_filter_case_value_product_bidding_category_id,
          NULL) AS google_product_category_l1,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL2',
          asset_group_listing_group_filter_case_value_product_bidding_category_id,
          NULL) AS google_product_category_l2,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL3',
          asset_group_listing_group_filter_case_value_product_bidding_category_id,
          NULL) AS google_product_category_l3,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL4',
          asset_group_listing_group_filter_case_value_product_bidding_category_id,
          NULL) AS google_product_category_l4,
        IF(
          asset_group_listing_group_filter_case_value_product_bidding_category_level = 'LEVEL5',
          asset_group_listing_group_filter_case_value_product_bidding_category_id,
          NULL) AS google_product_category_l5,
        IF(
          asset_group_listing_group_filter_case_value_product_channel_channel != 'UNSPECIFIED',
          asset_group_listing_group_filter_case_value_product_channel_channel,
          NULL) AS channel,
        IF(
          asset_group_listing_group_filter_case_value_product_condition_condition != 'UNSPECIFIED',
          asset_group_listing_group_filter_case_value_product_condition_condition,
          NULL) AS condition,
        asset_group_listing_group_filter_case_value_product_brand_value AS brand,
        asset_group_listing_group_filter_case_value_product_item_id_value AS offer_id
      FROM
        `{project_id}.{dataset}.ads_AssetGroupListingGroupFilter_{external_customer_id}`
    ),
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
    FilteredData AS (
      SELECT
        *
      FROM Campaigns
      INNER JOIN Merchants
        USING (_DATA_DATE, _LATEST_DATE, campaign_id)
      INNER JOIN AssetGroups
        USING (_DATA_DATE, _LATEST_DATE, campaign_id)
      INNER JOIN AssetGroupListingGroupFilters
        USING (_DATA_DATE, _LATEST_DATE, asset_group_id)
    ),
    JoinedData AS (
      # Get the leaf node of the listing group
      SELECT
        0 AS index,
        listing_group_filter_id AS leaf_node_listing_group_filter_id,
        *
      FROM FilteredData
      WHERE
        asset_group_listing_group_filter_type = 'UNIT_INCLUDED'
      UNION ALL
      # Traverse through all rules via parent_listing_group_filter_id
      SELECT
        Child.index + 1 AS index,
        Child.leaf_node_listing_group_filter_id,
        Parent.*
      FROM FilteredData AS Parent
      INNER JOIN JoinedData AS Child
        ON
          Child.parent_listing_group_filter_id = Parent.listing_group_filter_id
          AND Child._DATA_DATE = Parent._DATA_DATE
    )
  SELECT
    _DATA_DATE,
    _LATEST_DATE,
    merchant_id,
    target_country,
    asset_group_id,
    campaign_id,
    leaf_node_listing_group_filter_id,
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
  GROUP BY 1, 2, 3, 4, 5, 6, 7
);
