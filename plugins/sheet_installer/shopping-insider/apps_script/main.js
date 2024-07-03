/**
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** @fileoverview The constants and functions for Shopping Insider. */

/** @type {string} Http url base for SQL files. */
const SOURCE_REPO = 'https://raw.githubusercontent.com/google/shopping_insider/main';

/** Definitnion of the Looker dashboard. */
/** @type {string} Looker dashboard Id. */
const LOOKER_ID = 'f1859d41-b693-470c-a404-05c585f51f20';
/** @type {!Array<Object>} Data sources used in Looker dashboard. */
const LOOKER_DATA_SOURCES = [
  {
    connector: 'bigQuery',
    type: 'TABLE',
    projectId: '${projectId}',
    datasetId: '${dataset}',
    keepDatasourceName: 'true',
    aliases: {
      product_detailed: { tableId: 'product_detailed_materialized' },
      product_historical: { tableId: 'product_historical_materialized' },
    },
  },
];

/** Tables will be transferred through Google Ads Data Transfer. */
/** @type {!Array<string>} */
const GOOGLE_ADS_TABLES = [
  'Customer',
  'Campaign',
  'AdGroup',
  'AdGroupCriterion',
  'AssetGroup',
  'AssetGroupListingGroupFilter',
  'ShoppingProductStats',
];

/**
 * The list of supported GMC DT BigQuery regions.
 * @see https://cloud.google.com/bigquery/docs/locations
 */
const GMC_BQ_DT_LOCATIONS = [
  { displayName: 'United States', locationId: 'US' },
  { displayName: 'European Union', locationId: 'EU' },
  { displayName: 'Tokyo', locationId: 'asia-northeast1' },
  { displayName: 'Singapore', locationId: 'asia-southeast1' },
  { displayName: 'Sydney', locationId: 'australia-southeast1' },
  { displayName: 'Finland', locationId: 'europe-north1' },
  { displayName: 'London', locationId: 'europe-west2' },
  { displayName: 'Zürich', locationId: 'europe-west6' },
  { displayName: 'Northern Virginia', locationId: 'us-east4' },
];

/**
 * Creates or updates a data transfer configuration.
 * @param {string} name Data transfer configuration name.
 * @param {Object} resource Object contains other optional information, e.g.
 *   authorizationCode.
 * @return {!CheckResult}
 */
const createOrUpdateDataTransfer = (name, resource) => {
  const datasetId = getDocumentProperty('dataset');
  const authorizationCode = resource.attributeValue;
  const config = {
    displayName: name,
    destinationDatasetId: datasetId,
  }
  const getFilterFn = (idName) => {
    return (transferConfig) => {
      const { dataSourceId, destinationDatasetId, params } = transferConfig;
      return dataSourceId === config.dataSourceId
        && destinationDatasetId === config.destinationDatasetId
        && params[idName] === config.params[idName];
    };
  };
  let filterFn;
  if (name.startsWith('Merchant Center Transfer')) {
    const merchantId = getDocumentProperty('merchantId');
    const enableMarketInsight = false;//getDocumentProperty('marketInsight') !== 'FALSE';
    config.dataSourceId = DATA_TRANSFER_SOURCE.GOOGLE_MERCHANT_CENTER;
    config.params = {
      merchant_id: merchantId,
      export_products: true,
      export_regional_inventories: false,
      export_local_inventories: false,
      export_price_benchmarks: enableMarketInsight,
      export_best_sellers: enableMarketInsight,
    };
    filterFn = getFilterFn('merchant_id');
  } else if (name.startsWith('Google Ads Transfer')) {
    const customerId = getDocumentProperty('externalCustomerId');
    config.dataSourceId = DATA_TRANSFER_SOURCE.GOOGLE_ADS;
    config.dataRefreshWindowDays = 1;
    config.params = {
      table_filter: GOOGLE_ADS_TABLES.join(','),
      customer_id: customerId,
      include_pmax: true,
    };
    filterFn = getFilterFn('customer_id');
  } else {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Unknown Data Transfer type: ${name}`,
    };
  }
  return gcloud.createOrUpdateDataTransfer(
    config, datasetId, filterFn, authorizationCode);
}

/**
 * Creates or updates a scheduled query.
 * @param {string} name Scheduled query configuration name.
 * @param {Object} resource Object contains other optional information, e.g.
 *   authorizationCode.
 * @return {!CheckResult}
 */
const createOrUpdateScheduledQuery = (name, resource) => {
  const datasetId = getDocumentProperty('dataset');
  const [displayName, sql] = name.split('\n');
  const authorizationCode = resource.attributeValue;
  const query = getExecutableSql(`${SOURCE_REPO}/sql/${sql}`,
    PropertiesService.getDocumentProperties().getProperties(),
    replacePythonStyleParameters
  );
  return gcloud.createOrUpdateScheduledQuery(
    displayName, datasetId, query, authorizationCode);
}

/**
 * Loads a CSV file to a BigQuery table.
 * @param {string} tableName BigQuery table name.
 * @param {Object} resource Object contains the CSV file information.
 * @return {!CheckResult}
 */
const loadCsvToBigQuery = (tableName, resource) => {
  const url = `${SOURCE_REPO}/data/${resource.attributeValue}`;
  const response = UrlFetchApp.fetch(url);
  const status = response.getResponseCode();
  if (status >= 400) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Failed to get resource, HTTP status code: ${status}`,
    };
  }
  const data = response.getContentText();
  const datasetId = getDocumentProperty('dataset');
  return gcloud.loadDataToBigQuery(tableName, data, datasetId);
}

/**
 * Run a sql file to create BigQuery views.
 * @param {string} sql Sql file name.
 * @param {Object} resource Object contains other optional information, e.g.
 *   tables should exist before this query.
 * @return {!CheckResult}
 */
const createBigQueryViews = (sql, resource) => {
  const datasetId = getDocumentProperty('dataset');
  const url = `${SOURCE_REPO}/sql/${sql}`;
  return gcloud.createBigQueryViews(
    url, resource, datasetId, replacePythonStyleParameters);
}

/**
 * Checks whether the expected BigQuery tables/views exist.
 * @param {string} _ Not usesd here. This function inhabits the arguement
 *   structure from the Cyborg framework.
 * @param {Object} resource Object contains other optional information, e.g.
 *   tables should exist before this query.
 * @return {!CheckResult}
 */
const checkExpectedTables = (_, resource) => {
  const datasetId = getDocumentProperty('dataset');
  const result = gcloud.checkExpectedTables(resource.attributeValue, datasetId);
  if (result.status !== RESOURCE_STATUS.OK) {
    return Object.assign({ value: 'Available after installation' }, result);
  }
  const dashboardLink = getLookerCreateLink(LOOKER_ID, LOOKER_DATA_SOURCES);
  return Object.assign({
    value: 'Click here to make a copy of the dashboard',
    value_link: dashboardLink,
  }, result);
}

/**
 * Register two Mojo templates for 'BigQuery Data Table' and 'BigQuery Views'
 * so they can be reused in the solution definition.
 * 'MOJO_CONFIG_TEMPLATE' is part of the framework Cyborg.
 */
MOJO_CONFIG_TEMPLATE.bigQueryDataTable = {
  category: 'Solution',
  resource: 'BigQuery Data Table',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  attributeName: 'Source',
  checkFn: loadCsvToBigQuery,
};
MOJO_CONFIG_TEMPLATE.bigQueryView = {
  category: 'Solution',
  resource: 'BigQuery Views',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  attributeName: 'Expected table(s)',
  checkFn: createBigQueryViews,
};

/** Solution configurations for Shopping Insider. */
const SHOPPING_INSIDER_MOJO_CONFIG = {
  sheetName: 'Shopping Insider',
  config: [
    { template: 'namespace', value: 'insider' },
    {
      template: 'parameter',
      category: 'General',
      resource: 'GMC Account Id',
      propertyName: 'merchantId',
      checkFn: cleanAccountNumber,
    },
    // {
    //   category: 'General',
    //   resource: 'Market Insights',
    //   value: 'Enable',
    //   propertyName: 'marketInsight',
    //   propertyTarget: 'enable',
    //   optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
    //   group: 'marketInsights',
    // },
    {
      template: 'parameter',
      category: 'General',
      resource: 'Google Ads MCC',
      propertyName: 'externalCustomerId',
      checkFn: cleanAccountNumber,
    },
    { template: 'projectId' },
    {
      template: 'permissions',
      value: [
        'bigquery.datasets.create',
        'serviceusage.services.enable',
      ],
    },
    {
      category: 'Google Cloud',
      resource: 'APIs',
      value: [
        'BigQuery Data Transfer API',
      ],
      editType: RESOURCE_EDIT_TYPE.READONLY,
      checkFn: gcloud.checkOrEnableApi,
    },
    {
      template: 'bigQueryDataset',
      value: '${namespace}_dataset',
      attributes: [
        {
          attributeName: ATTRIBUTE_NAMES.bigquery.location,
          attributeValue_datarange: GMC_BQ_DT_LOCATIONS.map(getLocationListName),
        },
        {
          attributeName: ATTRIBUTE_NAMES.bigquery.partitionExpiration,
          attributeValue: 60
        }
      ],
      propertyName: 'dataset',
    },
    {
      category: 'Solution',
      resource: 'Data Transfer',
      value: [
        'Merchant Center Transfer - ${merchantId}',
        'Google Ads Transfer - ${externalCustomerId}',
      ],
      editType: RESOURCE_EDIT_TYPE.READONLY,
      attributeName: 'Authorization Code',
      checkFn: createOrUpdateDataTransfer,
    },
    {
      template: 'bigQueryDataTable',
      value: 'language_codes',
      attributeValue: 'language_codes.csv',
      attributeValue_link: `${SOURCE_REPO}/data/language_codes.csv`,
    },
    {
      template: 'bigQueryDataTable',
      value: 'geo_targets',
      attributeValue: 'geo_targets.csv',
      attributeValue_link: `${SOURCE_REPO}/data/geo_targets.csv`,
    },
    {
      template: 'bigQueryView',
      value: '1_product_view.sql',
      value_link: `${SOURCE_REPO}/sql/1_product_view.sql`,
      attributeValue: 'Products_${merchantId}',
    },
    {
      template: 'bigQueryView',
      value: '2_product_metrics_view.sql',
      value_link: `${SOURCE_REPO}/sql/2_product_metrics_view.sql`,
      attributeValue:
        'geo_targets, language_codes, ads_ShoppingProductStats_${externalCustomerId}',
    },
    {
      template: 'bigQueryView',
      value: '3_customer_view.sql',
      value_link: `${SOURCE_REPO}/sql/3_customer_view.sql`,
      attributeValue: 'ads_Customer_${externalCustomerId}',
    },
    {
      template: 'bigQueryView',
      value: '4_adgroup_criteria_view.sql',
      value_link: `${SOURCE_REPO}/sql/4_adgroup_criteria_view.sql`,
      attributeValue:
        'ads_Campaign_${externalCustomerId}, ads_AdGroup_${externalCustomerId}, ads_AdGroupCriterion_${externalCustomerId}',
    },
    {
      template: 'bigQueryView',
      value: '5_pmax_criteria_view.sql',
      value_link: `${SOURCE_REPO}/sql/5_pmax_criteria_view.sql`,
      attributeValue:
        'ads_AssetGroup_${externalCustomerId}, ads_AssetGroupListingGroupFilter_${externalCustomerId}',
    },
    {
      template: 'bigQueryView',
      value: '6_criteria_view.sql',
      value_link: `${SOURCE_REPO}/sql/6_criteria_view.sql`,
      attributeValue:
        'adgroup_criteria_view_${externalCustomerId}, pmax_criteria_view_${externalCustomerId}',
    },
    {
      template: 'bigQueryView',
      value: '7_targeted_products_view.sql',
      value_link: `${SOURCE_REPO}/sql/7_targeted_products_view.sql`,
      attributeValue: 'product_view_${merchantId}',
    },
    {
      template: 'bigQueryView',
      value: '8_product_detailed_view.sql',
      value_link: `${SOURCE_REPO}/sql/8_product_detailed_view.sql`,
      attributeValue: 'product_metrics_view_${externalCustomerId}',
    },
    {
      template: 'bigQueryView',
      value: '9_materialize_product_detailed.sql',
      value_link: `${SOURCE_REPO}/sql/9_materialize_product_detailed.sql`,
      attributeValue: 'targeted_products_view_${externalCustomerId}, product_detailed_view_${externalCustomerId}',
    },
    {
      template: 'bigQueryView',
      value: '10_materialize_product_historical.sql',
      value_link: `${SOURCE_REPO}/sql/10_materialize_product_historical.sql`,
    },
    // {
    //   template: 'bigQueryView',
    //   value: 'market_insights/snapshot_view.sql',
    //   attributeValue: 'product_detailed_materialized, Products_PriceBenchmarks_${merchantId}, BestSellers_TopProducts_Inventory_${merchantId}',
    //   group: 'marketInsights',
    // },
    // {
    //   template: 'bigQueryView',
    //   value: 'market_insights/historical_view.sql',
    //   attributeValue: 'Products_${merchantId}, Products_PriceBenchmarks_${merchantId}',
    //   group: 'marketInsights',
    // },
    {
      category: 'Solution',
      resource: 'Scheduled Query',
      value: 'Main workflow - ${dataset} - ${externalCustomerId}\nmain_workflow.sql',
      editType: RESOURCE_EDIT_TYPE.READONLY,
      attributeName: 'Authorization Code',
      checkFn: createOrUpdateScheduledQuery,
    },
    // {
    //   category: 'Solution',
    //   resource: 'Scheduled Query',
    //   value: 'Best sellers workflow - ${dataset} - ${merchantId}\nmarket_insights/best_sellers_workflow.sql',
    //   editType: RESOURCE_EDIT_TYPE.READONLY,
    //   attributeName: 'Authorization Code',
    //   checkFn: createOrUpdateScheduledQuery,
    //   group: 'marketInsights',
    // },
    {
      category: 'Solution',
      resource: 'Dashboard Template',
      editType: RESOURCE_EDIT_TYPE.READONLY,
      value: 'Available after installation',
      attributeName: 'Expected table(s)',
      attributeValue: getRequiredTablesForLooker(LOOKER_DATA_SOURCES),
      checkFn: checkExpectedTables,
    }
  ],
  headlineStyle: {
    backgroundColor: '#202124',
    fontColor: 'white',
  },
};

/**
 * The solution menus. 'SOLUTION_MENUS' is part of Cyborg framework.
 */
const SOLUTION_MENUS = [
  new MojoSheet(SHOPPING_INSIDER_MOJO_CONFIG),
  EXPLICIT_AUTH_MENUITEM,
];
