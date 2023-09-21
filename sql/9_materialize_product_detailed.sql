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

# Creates stored procedure for materializing product detailed data.
#
# Gets a snapshot of latest available data date.
# The main reason for the decision was the fragile nature of the data transfers. When either of the
# Google Ads or GMC transfer fails which seems to happen quite often bulk of the MarkUp dashboard is
# not usable.

CREATE OR REPLACE
  PROCEDURE
    `{project_id}.{dataset}.product_detailed_proc`()
      BEGIN
CREATE OR REPLACE TABLE `{project_id}.{dataset}.product_detailed_materialized`
AS (
  WITH
    MaxDataDate AS (
      SELECT
        MAX(_DATA_DATE) AS _DATA_DATE
      FROM `{project_id}.{dataset}.targeted_products_view_{external_customer_id}`
    )
  SELECT
    *
  FROM
    `{project_id}.{dataset}.product_detailed_view_{external_customer_id}`
  INNER JOIN MaxDataDate
    USING (_DATA_DATE)
);

END;
