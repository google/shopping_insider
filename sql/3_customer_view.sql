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

# Creates a snapshot of customer_view.
#
# The view will get the latest account info and create derived columns useful for further processing
# of data.
CREATE OR REPLACE VIEW `{project_id}.{dataset}.customer_view_{external_customer_id}`
AS (
  SELECT DISTINCT
    _DATA_DATE,
    _LATEST_DATE,
    customer_id,
    customer_descriptive_name
  FROM
    `{project_id}.{dataset}.ads_Customer_{external_customer_id}`
);
