# Shopping Insider

Disclaimer: This is not an officially supported Google product.

Shopping Insider is a tool to enable retailers grow their business using
[Google Merchant Center](https://www.google.com/retail/solutions/merchant-center/)
by taking actionable data-driven decisions to optimize shopping feed health and
ads performance.

## Contents

*   [1. Overview](#1-overview)
    *   [1.1. Value Proposition](#11-value-proposition)
    *   [1.2. Solution Architecture](#12-solution-architecture)
    *   [1.3. Solution Options](#13-solution-options)
*   [2. Installation](#2-installation)
    *   [2.1. Environment Setup](#21-environment-setup)
    *   [2.2. Installation Options](#22-installation-options)
    *   [2.3. Multiple Multi-Client Account(MCA) Support](#23-multiple-multi-client-accountmca-support)
    *   [2.4. SA360 Support](#24-sa360-support)

## 1. Overview

The Shopping Insider solution is built for Shopping Ads customers to take
actionable data-driven decisions to improve their feed health and shopping ads
performance.

### 1.1. Value Proposition

*   Users can find opportunities and issues at each stage of the Shopping Funnel
    both overall and detailed data cuts.

*   Richer insights with data joins to provide overall and product level
    performance information pivoted towards custom attributes (product type,
    brand, etc) for deeper insights.

*   A dashboard to share data and insights across different teams and areas of
    the business seamlessly to address issues & optimize performance.

### 1.2 Solution Architecture

The solution will export data from GMC and Google Ads to your Google Cloud
Project on a daily basis and provide insights via Looker Studio dashboard.

<img src="images/architecture.png">

### 1.3 Solution Options

Please join this
[Google Group](https://groups.google.com/g/shopping-insider-public) to gain the
viewer access for some of the resources below. (i.e. templates, spreadsheets)

#### Shopping Insider

This is the base solution that exclusively uses the products and product issues
tables available via the Merchant Center Transfer. This will allow you to set up
the
[Shopping Insider Dashboard Template](https://lookerstudio.google.com/c/u/0/reporting/f1859d41-b693-470c-a404-05c585f51f20/preview).

#### Shopping Insider + Market Insights [Work In Progress]

Stay tune for the updates!

## 2. Installation

### 2.1. Environment Setup

#### 2.1.1 Create a GCP project with billing account

You may skip this step if you already have a GCP account with billing enabled.

*   How to [Create a GCP account](https://cloud.google.com/?authuser=1) (if you
    don't have one already!)

*   How to
    [Create and Manage Projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects)

*   How to
    [Create, Modify, or Close Your Billing Account](https://cloud.google.com/billing/docs/how-to/manage-billing-account)

#### 2.1.2 Check the permissions

Make sure the user running the installation has following permissions.

*   [Standard Access For GMC](https://support.google.com/merchants/answer/1637190?hl=en)

*   [Standard Access For Google Ads](https://support.google.com/google-ads/answer/7476552?hl=en)

*   [Editor(or Owner) Role in Google Cloud Project](https://cloud.google.com/iam/docs/understanding-roles)

### 2.2. Installation Options

There are two ways you can install Shopping Insider:

[**Option 1:**](#221-option-1-install-via-cyborggoogle-sheet) Install via Cyborg
(Google Sheet), if you are not comfortable with Command Line Interface (CLI) and
want a clear view of any failed component install, if any.

*   **Pros:** Fast and easy to deploy. Displays what happens when Shopping
    Insider was installed and keeps a record of all kinds of GCP resources that
    were enabled, checked, created or updated. It’s easy to upgrade to a new
    version, e.g. the sql files are always downloaded from GitHub for the latest
    version. When there are optional features, Cyborg offers a way to easily
    reconfigure the features of the solution.

*   **Cons:** The user who makes the copy of Cyborg should belong to the same
    org which owns the GCP or have a Google Account, as they need to be able to
    change AppScript project number.

[**Option 2:**](#222-option-2-install-via-shell-scriptcommand-line) Install via
Shell script, if you are comfortable with Command Line Interface (CLI) and need
more details into what is getting installed beforehand.

*   **Pros:** will support any client GCP structure

*   **Cons:** longer and more technical to deploy than via Cyborg

#### 2.2.1. Option 1: Install via Cyborg(Google Sheet)

##### 2.2.1.1. Make a copy of the tool

1.  Join the [Google Group][group] group and wait for approval.
1.  After you join the group, you can visit the [Google Sheets tool][cyborg] and
    make a copy.

[group]: https://groups.google.com/g/shopping-insider-public
[cyborg]: https://docs.google.com/spreadsheets/d/1pcB_JK5yZRxKCs4fLQY_KoQWUy5AEApAjt5Vy79uXas/edit#gid=151491750

##### 2.2.1.2. Configure the OAuth consent screen

If there is no OAuth consent screen in this GCP project, you need to
[configure the OAuth consent screen][oauth_consent] first. When you create the
consent screen, some settings need to be:

1.  `Publishing status` as `In production`, otherwise the refresh token will
    expire every 7 days.
1.  `User type` could be `External` or check [User type][user_type] for more
    details.
1.  No `scopes` need to be filled in.

[oauth_consent]: https://developers.google.com/workspace/guides/configure-oauth-consent
[user_type]: https://support.google.com/cloud/answer/10311615?hl=en#zippy=%2Cexternal%2Cinternal

##### 2.2.1.3. Update GCP project number to your Google Sheet

1.  Get your GCP project number. See how to
    [determine the project number of a standard Cloud project][project_number].
1.  Click Google Sheets menu `Extensions` -> `Apps Script` to open Apps Script
    editor window.
1.  On the Apps Script window, click `⚙️` (Project Settings) at the left menu
    bar, then click the button `Change project`.
1.  Enter the project number and click the button `Set project`.

[project_number]: https://developers.google.com/apps-script/guides/cloud-platform-projects#determine_the_id_number_of_a_standard

##### 2.2.1.4. Deploy Shopping Insider

To install **Shopping Insider**:

1.  Switch to sheet `Shopping Insider` and input required information in the
    sheet, including `GMC Account Id`, `Google Ads MCC` and `Project Id`.
1.  Click menu `🤖 Cyborg` -> `Shopping Insider` -> `Check resources` to run a
    check. If an error happened, fix it and retry `Check resources`.
1.  If there are resources marked as `TO_APPLY`, use menu `🤖 Cyborg` ->
    `Shopping Insider` -> `Apply changes` to apply the modifications.
1.  If there are resources not checked, continue to step 2.
1.  After all resources are marked as `OK`, click the Dashboard Template link to
    make a copy. You need to confirm and save the dashboard in the opened
    window.

> Note: When you first time click the menu item, an OAuth authorization window
> may prompt you to grant permissions. After you complete it, you need to click
> the menu item again to continue.

> Note: Some processes, e.g. waiting for a newly created Data Transfer to finish
> the first run takes time. If there was a timeout, then wait sometime and come
> back retry `Check Resources`.

> Note: Why `TO_APPLY`? Some operations required user inputs, for example, the
> location of a new BigQuery dataset. Cyborg will pause there and ask you to
> select a location and click menu `Apply Changes` as a confirmation.

#### 2.2.2. Option 2: Install via Shell Script(command line)

##### 2.2.2.1. Setup local environment.

[Download and authenticate gcloud.](https://cloud.google.com/sdk/#Quick_Start)

Alternatively, if the GMC account has less than 50 Million products, you could
use [Cloud Shell](https://ssh.cloud.google.com/cloudshell?shellonly=true), which
comes with gcloud already installed. The cloud shell disconnects after 1 hour
and hence we recommend using local environment for large accounts since they
could take more than 1 hour to finish the installation.

##### 2.2.2.2. Check out source codes

Open the [cloud shell](https://ssh.cloud.google.com/cloudshell?shellonly=true)
or your terminal(if running locally) and clone the repository.

```
  git clone https://github.com/google/shopping_insider
```

##### 2.2.2.3 Run install script

Please provide following inputs when running the `setup.sh` script:

*   [GCP Project Id](https://cloud.google.com/resource-manager/docs/creating-managing-projects)

*   [Google Merchant Center Id](https://support.google.com/merchants/answer/188924?hl=en)

*   [Google Ads External Customer Id](https://support.google.com/google-ads/answer/1704344?hl=en)

```
cd shopping-insider;
sh setup.sh --project_id=<project_id> --merchant_id=<merchant_id> --ads_customer_id=<ads_customer_id>
```

When installing, the script will check whether the current user has the proper
authorization to continue. It may ask you to open cloud authorization URL in the
browser. Please follow the instructions as mentioned in the command line.

##### Note - If the script fails when you run it for the first time, it might be due to delay in preparing Merchant account data. Usually accounts with massive data set. Please wait up to 1-3 days before re-running the script.

During the installation process, the script will do following:

*   Enable Google Cloud Components and Google APIs

    *   [BigQuery](https://console.cloud.google.com/bigquery)

    *   [BigQuery Data Transfer](https://console.cloud.google.com/bigquery/transfers)

*   Create Google Merchant Center and Google Ads data transfers.

*   Create recurring data transfer jobs so that the latest data is imported in
    near real time.

*   Create following Shopping Insider specific SQL tables.

    *   product_detailed_materialized - Latest snapshot view of products
        combined with performance metrics. Each offer is split into rows for
        each targeted country, rows are keyed by unique_product_id and
        target_country.
    *   product_historical_materialized - Historic snapshot of performance
        metrics at a product category level.

##### 2.2.2.4. [Optional] Update location and locales if different than US

*   If your data shouldn't be materialized in US, change the BigQuery dataset
    location in config.yaml

*   [Market Insights only] Adjust the locales in best_sellers_workflow.sql, by
    default set to "en-US"

*   You could make the changes before running the install script or after

    *   If you're making the changes afterwards, re-run the install script
    *   Check the scheduled queries in BigQuery and disable any older version of
        the Main Workflow

##### 2.2.2.5. Configure Data Sources

You will need to create or copy required Data Source(s) in Data Studio:

###### For Shopping Insider:

*   Create `product_detailed_materialized` Data Source (linked to
    `shopping_insider.product_detailed_materialized`)
*   Create `product_historical_materialized` Data Source (linked to
    `shopping_insider.product_historical_materialized`)

To create a data source:

*   Click on the
    [link](https://lookerstudio.google.com/c/u/0/datasources/create?connectorId=2)

*   Make sure you are using BigQuery connector. If not choose "`BigQuery`" from
    the list of available connectors.

*   Search your GCP Project Id under My Projects.

*   Under Dataset, click on "`shopping_insider`".

*   Under Table, choose the required table view.

*   Click `Connect` on the top right corner and wait for the data-source to be
    created

To copy a data source:

*   Click on the data source template link above.

*   Click on the <img src="images/copy_icon.png"> icon in the top right corner
    next to "Create Report".

*   Click "Copy Data Source" on the "Copy Data Source" pop-up.

*   Select your Project, Dataset, and Table to be connected, then press
    "Reconnect" in the top right corner.

*   Click "Apply" on the "Apply Connection Changes" pop-up

*   Repeat this process for all three data source templates above.

##### 2.2.2.6. Create Data-Studio Dashboard(s)

###### For Shopping Insider:

*   Click on the following link to the Looker Studio template:
    [link](https://lookerstudio.google.com/c/u/0/reporting/f1859d41-b693-470c-a404-05c585f51f20/preview)

*   Click "`Use my own data`"

*   Replace data sources by choosing the new "`product_detailed_materialized`"
    and "`product_historical_materialized`" data-sources created in the previous
    step

*   Click "`Edit and share`"

##### Note - The performance metrics in the dashboard might take 12-24 hours to appear.

### 2.3. Multiple Multi-Client Account(MCA) Support

1.  If you have more than one Google Merchant Center, repeat the installation
    steps for all MCA.

1.  Creates a data set in Big Query
    ([Guide](https://cloud.google.com/bigquery/docs/datasets#create-dataset)).

1.  Creates the views to union all the data set from step 1.

    ```
    CREATE OR REPLACE VIEW `<project_id>.<final_dataset>.product_detailed_materialized`
    AS (
      SELECT
        *
      FROM
        `<project_id>.<dataset_1>.product_detailed_materialized`
      UNION ALL
      SELECT
        *
      FROM
        `<project_id>.<dataset_2>.product_detailed_materialized`
      ......
    );
    ```

    ```
    CREATE OR REPLACE VIEW `<project_id>.<final_dataset>.product_historical_materialized`
    AS (
      SELECT
        *
      FROM
        `<project_id>.<dataset_1>.product_historical_materialized`
      UNION ALL
      SELECT
        *
      FROM
        `<project_id>.<dataset_2>.product_historical_materialized`
      ......
    );
    ```

1.  Replaces the dashboard data sources with the views.

### 2.4. SA360 Support

Shopping Insider aggregates information from both Google Ads and Merchant Center accounts. When
you use SA360 and want to advertise on Google, you need to create a Google Ads account
([source](https://support.google.com/searchads/answer/1717081?hl=en#link)).
Those accounts are then synced together, so all the updates you do through SA360 are automatically
reflected in Google Ads accounts.

Shopping Insider, even though it is not taking any information directly from SA360, will accurately
reflect the shopping campaigns status based on information coming from Google Ads accounts. **When you
install Shopping Insider, provide your Google Ads account number, not your SA360 account number.**
