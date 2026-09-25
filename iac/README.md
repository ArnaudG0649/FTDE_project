## Local Setup for Terraform and GCP

### Pre-Requisites
1. Terraform: https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
2. Cloud Provider account: https://console.cloud.google.com/

### Setup Terraform (once it is installed and your GCP account has been created)
1. Create a new project in your Google Cloud account, for example named "FTDE-project".
2. In IAM & Admin/Identity & Access/IAM, grant yourself (click the pencil icon to the right of the row showing your principal and name, then click "Add another role") the roles **Storage Admin** + **Storage Object Admin** + **BigQuery Admin** + **BigQuery Data Owner**.
3. In IAM & Admin/Resource Management/Organization, ensure that the policies `iam.managed.disableServiceAccountCreation`, 
`iam.managed.disableServiceAccountKeyCreation` and `storage.uniformBucketLevelAccess` are inactive.
4. In IAM & Admin/Identity & Access/Service Accounts, create a new service account (for example "FTDE-project-service-account"). Grant this service account the same roles as the ones stated in step 2.
5. Still in IAM & Admin/Identity & Access/Service Accounts, click on your new service account and go to Keys/Add key/Create a new key. You can choose the JSON type. After creation, your browser will download the authentication key file to your computer.
6. Put the JSON file in the `config` folder and name it `gcp-service-account.json`. Be careful: this file must absolutely remain secret. You can move it to a more secure folder when you don't need GCP access. This file is git-ignored within the `config` folder.
7. (From this point on, additional or different steps might be needed on Windows.)
8. Install the Google Cloud SDK: https://docs.cloud.google.com/sdk/docs/install-sdk#linux
9. Set the environment variable:
```shell
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/the/project>/config/gcp-service-account.json"
```
Then verify authentication with `gcloud auth application-default login`.

10. In `iac/variables.tf`, put your project ID and the regions/locations (list here: https://cloud.google.com/about/locations?hl=fr#regional-products).
11. In the `iac` folder, execute 
```shell
terraform init
terraform plan 
terraform apply
```
and your bucket and dataset will be created!