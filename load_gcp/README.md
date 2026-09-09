## Local Setup for Terraform and GCP

### Pre-Requisites
1. Terraform : https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
2. Cloud Provider account: https://console.cloud.google.com/ 

### Setup Terraform (once it is installed and your gcp account is made)
1. Create a new project in your google cloud account, for exemple named "France Travail Data Project".
2. (Optionnal but higly recommanded) In IAM & Admin/Identity & Access/IAM, grant to yourself (click on the pencil at right of the first line with your principal and name and click on "add another role") the roles **Storage Admin** + **Storage Object Admin** + **BigQuery Admin** + **BigQuery Data Owner**.
3. In IAM & Admin/Resource Management/Organization, assure that the policies `iam.managed.disableServiceAccountCreation`, 
`iam.managed.disableServiceAccountKeyCreation` and `storage.uniformBucketLevelAccess` are inactive.
4. In IAM & Admin/Identity & Access/Service Accounts, create a new service account (for exemple "FTDP_service_account"). Grant the same roles to this service account as the ones stated in step 2.
5. Still in IAM & Admin/Identity & Access/Service Accounts, click on your new service account and go to Keys/Add key/Create a new key. You can chose json type. After creation the authentication keys will be downloaded on your computer by your browser.
6. Put the json file in a secured folder. 
7. Install Google SDK : https://docs.cloud.google.com/sdk/docs/install-sdk#linux
8. Set environnment variable : 
```shell
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
```
. Verify authentication with `gcloud auth application-default login`
9. In `variables.tf`, put your project-id and the regions/locations (list here : https://cloud.google.com/about/locations?hl=fr#regional-products).
10. Execute 
```shell
terraform init
terraform plan 
terraform apply
``` 
and your bucket and dataset will be created ! 