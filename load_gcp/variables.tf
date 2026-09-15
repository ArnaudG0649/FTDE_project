variable "project" {
  description = "Project"
  # ENTER YOUR OWN PROJECT ID HERE
  default     = "project-addc57f7-fb9f-4221-a91" 
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "FT_data" # DON'T TOUCH
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "ft_data_files_bucket" # DON'T TOUCH
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD" # DON'T TOUCH
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "europe-west8"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west8"
}
