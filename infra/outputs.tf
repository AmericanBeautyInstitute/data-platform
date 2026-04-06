output "dagster_vm_name" {
  description = "Name of the Dagster daemon VM."
  value       = google_compute_instance.dagster.name
}

output "dagster_vm_zone" {
  description = "Zone of the Dagster daemon VM."
  value       = google_compute_instance.dagster.zone
}

output "raw_bucket_name" {
  description = "Name of the GCS raw data bucket."
  value       = google_storage_bucket.raw.name
}

output "tf_state_bucket_name" {
  description = "Name of the GCS Terraform state bucket."
  value       = google_storage_bucket.tf_state.name
}

output "dagster_service_account_email" {
  description = "Email of the Dagster daemon service account."
  value       = google_service_account.dagster.email
}
