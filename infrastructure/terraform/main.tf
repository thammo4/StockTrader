#
# FILE: `StockTrader/infrastructure/terraform/main.tf`
#

#
# Define data source to retrieve latest Debian 12 image
#

# data "google_compute_image" "debian_image" {
# 	family = var.source_image_family
# 	project = var.source_image_project
# }


#
# Define primary compute instance resource
#

resource "google_compute_instance" "stocktrader_vm" {
	name = var.instance_name
	machine_type = var.machine_type
	zone = var.zone

	tags = var.network_tags

	boot_disk {
		initialize_params {
			# image = data.google_compute_image.debian_image.self_link
			image = "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-12-bookworm-v20250709"
			size = var.disk_size_gb
			type = "pd-standard"
		}
		auto_delete = true
	}

	network_interface {
		network = "default"
		access_config {
			network_tier = "PREMIUM"
		}
	}

	service_account {
		email = data.google_compute_default_service_account.default.email
		scopes = [
			"https://www.googleapis.com/auth/devstorage.read_only",
			"https://www.googleapis.com/auth/logging.write",
			"https://www.googleapis.com/auth/monitoring.write",
			"https://www.googleapis.com/auth/pubsub",
			"https://www.googleapis.com/auth/service.management.readonly",
			"https://www.googleapis.com/auth/servicecontrol",
			"https://www.googleapis.com/auth/trace.append"
		]
	}

	shielded_instance_config {
		enable_secure_boot 		= false
		enable_vtpm 			= true
		enable_integrity_monitoring 	= true
	}

	scheduling {
		automatic_restart 		= true
		on_host_maintenance 		= "MIGRATE"
		preemptible 			= false
	}

	metadata = {
		startup-script = file("${path.module}/scripts/gcp_vm_debian.sh")
		# ssh-keys = ...
		# startup-script = file("${path.module}/startup-script.sh")
	}

	lifecycle {
		prevent_destroy = false
		ignore_changes = [
			metadata["ssh-keys"],
# 			metadata["startup-script"]
		]
	}
}


#
# Define data source to get default compute service account
#

data "google_compute_default_service_account" "default" {}
