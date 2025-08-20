#
# FILE: `StockTrader/infrastructure/terraform/variables.tf`
#

#
# Project Configuration Variables
#

variable "project_id" {
	description = "GCP Project ID within which resources created"
	type = string
	default = "stocktrader12"
}

variable "region" {
	description = "GCP region"
	type = string
	default = "us-east1"
}

variable "zone" {
	description = "GCP zone in which VM instance created"
	type = string
	default = "us-east1-b"
}



#
# Compute Instance VM Variables
#

variable "instance_name" {
	description = "Compute instance name"
	type = string
	default = "stocktrader-rudi"
}

variable "machine_type" {
	description = "Machine type of compute instance"
	type = string
	default = "e2-micro"
}

variable "disk_size_gb" {
	description = "Size of boot disk (GB)"
	type = number
	default = 30
}

variable "network_tags" {
	description = "List of network tags to apply to compute instance vm"
	type = list(string)
	default = ["stocktrader"]
}

#
# VM OS Configuration Variables
#

variable "source_image_family" {
	description = "Image family for boot disk"
	type = string
	default = "debian-12"
}

variable "source_image_project" {
	description = "Project containing the source image"
	type = string
	default = "debian-cloud"
}
