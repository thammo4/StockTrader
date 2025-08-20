#
# FILE: `StockTrader/infrastructure/terraform/outputs.tf`
#

#
# Produce relevant information about created vm instance
#

output "instance_name" {
	description = "Name of created compute instance vm"
	value = google_compute_instance.stocktrader_vm.name
}

output "instance_id" {
	description = "Server-assigned instance unique ID"
	value = google_compute_instance.stocktrader_vm.instance_id
}

output "external_ip" {
	description = "External IP address of compute instance vm"
	value = google_compute_instance.stocktrader_vm.network_interface[0].access_config[0].nat_ip
}

output "internal_ip" {
	description = "Internal IP address of compute instance vm"
	value = google_compute_instance.stocktrader_vm.network_interface[0].network_ip
}

output "self_link" {
	description = "URI of created compute instance vm"
	value = google_compute_instance.stocktrader_vm.self_link
}

output "zone" {
	description = "GCP zone of compute instance vm"
	value = google_compute_instance.stocktrader_vm.zone
}
