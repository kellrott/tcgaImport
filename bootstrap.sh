#!/usr/bin/env bash

apt-get update
apt-get install -y git
apt-get install -y python-pip

easy_install -U distribute

pip install synapseclient

if [ -e /vagrant/synapseConfig ]; then 
	cp /vagrant/synapseConfig /home/vagrant/.synapseConfig
	chown vagrant:vagrant /home/vagrant/.synapseConfig 
fi

