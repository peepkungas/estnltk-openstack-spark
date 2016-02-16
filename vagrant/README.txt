Setting up Cloudera cluster with Spark in OpenStack
url: https://docs.google.com/document/d/1qHifGWJcWnNjuPfAE7odiES7qwNtNXwIVNsA6wp-EgU/pub

Ansible script to set up a cluster with Cloudera on OpenStack
 - Created script is for replacing manual actions on setup ("3.1 Installing Cloudera Manager")
 - Created script is for test/dev env only

Half-manual
 - use "vagrant up" and wait setup to complete
 - use "vagrant ssh" to log into server and execute: "sudo /opt/cloudera-manager/cloudera-manager-installer.bin --i-agree-to-all-licenses --noprompt --noreadme --nooptions"
 - wait for installation to complete (will show progress on screen) and continue with "3.2 Using Cloudera Manager to install Cloudera" 

Auotomatic
 - use "vagrant up" and wait setup to complete
 - wait for installation to complete (will take a while) and continue with "3.2 Using Cloudera Manager to install Cloudera"
 - to login into virtualbox use "vagrant ssh"

For production
 - create production.yml, choose method (half-manual or automatic) and deploy file to all instances
 - copy inside of vagrant.yml to production.yml and replace/add all hosts 
 - for deploying use: ansible-playbook -i <hosts_file> production.yml
