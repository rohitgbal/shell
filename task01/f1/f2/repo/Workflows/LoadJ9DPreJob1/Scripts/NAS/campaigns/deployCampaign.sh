#!/bin/sh
##########################################################################
#    deployCampaign - Runs One-time Scripts
#    Input Parmaters:
#     - env - Environment
##########################################################################

# Place each script of command to be ran into the script[] array
script[0]="hdfs_dir_setup.txt";
env=$1;

for i in ${script[@]}; do 
    eval "$(<${script[0]})" | sed -e "s/\${env}/$env/g";
done