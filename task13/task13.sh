# Task 13 Extract The tables from properties file
#Usage : ./task13.sh or ./task13.sh <repo> or ./task13.sh <repo> <workflow> or ./task13.sh <repo> <workflow> <propertyfile>
# @Author RGB 
# @email rohitgbal@gmail.com
# @Updated 29/03/2022

#Remove Previous CSV file
rm  -f out.csv

# Directories root
root_d=$(grep "rootfolder:" config | cut -d ":" -f 2)

#Get Staging Table prefixes  from config
stages=$(sed -e '1,/##STAGESTART##/d' -e '/##STAGEEND##/,$d' config)

#Get Master Table prefixes config
masters=$(sed -e '1,/##MASTERSTART##/d' -e '/##MASTEREND##/,$d' config)

#Get Master Table prefixes config
others=$(sed -e '1,/##OTHERSTART##/d' -e '/##OTHEREND##/,$d' config)

fun_get_prop()
{
repo=$1
wflow=$2
prop=$3 
prop_f="${root_d}/${1}/Workflows/${2}/Properties/DEV/${3}"
#master values extraction
master_all=""
for master in $masters
do
m_varvalues=$(grep ${master} ${prop_f})
m_var_value=( ${m_varvalues} )
done
for (( i=0; i<5; i++ ))
do
m_var=$(echo ${m_var_value[$i]} | cut -d "=" -f 1)
m_value=$(echo ${m_var_value[$i]} | cut -d "=" -f 2 | rev | cut -d "/" -f 1 | rev )
master_all+=$(echo ",${m_var},${m_value}")
done

#Stating values extraction
stage_all=""
for stage in $stages
do
s_varvalues=$(grep ${stage} ${prop_f})
s_var_value=( ${s_varvalues} )
done
for (( i=0; i<5; i++ ))
do
s_var=$(echo ${s_var_value[$i]} | cut -d "=" -f 1)
s_value=$(echo ${s_var_value[$i]} | cut -d "=" -f 2 | rev | cut -d "/" -f 1 | rev )
stage_all+=$(echo ",${s_var},${s_value}")
done

#other values extraction
other_all=""
for other in $others
do
o_varvalues=$(grep ${other} ${prop_f})
o_var_value=( ${o_varvalues} )
done
for (( i=0; i<5; i++ ))
do
o_var=$(echo ${o_var_value[$i]} | cut -d "=" -f 1)
o_value=$(echo ${o_var_value[$i]} | cut -d "=" -f 2 | rev | cut -d "/" -f 1 | rev )
other_all+=$(echo ",${o_var},${o_value}")
done
echo "${repo},${wflow},${prop}${stage_all}${master_all}${other_all}" >> out.csv
}

#Get Repo name

if [[ $# > 3 ]]
then 
echo "Error in arg number"
exit
elif [[ $# == 0 ]]
then
repos=$(echo ${root_d}/*/)
elif [[ $# == 3 ]]
then
fun_get_prop $1 $2 $3
else
repos="${root_d}/${1}/"
fi

#Workflows
for repo in $repos
do

if [[ $# < 2 ]]
then
wflows=$(echo ${repo}Workflows/*/)
else
wflows="${repo}Workflows/${2}/"
fi

for wfl in $wflows
do
wflow=$(echo $wfl | rev | cut -d "/" -f 2| rev )
repo=$(echo $wfl | rev | cut -d "/" -f 4 | rev )
#properties File
if [[ -d "${root_d}/${repo}/Workflows/${wflow}/Properties/DEV" ]]
then
p_count=$(find "${root_d}/${repo}/Workflows/${wflow}/Properties/DEV" -type f -name "*.properties")
if [[ $p_count > 0 ]]
then
prop_fs=$(find "${root_d}/${repo}/Workflows/${wflow}/Properties/DEV" -type f -name "*.properties" | rev | cut -d "/" -f 1 | rev)
for prop_f in $prop_fs
do
fun_get_prop $repo $wflow $prop_f
done
fi
fi
done
done
