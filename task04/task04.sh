#Task 01a Extraction of Workflow.xml data to CSV foreach property file
#Execution ./task04.sh <reponame> <workflowname> <file>  or ./task0sh.sh <reponame> or ./task04.sh <reponame> <workflowname>
# @Author RGB 
# @email rohitgbal@gmail.com
# @Updated 14/02/2022

# Directories input output root
root_d=$(grep "rootfolder:" config | cut -d ":" -f 2)

fun_get_ql()
{

repo=$1
wflow=$2
#Creating Path
path="${root_d}/${repo}/Workflows/${wflow}"

if [[ $# == 3 ]]
then
ql_fs="${path}/Scripts/HDFS/${3}"
if [[ ! -f ${ql_fs} ]]
then
echo "!!! Erorr in file path"
exit
fi
else
ql_fs=$(find "${path}/Scripts/HDFS" -type f -name "*.ql")
fi

for ql_f in $ql_fs
do
#Skip DELETE FROM 
del_c=$(grep -i "DELETE FROM" $ql_f | wc -l)
if [[ $del_c != 0 ]]
then
continue #skip loop
else
grep -i " FROM " $ql_f == ignore the temp tables.
fi
# Getting Properties Files
prop_fs=$(find "${path}/Properties/DEV" -type f -name "*.properties")

#Source extraction

from_c=$(grep -i -w "from" $ql_f | wc -l )
insert_c=$(grep -i -w "insert" $ql_f | wc -l )

#for prop_f in $prop_fs
#do
result=$(grep -i "insert " $ql_f )
if [[ $result == *"insert into table"* ]]
then
results=$(echo $result | awk '{for (I=1;I<NF;I++) if ($I == "table") print $(I+1)}')
elif [[ $result == *"INSERT INTO TABLE"* ]]
then
results=$(echo $result | awk '{for (I=1;I<NF;I++) if ($I == "TABLE") print $(I+1)}' )
elif [[ $result == *"insert overwrite table"* ]]
then
results=$(echo $result | awk '{for (I=1;I<NF;I++) if ($I == "table") print $(I+1)}' )
elif [[  $result == *"INSERT OVERWRITE TABLE"*  ]]
then
results=$(echo $result | awk '{for (I=1;I<NF;I++) if ($I == "TABLE") print $(I+1)}')
elif [[ $result == *"insert into"* &&  $result != *"table"*  ]]
then
results=$(echo $result | awk '{for (I=1;I<NF;I++) if ($I == "into") print $(I+1)}' )
elif [[ $result == *"INSERT INTO"* &&  $result != *"TABLE"*  ]]
then
results=$(echo $result | awk '{for (I=1;I<NF;I++) if ($I == "INTO") print $(I+1)}' )
else
continue
fi
echo $results > tmp 
sed -e "s/\r//g" tmp > tmp1
results=$(cat tmp1)
for rslt in $results
do
x=y
#echo $rslt $ql_f $wflow $repo
done
#done # Properties
rm -f tmp*
done # QL loop
}

#Get Repo name
if [[ $# > 3 || $# == 0 ]]
then 
echo "Error in arg number"
exit
elif [[ $# == 2 || $# == 1 ]]
then
repos="${root_d}/${1}/"
else
fun_get_ql $1 $2 $3
fi

# To Get the  workflows
if [[ $# > 1 ]]
then
wflow=$2
repo=$(echo $repos | rev | cut -d "/" -f 2| rev )
if [[ -d "${root_d}/${repo}/Workflows/${wflow}/Properties/DEV" ]]
then
p_count=$(find "${root_d}/${repo}/Workflows/${wflow}/Properties/DEV" -type f -name "*.properties")
if [[ $p_count > 0 ]]
then
fun_get_ql $repo $wflow
fi
fi
else
for repo in $repos
do 
wflows=$(echo ${repo}Workflows/*/)
for wfl in $wflows
do
wflow=$(echo $wfl | rev | cut -d "/" -f 2| rev )
repo=$(echo $wfl | rev | cut -d "/" -f 4| rev )
if [[ -d "${root_d}/${repo}/Workflows/${wflow}/Properties/DEV" ]]
then
p_count=$(find "${root_d}/${repo}/Workflows/${wflow}/Properties/DEV" -type f -name "*.properties")
if [[ $p_count > 0 ]]
then
fun_get_ql $repo $wflow 
fi
fi
done
done
fi