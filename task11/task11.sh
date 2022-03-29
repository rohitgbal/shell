# Task 11 
#
# @Author RGB 
# @email rohitgbal@gmail.com
# @Updated 09/03/2022

# Directories root
root_d=$(grep "rootfolder:" config | cut -d ":" -f 2)
exts=$(awk '/^#EXT#START$/{flag=1;next}/^#EXT#END$/{flag=0}flag' config)

fun_get_file()
{
repo=$1
wflow=$2
#Creating Path
path="${root_d}/${repo}/Workflows/${wflow}"
for ext in $exts
do
p_files=$(find "${path}/Properties/" -type f -name "*.prop")
for p_file in $p_files
do
lines=$(grep ${ext} $p_file)
#Loop throgh all lines contains extension
for line in $lines
do
# Check File exists
if [ -e "$line" ]
then
i=0;
while((1))
do
((i++))
f_loc=$(echo $line | rev | cut -d "/" -f 2- | rev )
file=$(echo $line | rev | cut -d "/" -f 1 | rev )
line=$(find . -name "${file}" -printf "%l\n")
p_file=$(echo $p_file | rev | cut -d "/" -f 1 | rev)
echo $repo,$wflow,$p_file,$i,$file,$f_loc
if [ -z "$line" ]
then
break;
fi
done # while loop
fi
done
done
done
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
fun_get_file $1 $2 $3
fi

# To Get the  workflows
if [[ $# > 1 ]]
then

wflow=$2
repo=$(echo $repos | rev | cut -d "/" -f 2| rev )
echo $repo
fun_get_file $repo $wflow
else
for repo in $repos
do 
wflows=$(echo ${repo}Workflows/*/)
for wfl in $wflows
do
wflow=$(echo $wfl | rev | cut -d "/" -f 2| rev )
repo=$(echo $wfl | rev | cut -d "/" -f 4| rev )
fun_get_file $repo $wflow 
done
done
fi
