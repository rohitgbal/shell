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
echo $1 $2 $exts
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
