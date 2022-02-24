#Task 01a Extraction of Workflow.xml data to CSV foreach property file
#Execution ./task01a.sh  or ./task01a.sh <reponame> or ./task01a.sh <reponame> <workflowname>
# @Author RGB 
# @email rohitgbal@gmail.com
# @Updated 09/02/2022

# Directories input output root
root_d=$(grep "rootfolder:" config | cut -d ":" -f 2)
out_d=$(grep "outputfolder:" config | cut -d ":" -f 2)
#echo $root_d $out_d
mkdir -p $out_d
out_csv="${out_d}/out.csv"
rm -rf "${out_d}/out"*
num=0
#echo "${num},${repo},${wflow},${prop_f},${action_n},${argmnt_s}${arg_s}${parm_s}${fl_s}"
echo "SeqNo,RepoName,WflowName,Properties_file,Actionname,SH_QL_file,Arg1,Arg2,Arg3,Arg4,Arg5,Arg6,Arg7,Arg8,Arg9,Arg10,Arg11,Arg12,Arg13,Arg14,Arg15,Arg16,Arg17,Arg18,Arg19,Arg20,Arg21,Arg22,Arg23,Arg24,Arg25,Arg26,Arg27,Arg28,Arg29,Arg30,Arg31,Arg32,Arg33,Arg34,Arg35,Argname1,Argvalue1,Argname2,Argvalue2,Argname3,Argvalue3,Argname4,Argvalue4,Argname5,Argvalue5,Argname6,Argvalue6,Argname7,Argvalue7,Argname8,Argvalue8,Argname9,Argvalue9,Argname10,Argvalue10,Argname11,Argvalue11,Argname12,Argvalue12,Argname13,Argvalue13,Argname14,Argvalue14,Argname15,Argvalue15,Pname1,Pvalue1,Pname2,Pvalue2,Pname3,Pvalue3,Pname4,Pvalue4,Pname5,Pvalue5,Pname6,Pvalue6,Pname7,Pvalue7,Pname8,Pvalue8,Pname9,Pvalue9,Pname10,Pvalue10,Pname11,Pvalue11,Pname12,Pvalue12,Pname13,Pvalue13,Pname14,Pvalue14,Pname15,Pvalue15,Pname16,Pvalue16,Pname17,Pvalue17,Pname18,Pvalue18,Pname19,Pvalue19,Pname20,Pvalue20,Pname21,Pvalue21,Pname22,Pvalue22,Pname23,Pvalue23,Pname24,Pvalue24,Pname25,Pvalue25,Pname26,Pvalue26,Pname27,Pvalue27,Pname28,Pvalue28,Pname29,Pvalue29,Pname30,Pvalue30,File1,File2,File3,File4,File5,File6,File7,File8,File9,File10," >> ${out_csv}
#Function for CSV Generation
fun_csv()
{
repo=$1
wflow=$2
path="${root_d}/${repo}/Workflows/${wflow}"

# Getting Properties Files
prop_fs=$(find "${path}/Properties/DEV" -type f -name "*.properties")
for prop_f in $prop_fs
do

#Getting Xml Files & Creating Temp Xml Files
r_xml_f="${path}/Scripts/HDFS/workflow.xml"
xml_f="${r_xml_f}.tmp"
cp ${r_xml_f} ${xml_f}

# Actions 
actn_s=$(grep -n '<action' $xml_f | cut -d ":" -f 1)
action_s=( ${actn_s} )
actn_e=$(grep -n '</action>' $xml_f | cut -d ":" -f 1)
action_e=( ${actn_e} )
a_len=${#action_s[@]}

# Action/Argument/File Processing
for ((k=0; k<$a_len; k++ ))
do
#Action Name
action_n=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f |grep '<action'| cut -d "\"" -f 2 | tr -d '\r')
sh_f=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f | grep '<exec>'  | cut -d ">" -f 2 | cut -d "<" -f 1)
ql_f=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f | grep '<script>'  | cut -d ">" -f 2 | cut -d "<" -f 1)
if [[ ! -z "$sh_f" ]]
then
sh_ql_f="${sh_f}"
elif [[ ! -z "$ql_f" ]]
then
sh_ql_f="${ql_f}"
else
sh_ql_f=""
fi
sh_ql_f=$(echo "${sh_ql_f}" | tr -d '\r')

#argument Processing
argmnts=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f | grep '<argument>'  | cut -d '>' -f 2 | cut -d '<' -f 1)
argmnt=( $argmnts )
argmnt_s=""
for(( j=0; j<35; j++ ))
do
argmnt_s+=$(echo "${argmnt[$j]},")
done #argumnt

# Arg & Value Processing
args1=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f | grep '<arg>'  | cut -d ">" -f 2 | cut -d "<" -f 1 | cut -d "=" -f 1)
args2=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f | grep '<arg>'  | cut -d ">" -f 2 | cut -d "<" -f 1 | cut -d "=" -f 2)
arg1=( ${args1} )
arg2=( ${args2} )
arg_s=""
for(( j=0; j<15; j++ ))
do
arg_s+=$( echo "${arg1[$j]},${arg2[$j]}," )
done #arg

# Param & Value Processing

parms1=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f | grep '<param>'  | cut -d ">" -f 2 | cut -d "<" -f 1 | cut -d "=" -f 1)
parms2=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f | grep '<param>'  | cut -d ">" -f 2 | cut -d "<" -f 1 | cut -d "=" -f 2)
parm1=( ${parms1} )
parm2=( ${parms2} )
parm_s=""
for(( j=0; j<30; j++ ))
do
parm_s+=$( echo "${parm1[$j]},${parm2[$j]}," )
done #parm

# file Processing
fls=$(sed -n "${action_s[$k]},${action_e[$k]}p" $xml_f | grep '<file>'  | cut -d '>' -f 2 | cut -d '<' -f 1 | cut -d '#' -f 1 | xargs)
fl=( ${fls} )
fl_s=""
for(( j=0; j<10; j++ ))
do
fl_s+=${fl[$j]},
done #file

# Writing to CSV
num=$((num+1))
echo "${num},${repo},${wflow},${prop_f},${action_n},${sh_ql_f},${argmnt_s}${arg_s}${parm_s}${fl_s}" >> ${out_csv}
done #action_len
rm -rf ${xml_f}
done #props
} # End of Function


#Get Repo name
if [[ $# > 2 ]]
then 
echo "Error in arg number"
exit
elif [[ $# == 2 || $# == 1 ]]
then
repos="${root_d}/${1}/"
else
repos=$(echo ${root_d}/*/)
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
fun_csv $repo $wflow
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
fun_csv $repo $wflow
fi
fi
done
done
echo "Generated Out file ${out_csv}"
fi

