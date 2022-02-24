#Task 01a Extraction of Workflow.xml data to CSV foreach property file
#Execution ./task04.sh -f "<filename>" A/D OR ./task04.sh -s "<softlink>" A/D
# @Author RGB 
# @email rohitgbal@gmail.com
# @Updated 24/02/2022

# Default sorting set to Acsending order
s_order=0

#Identifying Switches and option
while getopts ":f:s:DA" option; do
  case $option in
	h) echo "usage: $0 "; exit ;;
    f) s_term=${OPTARG}; t_flag="f" ;;
    s) s_term=${OPTARG}; t_flag="l" ;;
	A) s_order=0 ;; #Acsending order
	D) s_order=1 ;; #Descending order
    ?) echo "error: option -$OPTARG is not implemented"; exit ;;
  esac
done
rm -rf "${s_term}.csv"
f_files=$(find . -type ${t_flag} -name "*${s_term}*")
f_fs=( $f_files ) # to array
i=1
for f_f in "${f_fs[@]}"
do
f_f=$(echo $f_f | cut -d "." -f 2- | cut -d "/" -f 2- )
f_file=$f_f
while((1))
do
f_loc=$(pwd)"/"$(echo $f_file | rev | cut -d "/" -f 2- | rev )
file=$(echo $f_file | rev | cut -d "/" -f 1 | rev )
f_file=$(find . -name "${file}" -printf "%l\n")
l_loc=$(pwd)"/"$(echo $f_file | rev | cut -d "/" -f 2- | rev )
link=$(echo $f_file | rev | cut -d "/" -f 1 | rev )
echo $i,$file,$link,$f_loc,$l_loc >> tmp
if [ -z "$f_file" ]
then
break;
fi
done # while loop
if [ $s_order == 0 ]
then
cat tmp >> "${s_term}.csv"
else
tac tmp >> "${s_term}.csv"
fi
((i++))
rm tmp
done # For loop