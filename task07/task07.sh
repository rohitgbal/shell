#Task 01a Extraction of Workflow.xml data to CSV foreach property file
#Execution ./task04.sh -f "<filename>" A/D OR ./task04.sh -s "<softlink>" A/D
# @Author RGB 
# @email rohitgbal@gmail.com
# @Updated 22/02/2022

# Default sorting set to Acsending order
s_oder="-a"

#Identifying Switches and option
while getopts ":f:s:DA" option; do
  case $option in
	h) echo "usage: $0 "; exit ;;
    f) s_term=${OPTARG}; t_flag="f" ;;
    s) s_term=${OPTARG}; t_flag="l" ;;
	A) s_oder="-a" ;;
	D) s_oder="-r" ;;
    ?) echo "error: option -$OPTARG is not implemented"; exit ;;
  esac
done

f_files=$(find . -type ${t_flag} -name "*${s_term}*")
f_fs=($f_files) # to array
for f_f in "${f_fs[@]}"
do
f_f=$(echo $f_f | cut -d "." -f 2- | cut -d "/" -f 2- )
f_file=$f_f
while((1))
do
loc=$(pwd)"/"$(echo $f_file | rev | cut -d "/" -f 2- | rev )
file=$(echo $f_file | rev | cut -d "/" -f 1 | rev )
f_file=$(find . -name "${file}" -printf "%l\n")
link=$(echo $f_file | rev | cut -d "/" -f 1 | rev )
echo $file,$link,$loc
if [ -z "$f_file" ]
then
exit
fi
done # while loop
done # For loop