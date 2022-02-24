#Task 01b Substituion of out.csv based on properties files
#Execution ./task01b.sh
# @Author RGB 
# @email rohitgbal@gmail.com
# @Updated 09/02/2022

#echo "${num},${repo},${wflow},${prop_f},${action_n},${arg_s},${parm_s},${fl_s}"
# Directories input output root
root_d=$(grep "rootfolder:" config | cut -d ":" -f 2)
out_d=$(grep "outputfolder:" config | cut -d ":" -f 2)
outfile="${out_d}/out.csv"
while((1))
do
in_f=$(date +"%Y%m%d_%H%M%S")
infile="${out_d}/out_${in_f}.csv"
mv $outfile $infile

#Read Each line 
while read -r line
do
# read feild of each line to array
feild=()
IFS=',' read -ra feild <<< ${line}
feild_len=${#feild[@]}
if [[ -e "${feild[3]}" ]]
then
prop_f="${feild[3]}.tmp"
cp ${feild[3]} ${prop_f}
sed -i "s/( )//g" ${prop_f}
fi
n_line=""
for (( i=0; i<$feild_len; i++ ))
do
echo ${feild[$i]} > tmp
if [[ "${feild[$i]}" == *'${'* ]]
then
fld=$(echo "${feild[$i]}" | cut -d "{" -f 2 | cut -d "}" -f 1 )
sub_fld=$(grep -w "^${fld}" ${prop_f} | grep -w "^${fld}=.*" | cut -d "=" -f 2 | tail -1 | tr -d "\r")
fld1='${'${fld}'}'
if [[ "$sub_fld" != "" ]]
then
sed -i -e "s|$fld1|$sub_fld|" tmp
fi
n_line+=$(cat tmp)","
else
n_line+=${feild[$i]}","
fi
rm -rf tmp
done # Reading colum values
echo $n_line >> $outfile
rm -rf $prop_f
done < ${infile}

# Check End of Execution
dif=$(diff $infile $outfile)
if [[ "$dif" == "" ]]
then
echo "End of Substitution"
exit
fi
done


