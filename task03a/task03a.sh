ddl_file="LoadAXHD.ql"
line_nos=$(cat $ddl_file|grep -wn "create" | cut -d ":" -f 1)
line_nos+=" "$(cat $ddl_file| wc -l)
line_nos=($line_nos)
len=${#line_nos[@]}
#echo $len
for (( i=0; i<len-1; i++ ));
do
end=$((i+1))
t_name=$(sed -n "${line_nos[$i]}p" $ddl_file | sed -e 's/\s*$//g'| rev | cut -d " " -f 1 | rev)
t_type=$(sed -n "${line_nos[$i]}p" $ddl_file | cut -d " " -f 2)
st_as=$(sed -n "1,${line_nos[$i]}b;${line_nos[$end]}q;p" $ddl_file | grep -w "stored as" | rev |cut -d " " -f 1 | rev)
loc=$(sed -n "1,${line_nos[$i]}b;${line_nos[$end]}q;p" $ddl_file | grep -w "location" | cut -d "'" -f 2)
row_f=$(sed -n "1,${line_nos[$i]}b;${line_nos[$end]}q;p" $ddl_file | grep -w "ROW FORMAT SERDE" | cut -d "'" -f 2)
col_ds=$(sed -n "1,${line_nos[$i]}b;${line_nos[$end]}q;p" $ddl_file | awk '/^\($/{flag=1;next}/^\)$/{flag=0}flag')
j=0
export IFS=','
for col_d in $col_ds
do
j=$((j+1))
col_d=$(echo $col_d | xargs)
col=$(echo $col_d|cut -d " " -f 1)
d_type=$(echo $col_d|cut -d " " -f 2)
#echo $col $d_type
echo "${t_name},${t_type},${j},${col},${d_type},${row_f},${st_as},${loc}" >> out1.csv
done
p_bys=$(sed -n "1,${line_nos[$i]}b;${line_nos[$end]}q;p" $ddl_file | grep -w "partitioned by" | cut -d "(" -f 2 | cut -d ")" -f 1)
#echo $p_bys
p_by=($p_bys)
#echo ${p_by[@]}
p_len=${#p_by[@]}
#echo $p_len
for (( k=0; k<p_len; k++ ));
do
j=$((k+1))
p_name=$(echo ${p_by[$k]} | xargs |cut -d " " -f 1)
p_type=$(echo ${p_by[$k]} | xargs |cut -d " " -f 2)
echo "${t_name},${j},${p_name},${p_type}" >> out2.csv
done
t_props=$(sed -n "1,${line_nos[$i]}b;${line_nos[$end]}q;p" $ddl_file | grep -w "tblproperties" | cut -d "(" -f 2 | cut -d ")" -f 1)
#echo $t_props
x=0
export IFS=','
for t_prop in $t_props
do
((x++))
echo "${t_name},${x},${t_prop}" >> out3.csv
done

done
