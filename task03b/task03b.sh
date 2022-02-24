#in file argument example
t_file=$1

#Column Details Section
s_col=$(( $(cat $t_file | grep -wn "           col_name"|cut -d ":" -f 1)+1 ))
e_col=$(( $(cat $t_file | grep -m 1 -n "NULL"|cut -d ":" -f 1)-1 ))
#echo $s_col $e_col
col_name=$(awk "NR>=${s_col} && NR<=${e_col}" $t_file | grep "| " |cut -d "|" -f 2 | cut -d ":" -f 1)
col_type=$(awk "NR>=${s_col} && NR<=${e_col}" $t_file | grep "| " | cut -d "|" -f 3)
col_name=($col_name)
col_type=($col_type)
col_len=${#col_name[@]}
for(( i=0; i<$col_len; i++ ))
do
echo "${i},${col_name[$i]},${col_type[$i]}" >>csv_col_details.csv
done

#Partition Info Details
s_par=$(( $(cat $t_file | grep -wn "# Partition" |cut -d ":" -f 1)+2 ))
e_par=$(( $(cat $t_file | grep -wn "# Detailed" |cut -d ":" -f 1)-2 ))
par_name=$(awk "NR>=${s_par} && NR<=${e_par}" $t_file | grep "| " | cut -d "|" -f 2 | cut -d ":" -f 1)
par_type=$(awk "NR>=${s_par} && NR<=${e_par}" $t_file | grep "| " | cut -d "|" -f 3)
par_name=($par_name)
par_type=($par_type)
par_len=${#par_name[@]}
for(( i=0; i<$par_len; i++ ))
do
echo "${i},${par_name[$i]},${par_type[$i]}" >>csv_par_details.csv
done

#Table Detailed Information
s_tdet0=$(( $(cat $t_file | grep -wn "# Detailed" |cut -d ":" -f 1)+1 ))
e_tdet0=$(( $(cat $t_file | grep -wn " CreateTime:" |cut -d ":" -f 1)-1 ))
tdet_name0=$(awk "NR>=${s_tdet0} && NR<=${e_tdet0}" $t_file | grep "| " | cut -d "|" -f 2 | cut -d ":" -f 1)
tdet_type0=$(awk "NR>=${s_tdet0} && NR<=${e_tdet0}" $t_file | grep "| " | cut -d "|" -f 3)
c_time=$(grep " CreateTime:" $t_file | cut -d "|" -f 3| xargs| tr " " "#")
s_tdet1=$(( $(cat $t_file | grep -wn " CreateTime:" |cut -d ":" -f 1)+1 ))
e_tdet1=$(( $(cat $t_file | grep -wn " Table Type:" |cut -d ":" -f 1)-1 ))
tdet_name1=$(awk "NR>=${s_tdet1} && NR<=${e_tdet1}" $t_file | grep "| " | cut -d "|" -f 2 | cut -d ":" -f 1)
tdet_type1=$(awk "NR>=${s_tdet1} && NR<=${e_tdet1}" $t_file | grep "| " | cut -d "|" -f 3)
t_type=$(cat $t_file | grep -wn " Table Type:" |cut -d "|" -f 3| xargs)
tdet_name0=($tdet_name0)
tdet_type0=($tdet_type0)
tdet_name1=($tdet_name1)
tdet_type1=($tdet_type1)
tdet_name=( "${tdet_name0[@]}" "CreateTime" "${tdet_name1[@]}" "Table#Type" )
tdet_type=( "${tdet_type0[@]}" "${c_time}" "${tdet_type1[@]}" "${t_type}" )
tdet_len=${#tdet_name[@]}
for(( i=0; i<$tdet_len; i++ ))
do
echo "${i},${tdet_name[$i]},${tdet_type[$i]}">>csv_table_details.csv
done

#Table Parameters
s_tpar=$(( $(cat $t_file | grep -wn "Table Parameters" |cut -d ":" -f 1)+1 ))
e_tpar=$(( $(cat $t_file | grep -wn "Storage Information" |cut -d ":" -f 1)-2 ))
tpar_name=$(awk "NR>=${s_tpar} && NR<=${e_tpar}" $t_file |grep "|"| cut -d "|" -f 3)
tpar_type=$(awk "NR>=${s_tpar} && NR<=${e_tpar}" $t_file | grep "|"| cut -d "|" -f 4)
tpar_name=($tpar_name)
tpar_type=($tpar_type)
tpar_len=${#tpar_name[@]}
for(( i=0; i<$tpar_len; i++ ))
do
echo "${i},${tpar_name[$i]},${tpar_type[$i]}">>csv_table_par.csv
done

#Storage Information
s_str=$(( $(cat $t_file | grep -wn "Storage Information" |cut -d ":" -f 1)+2 ))
e_str=$(( $(cat $t_file | grep -wn "Bucket Columns" |cut -d ":" -f 1)-2 ))
s_lib=$(cat $t_file | grep -w "SerDe Library" |cut -d "|" -f 3| xargs)
n_buk=$(cat $t_file | grep -w "Num Buckets" |cut -d "|" -f 3| xargs)
str_name=$(awk "NR>=${s_str} && NR<=${e_str}" $t_file |grep "|"| cut -d "|" -f 2 | cut -d ":" -f 1)
str_type=$(awk "NR>=${s_str} && NR<=${e_str}" $t_file | grep "|"| cut -d "|" -f 3)
str_name=($str_name)
str_type=($str_type)
str_name=( "SerDe#Library" "${str_name[@]}" "Num#Buckets" )
str_type=("${s_lib}" "${str_type[@]}" "${n_buk}" )
str_len=${#str_name[@]}
for(( i=0; i<$str_len; i++ ))
do
echo "${i},${str_name[$i]},${str_type[$i]}">>csv_storage_info.csv
done

#Bucket Columns
b_cols=$(grep -w "Bucket Columns:" $t_file | cut -d "[" -f 2 | cut -d "]" -f 1 | tr "," "\n")
i=0
for b_col in $b_cols
do
i=$((i+1))
echo "${i},${b_col}">>csv_bucket_col.csv
done

#Sortt Columns
s_cols=$(grep -w "Sort Columns:" $t_file | cut -d "[" -f 2 | cut -d "]" -f 1 | tr "," "\n")
i=0
for s_col in $s_cols
do
i=$((i+1))
echo "${i},${s_col}">>csv_sort_col.csv
done
