conf_f="config"
rm final_out.csv
mkdir -p "out"
tot=$1
end=$((tot*3))
echo $end
#Reading Seed file and  schema file location
#seed_f=$(grep -w "csv_seed_name and location" $conf_f | cut -d ":" -f 2 | xargs)
scme_f=$(grep -w "csv_scme-file_location" $conf_f | cut -d ":" -f 2 | xargs)

#######Reading Flags########
#Same/Random Flag
r_flag=$(grep -w "same_text" $conf_f | cut -d "=" -f 2 | xargs)
#echo $r_flag
#Number flag
n_flag=$(grep -w "use_numbers" $conf_f | cut -d "=" -f 2 | xargs)
#echo $n_flag
#Al-Num Flags
an_flag=$(grep -w "use_random_alphanumeric" $conf_f | cut -d "=" -f 2 | xargs)
#echo $an_flag
#Given Chars  Flag
g_flag=$(grep -w "same_text_with" $conf_f | cut -d "=" -f 2 | xargs)
#echo $g_flag
#Given Chars
s_text=$(grep -w "same_text_with" $conf_f | cut -d "=" -f 1 | rev |cut -d " " -f 1| rev | xargs)
#echo $s_text
#No of Chars
num_text=$(grep -w "same_text_with" $conf_f |cut -d " " -f 2 |xargs)
#echo $num_text
##############################################################################
if [[ $an_flag == "Y" ]]
then
pat="a-zA-Z0-9"
elif [[ $n_flag == "Y" && $an_flag == "N" ]]
then
pat="0-9"
else
pat="a-zA-Z"
fi
##############################################################################
for ((rn=0; rn<$end; rn++))
do
ai=0 # Array index & out file
out=() # Out Array
#Reading each line in schema file
while read line;
do
line="${line%\,}"
line="${line#\,}"
#Extracting Types and name in schema files
c_names=$(echo $line | cut -d " " -f 1)
c_types=$(echo $line | cut -d " " -f 2)
#Loop through Types & Generate random number
for c_type in $c_types
do
case "$c_type" in
	"varchar"*)
	#Varchar variable#########################################
	#Extracting max len in varchar
	max=$(echo $c_type| cut -d "(" -f 2 | cut -d ")" -f 1)
	#randomly generating digit
	dig=$(cat /dev/urandom  | tr -dc "0-9" | fold -w 3 | head -n 1)
	dig=$(echo $dig | sed 's/^0*//')
	dig=$((dig%max+1))
	out_tmp=$(cat /dev/urandom  | tr -dc "${pat}" | fold -w ${dig} | head -n 1)
	if [[ $g_flag == 'Y' ]]
	then
	#Getting Like varible
        s_text=$(echo $s_text | fold -w 1)
        s_text=($s_text)
        z=""
        l=$(cat /dev/urandom | tr -dc "1-5"| fold -w 1| head -n 1)
        for(( j=1; j<=l; j++ ))
        do
        i=$(cat /dev/urandom | tr -dc ${num_text} | fold -w 1| head -n 1)
        z="$z${s_text[$i]}"
        done
	out_tmp="${out_tmp}${z}"
	out_tmp=$(echo $out_tmp | cut -c ${max}-)
	fi
	;;
	"char"*)
	#char Variable###############################################
	#get no of char in varble
	dig=$(echo $c_type| cut -d "(" -f 2 | cut -d ")" -f 1)
	#Genrating random alphanumeric
	out_tmp=$(cat /dev/urandom  | tr -dc "${pat}" | fold -w ${dig} | head -n 1)
	if [[ $g_flag == 'Y' ]]
	then
	#Getting Like varible
	s_text=$(echo $s_text | fold -w 1)
	s_text=($s_text)
	z=""
	l=$(cat /dev/urandom | tr -dc "1-5"| fold -w 1| head -n 1)
	for(( j=1; j<=l; j++ ))
	do
	i=$(cat /dev/urandom | tr -dc ${num_text} | fold -w 1| head -n 1)
	z="$z${s_text[$i]}"
	done
	out_tmp="${out_tmp::-${l}}${z}"
	fi
        ;;
	"numeric"*)
	#Numeric variable#########################################
	dig=$(echo $c_type| cut -d "(" -f 2 | cut -d "," -f 1)
	dec=$(echo $c_type| cut -d ")" -f 1| cut -d "," -f 2)
	dig=$((dig+1))
	p=$((dig-dec))
        n=$(cat /dev/urandom  | tr -dc "0-9" | fold -w ${dig} | head -n 1)
        out_tmp=$(echo $n | sed "s/./\./${p}")
	;;
	"Boolean")
	#Boolean variable########################################
	b=$(cat /dev/urandom  | tr -dc "0-1" | fold -w 1 | head -n 1)
	if [ $b -eq 0 ]
	then
	out_tmp="False"
	else
	out_tmp="True"
	fi
	;;
	"int")
	#Integer variable#####################################
	out_tmp=$(cat /dev/urandom  | tr -dc "0-9" | fold -w 6 | head -n 1)
        ;;
esac
done
out[$ai]=$out_tmp
echo "${out[$ai]}" >> "out/${ai}.txt"
ai=$((ai+1)) # Incrementing array index
done <$scme_f
done

for file in "out"/*.txt
do
if [[ $(grep -w "True\|False" $file | wc -l) == $(cat $file |wc -l) ]]
then
cat $file | head -n ${tot} >>"${file}.csv"
else
cat $file | uniq | head -n ${tot} >>"${file}.csv"
fi
done

#Checking Random flag
if [[ r_flag == "Y" ]]
then
paste -d "," out/*.csv | sort | uniq>> final_out.csv
else
paste -d "," out/*.csv >> final_out.csv
fi
rm -rf "out"

#while read line;
#do
#line="${line%\,}"
#line="${line#\,}"
#echo $line
#done < $seed_f
