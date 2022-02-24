while IFS= read -r line
do
ql_f=$(echo $line |cut -d "," -f 5)
if ! [ -z ${ql_f} ]
then
#echo $ql_f
if [ -f ${ql_f}  ]
then
#echo $ql_f
cat "${ql_f}" | tr '\n' "X" | tr '\n' ',' > abc
#cat "${ql_f}" | tr -s  " " |grep -i " insert into "|tr '[:upper:]' '[:lower:]'|awk -F 'into' '{print $2}'
fi
fi
done < "out.csv"
