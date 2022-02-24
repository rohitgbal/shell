x='${abc()}'
fl="text.txt"
echo ${x} > tmp
while [[ "${x}" == '${'* ]]
do
y=$(echo "${x}" | cut -d "{" -f 2 | cut -d "}" -f 1)
z=$(grep "${y}=" ${fl} | cut -d "=" -f 2)
if [[ "${x}" == *'${'* ]]
then
sed -i "1s|"${x}"\}|${z}|" tmp
x=$(tail -1 tmp | grep -m 1 -o "\${.*}" | cut -d "}" -f 1)
fi
done
x=$(tail -1 tmp)
tail -1 tmp
rm -rf tmp


