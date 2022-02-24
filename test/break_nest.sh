for((i=0; i<5; i++ ))
do
for((j=0; j<4; j++ ))
do
if [[ j==3 ]]
then
exit
fi
echo $i $j
done
done
