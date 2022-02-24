c="ABCDE"
x="VWXYZ"
c=$(echo $c | fold -w 1)
c=($c)
z=""
l=$(cat /dev/urandom | tr -dc "1-5"| fold -w 1| head -n 1)
echo $l
for(( j=1; j<=l; j++ ))
do
i=$(cat /dev/urandom | tr -dc "1-3"| fold -w 1| head -n 1)
echo $i ${c[$i]}

z="$z${c[$i]}"
o=${x::-${l}}
done
echo $o$z

i=0;
x=()
ys="a b c d"
for y in $ys
do
x[$i]=$y
echo $i ${x[$i]} $y
i=$((i+1))
done

