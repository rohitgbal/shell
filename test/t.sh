for(( i=1; i<=15; i++))
do
x+=$(echo "Argname${i},Argvalue${i},");
done
echo $x > x.txt
