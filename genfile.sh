#/bin/sh
for i in $(seq 1 1000); do
    cp file.so $(printf "file%04u.so" $i)
done
