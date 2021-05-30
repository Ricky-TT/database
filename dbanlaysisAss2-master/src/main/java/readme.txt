The complete process is as follows:
Firstly,run dbload, load csv file into heap file.
How to run: java dbload -p <page size> <file path>
for example, java dbload -p 4096 Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv

Secondly, run treeload, write B+tree index into file.
How to run: java -Xmx900m treeload <page size>
for example, input "java -Xmx900m treeload 4096", then you will look a file "tree.4096"

thirdly, run dbqueryByIndex, read B+tree index from file.
How to run: java -Xmx900m dbqueryByIndex <keyword> <page size>
for example, input "java -Xmx900m dbqueryByIndex 50-11/01/2019 05:00:00 PM 4096", will output matched records
Please note, keyword need to meet the left matching principle,like "50-11/01/2019 05:00:00".
