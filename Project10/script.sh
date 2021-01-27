#!/bin/bash

: '
26.01.21
This script downloads a file from a specified location and after unziping it strips the file from malformed, emopty or redundant lines. 
Afterwards data from the cleaned file is uploaded to mysql database and an email is sent containing a report from the process.

bash script.sh <download_url> <unzip_password> <input_file_path> <index_number> <db_name> <db_password> <db_hostname> <email>

Script arguments:

arg 1 - download url
arg 2 - unzip password
arg 3 - path to the loaction of InternetSales_old.txt
arg 4 - index_number
arg 5 - database name
arg 6 - mysql password
arg 7 - myslq hostname
arg 8 - email where report will be sent

'

mkdir PROCESSED

cur_date=$(date '+%Y%m%d')
log_filename="$0_$cur_date.log"

touch PROCESSED/$log_filename

# downloading the file
wget -cq "$1"  

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - File downloaded" >>PROCESSED/"$log_filename"

filename=$(basename "$1")
unzip -P "$2" $filename -d downloaded_data 

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - File extracted" >>PROCESSED/"$log_filename"

files=(downloaded_data/*)

filename_txt=$(basename "${files[0]}" )
txt_file_path=downloaded_data/${filename_txt}

lines_count=$(wc -l < "$txt_file_path")

echo -e "File has: $lines_count lines" >>PROCESSED/"$log_filename"

# removing empty lines
sed -i '' '/^$/d' $txt_file_path

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Removed empty lines" >>PROCESSED/"$log_filename"

no_empty_lines_cnt=$(wc -l < "$txt_file_path")
no_empty_lines_cnt=$(($no_empty_lines_cnt - 1))

empty_lines="$(($lines_count-$no_empty_lines_cnt))"
echo -e "Dowloaded file has: $empty_lines empty lines" >> PROCESSED/"$log_filename"

# remove malformed lines

filename_no_ext=$(echo "$filename" | cut -f 1 -d '.')

filename_bad=${filename_no_ext}".bad"${cur_date}

awk -v n="7" -F'|' 'NF!=n ' "$txt_file_path" > "$filename_bad"     
awk -v n="7" -F'|' 'NF==n ' "$txt_file_path" > temp_awk.txt
mv temp_awk.txt "$txt_file_path" 


cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Removed malformed lines" >>PROCESSED/"$log_filename"


invalid_ncol=$(wc -l < "$filename_bad") 				
echo -e "Downloaded file has: $invalid_ncol wrong length rows" >>PROCESSED/"$log_filename"

# removeing duplicates

awk 'NR == 1; NR>1 {print $0 |"sort -n"}' "$txt_file_path" > sorted_txt  
uniq -d sorted_txt  >> "$filename_bad"

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Removed duplicates" >>PROCESSED/"$log_filename"

uniq -u sorted_txt > "$txt_file_path"
rm sorted_txt

bad_lines_with_duplicated=$(wc -l < "$filename_bad")
duplicated_lines="$(($bad_lines_with_duplicated-$invalid_ncol))"
mail_duplicated="Downloaded file has: $duplicated_lines duplicated lines."
echo -e "$mail_duplicated" >>PROCESSED/"$log_filename"

# remove records with wrong OrderQuality

awk -v val=100 -F'|' '$3 >val || $3 ==""' "$txt_file_path" |tail -n +2 >> "$filename_bad"
head -n 1 "$txt_file_path" >header_line
cat header_line >tmp_awk_quant
awk -v val=100 -F'|' '$3 <=val && $3!="" ' "$txt_file_path"  >>tmp_awk_quant

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Removed records with wrong OrderQuality" >>PROCESSED/"$log_filename"

mv tmp_awk_quant "$txt_file_path"

bad_lines_with_quantity=$(wc -l < "$filename_bad")
quant_lines="$(($bad_lines_with_quantity-$bad_lines_with_duplicated))"
echo -e "File has: $quant_lines bad OrderQuality lines" >>PROCESSED/"$log_filename"

# comparing with old file
tail -n +2 "$txt_file_path" | sort > sorted_new_txt	
tail -n +2 "$3" | sort > sorted_old_txt

diff  sorted_old_txt sorted_new_txt  --changed-group-format=""  >> "$filename_bad"
EXITCODE=$?

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Removing existing lines" >>PROCESSED/"$log_filename"


all_bad_lines=$(wc -l <"$filename_bad")
lines_common_to_both_files=$(($all_bad_lines-$bad_lines_with_quantity))
echo -e "Already existing lines: $lines_common_to_both_files" >> PROCESSED/"$log_filename"

cat header_line > tmp_after_check 
diff sorted_old_txt sorted_new_txt --old-group-format=""  --unchanged-group-format=""  >>tmp_after_check
mv tmp_after_check "$txt_file_path"

# remove lines with sevcret code

awk  -F'|' ' $7 !=""'  InternetSales_new.txt  | tail -n +2 | cut -d'|' -f -6 | awk '{print $0"|"}' >> "$filename_bad"

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Removed lines with secret code present" >>PROCESSED/"$log_filename"

all_bad_lines_without_secret_code=$(wc -l <"$filename_bad")
lines_with_secret_code=$(($all_bad_lines_without_secret_code-$all_bad_lines))
echo "File has $lines_with_secret_code lines with secret_code column not empty" >>PROCESSED/"$log_filename"

cat header_line >tmp_non_secret_code
tail -n +2 InternetSales_new.txt|awk  -F'|' ' $7 ==""'  >>tmp_non_secret_code 
mv tmp_non_secret_code "$txt_file_path"

rm sorted_old_txt sorted_new_txt

# remove lines with no comma between name and surname

awk -F"|" '!match($3,",") ' "$txt_file_path" | tail -n +2  >> "$filename_bad"

cat header_line > tmp_without_comma
awk -F"|" 'match($3,",") ' "$txt_file_path"  >> tmp_without_comma

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Removed lines with missing comma" >>PROCESSED/"$log_filename"

mv tmp_without_comma "$txt_file_path"
all_bad_lines_name_surname=$(wc -l <"$filename_bad")
lines_with_bad_name_surname=$(($all_bad_lines_name_surname-$all_bad_lines_without_secret_code))
mail_all_bad="File has got $all_bad_lines_name_surname wrong or not appropriate lines"
echo -e "File has $lines_with_bad_name_surname lines without comma between name and surname" >>PROCESSED/"$log_filename"

# split first name and last name
 
echo "FIRST_NAME" > first_name
echo "LAST_NAME" > last_name
cut -d'|' -f-2 "$txt_file_path" >first2col
cut -d'|' -f4- "$txt_file_path"  >last4col
cut -d'|' -f3 "$txt_file_path" | tr -d "\""| cut -d','  -f2 |tail -n +2 >>first_name
cut -d'|' -f3 "$txt_file_path" | tr -d "\""| cut -d','  -f1 |tail -n +2 >> last_name 
paste -d'|' first2col first_name last_name last4col > "$txt_file_path"


cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Splitting column Customer_name to first and last name  - Succesful" >>PROCESSED/"$log_filename"

rm first2col last4col first_name last_name header_line

# create table in msql db 

col1=$(head -n1 "$txt_file_path" |cut -d'|' -f1)
col2=$(head -n1 "$txt_file_path" |cut -d'|' -f2)
col3=$(head -n1 "$txt_file_path" |cut -d'|' -f3)
col4=$(head -n1 "$txt_file_path" |cut -d'|' -f4)
col5=$(head -n1 "$txt_file_path" |cut -d'|' -f5)
col6=$(head -n1 "$txt_file_path" |cut -d'|' -f6)
col7=$(head -n1 "$txt_file_path" |cut -d'|' -f7)
col8=$(head -n1 "$txt_file_path" |cut -d'|' -f8)

password_db=$(echo "$6")
export MYSQL_PWD=$password_db 
db_name="CUSTOMERS_$4"

mysql -u "$5" -h  "$7" -P 3306 -D "$5" --silent -e "CREATE TABLE $db_name($col1 INTEGER,$col2 VARCHAR(20),$col3 VARCHAR(40),$col4 VARCHAR(40),$col5 VARCHAR(20),$col6 VARCHAR(20),$col7 FLOAT,$col8 VARCHAR(20) );"

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Creating table in db - Succesful" >>PROCESSED/"$log_filename"

tail -n +2 "$txt_file_path" | tr ',' '.' >txt_file_path_no_header
mv "$txt_file_path" PROCESSED/

mysql -u "$5" -h  "$7" -P 3306 -D "$5" --silent -e "LOAD DATA LOCAL INFILE 'txt_file_path_no_header' INTO TABLE $db_name FIELDS TERMINATED BY '|';"

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Inserting data to db table - Succesful" >>PROCESSED/"$log_filename"

rm txt_file_path_no_header

# change secret code values 

random_string="$(openssl rand -hex 5)"

mysql -u "$5" -h  "$7" -P 3306 -D "$5" --silent -e "UPDATE $db_name SET $col8='$random_string';"

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Updatind table  - Succesful" >>PROCESSED/"$log_filename"

# export data from table to .csv file

mysql -u "$5" -h  "$7" -P 3306 -D "$5" --silent -e "SELECT * FROM $db_name;"|sed 's/\t/,/g' > $db_name.csv

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Exporting table to .csv file  - Succesful" >>PROCESSED/"$log_filename"

# compress .csv file

zip -q $db_name $db_name.csv  

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Zipping .csv file - Succesful" >>PROCESSED/"$log_filename"


mail_all_bad="File has got $all_bad_lines_name_surname wrong or not appropriate lines"

good_lines=$(($lines_count-$all_bad_lines_name_surname))
mail_all_good="File has $good_lines proper lines"

count_table=$(mysql -u "$5" -h  "$7" -P 3306 -D "$5" --silent -e "SELECT COUNT(*) FROM $db_name;")
mail_insert="Inserted $count_table lines to database"

echo "Downloaded File has: $lines_count lines \n$mail_all_good \n$mail_duplicated \n$mail_all_bad \n$mail_insert"

# send emails with attachment

echo -e "$mail_downloaded\n$mail_all_good \n$mail_duplicated \n$mail_all_bad \n$mail_insert" |mailx -s "CUSTOMERS LOAD - $timestamp" $8
mailx -s "$timestamp,$good_lines "-a $db_name.zip -a $log_filename ${8}

cur_time=$(date '+%Y%m%d%H%M%S')
echo "$cur_time - Email sent" >>PROCESSED/"$log_filename"
