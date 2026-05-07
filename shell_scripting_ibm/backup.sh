#!/bin/bash

# This checks if the number of arguments is correct
# If the number of arguments is incorrect ( $# != 2) print error message and exit
if [[ $# != 2 ]]
then
  echo "backup.sh target_directory_name destination_directory_name"
  exit
fi

# This checks if argument 1 and argument 2 are valid directory paths
if [[ ! -d $1 ]] || [[ ! -d $2 ]]
then
  echo "Invalid directory path provided"
  exit
fi

# [TASK 1]
targetDirectory=$1
destinationDirectory=$2

# [TASK 2]
echo "The target directory is: $targetDirectory"
echo "The destination directory is: $destinationDirectory"

# [TASK 3]
# date as the current timestamp, expressed in seconds format
currentTS=$(date +%s)

# [TASK 4]
backupFileName="backup-[$currentTS].tar.gz"

# We're going to:
  # 1: Go into the target directory
  # 2: Create the backup file
  # 3: Move the backup file to the destination directory

# To make things easier, we will define some useful variables...

# [TASK 5]
# Define a variable called origAbsPath with the absolute path of the current directory as the variable's value.
origAbsPath=$(pwd)

# [TASK 6]
# Define a variable called destAbsPath whose value equals the absolute path of the destination directory.
cd $destinationDirectory
destAbsPath=$(pwd)

# [TASK 7]
# cd into the original directory origAbsPath and then cd into targetDirectory.
cd $origAbsPath
cd $targetDirectory

# [TASK 8]
#Define a numerical variable called yesterdayTS as the timestamp (in seconds) 24 hours prior to the current timestamp, currentTS
yesterdayTS=$(($currentTS - 86400))

declare -a toBackup

# In the for loop, use the wildcard to iterate over all files and directories in the current folder.
# Inside the for loop, you want to check whether the $file was modified within the last 24 hours. To get the last-modified date of a file in seconds, use date -r $file +%s then compare the value to yesterdayTS. if [[ $file_last_modified_date -gt $yesterdayTS ]] then the file was updated within the last 24 hours! Since much of this wasn't covered in the course, for this task you may copy the code below and paste it into the double square brackets [[]]: `date -r $file +%s` -gt $yesterdayTS
for file in *  # [TASK 9] 
do
  if [[ $(date -r "$file" +%s) -gt $yesterdayTS ]]
  then
    toBackup+=("$file")
  fi
done

# [TASK 12]
tar -czvf $backupFileName ${toBackup[@]}

# [TASK 13]
mv $backupFileName $destAbsPath

# Congratulations! You completed the final project for this course!