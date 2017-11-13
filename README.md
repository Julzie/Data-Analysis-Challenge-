# Data-Analysis-Challenge

The script reads 3 CSV files, parses the data, creates row for each user (with users in   the original input order).
The assumption is that there is too much data to fit in memory, hence I use data streaming. I also assume that the transactions for a single user fit in memory.
The output has the following columns:
1. user_id
2. Number of transactions per user
3. The sum of transaction amounts for the user
4. Minimum balance (running sum) for a user at the end of each day
5. Maximum balance (running sum) for the user at the end of each day


To run the script:
	Run through command line with a parameter of the absolute path where the data files to be processed are stored. 
	
  Example:
		python /Users/Desktop/dataChallange/dt.py <path>
		<path>  ==> Folder with the files transactions1.csv, transactions2.csv and transactions3.csv
		The script reads the content for the folder. Hence if there are other files that are unrelated, it will cause an error.

Output:
	The script writes the results into an excel file which is created in the same folder where the data files are. 
