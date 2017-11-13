import sys
import os
import pandas as pd
import re


""" Global list to insert final results after processing """
results = []

""" Method to process each portion of the """
def processing(df,results):
    df = df.astype(dtype={'user_id':"str",'acc_id':"str", 'amnt':"float64",'date':'datetime64[ns]','type':"str" })
    df = df[['user_id','amnt','date','type']]   # Reduce to relevant columns
    user_id = df['user_id'].values[0]   # extract the user_id for future usage
    df = df.groupby(['date','type']).aggregate({'amnt':'sum', 'user_id':'count'}).reset_index()  # Group by date and type for calculations
    trans_total = len(df)   # transactions total
    trans_sum = round(df.amnt.sum(),2)  # Transactions sum

    """Here make credit column and debit column to more easily find total balance for each day"""
    df = df.pivot(index='date', columns='type', values='amnt').fillna(0).reset_index() # reshape to a table with 'credit' and 'debit' as column names to calculate daily balance
    if 'credit' in df.columns:
        df['cum_total'] = (df['credit'] - df['debit']).cumsum() # cumulutive total (rolling sum) 
    else:
        df['cum_total'] = -df['debit'].cumsum()
    min_bal = round(float(df['cum_total'].min()),2)     # minimum balance 
    max_bal = round(float(df['cum_total'].max()),2)     # maximum balance
    results.append([user_id,trans_total, trans_sum, min_bal,max_bal])   # append the final result for this uder to the list which later will be converted to data frame



""" Method to generate one line from the large files at a time. Later th lines are processed into chunks of dataframes"""
def read_large_file(file_object):
    # Loop until reach the end of the file
    while True:
        
        ls = []
        first = file_obj.readline() # Read first line to get the user_id in order to compare later with subsequent reads and stop wheh reach ne user_id
        r = re.split('(\d{14,16}).(\d{14,16}).(\d*\.\d*).\D*(\d{4}-\d{2}-\d{2}).(debit|credit).\D*',"") # Parse the line into the relevant groups. 
        ls.append(r[1:-1])  # Append the parsed line (now a list with separate data for future columns) to a list.
        for row in file_obj:
            
            r = re.split('(\d{14,16}).(\d{14,16}).(\d*\.\d*).\D*(\d{4}-\d{2}-\d{2}).(debit|credit).\D*',"".join(row))
            if r[1] == ls[0][0]:    # If the user_id in the new line is in the list, that is, if this line still belongs to the current batch
                ls.append(r[1:-1])  # append
            else:
                data = pd.DataFrame(ls, columns=['user_id', 'acc_id', 'amnt','date','type'])    # Otherwise, create a dataframe and later pass for processing
                ls.clear()  # Clear the list 
                ls.append(r[1:-1])  # append the current (new) user_id
                
                yield data
        data = pd.DataFrame(ls, columns=['user_id', 'acc_id', 'amnt','date','type'])
        yield data
        if row:
            break



path = sys.argv[1]  # Get the pathname from command line
names = os.listdir(path)    # List all files to read in the pathname directory

for name in names:  # For each file name in the directory
    with open(path+name, 'r') as file_obj:  
        ls = []     # list to store the lines of each user. Being cleared after a new user is introduced.
        file_obj.readline() # Skip the header. Once per file.  # CAN ALSO DO ==> f.seek(1) instead
 
        for chunk in read_large_file(file_obj):   # Generate a new line from the file
            processing(chunk,results)

### Write the final results to an Excel file
##writer = pd.ExcelWriter(path + "output.xlsx")
##pd.DataFrame(results,columns=['user_id','trans_total', 'trans_sum', 'min_bal','max_bal']).to_excel(writer,'Sheet1')
results = pd.DataFrame(results,columns=['user_id','trans_total', 'trans_sum', 'min_bal','max_bal'])
print(results)

##writer.save()

