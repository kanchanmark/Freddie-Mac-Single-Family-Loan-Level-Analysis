import glob
import re
import pandas as pd
import os
import numpy as np
import sys

path = sys.argv[1]
removeFile = ''.join((path,'merged.csv'))
print (removeFile)

year = sys.argv[2]
years = re.split('-',year)
print (years)

try:
    os.remove(removeFile)
except OSError:
    pass
	
def label_delinquent_status (row):
   if row['Current Loan delinquency status'] == 'XX' :
      return 'U'
   elif row['Current Loan delinquency status'] == '0' :
      return 'C'
   elif row['Current Loan delinquency status'] == '1':
      return 'L'
   elif row['Current Loan delinquency status'] == '2':
      return 'L'
   elif row['Current Loan delinquency status'] == '3':
      return 'L'
   elif row['Current Loan delinquency status'] == 'R':
      return 'R'
   elif row['Current Loan delinquency status'] == ' ':
      return 'U'
   else:
      return 'L'
	  
def label_balance_code (row):
   if row['Zero Balance Code'] == '1.0' :
      return 'P'
   if row['Zero Balance Code'] == '3.0' :
      return 'D'
   if row['Zero Balance Code'] == '6.0':
      return 'R'
   if row['Zero Balance Code'] == '9.0':
      return 'D'

for x in years:
	File = "".join((x,".csv"))
	FileName = "".join(("sample_orig_",File))
	readOrigFile = "".join((path,FileName))
	for fname in glob.glob(readOrigFile):
	
		matchObj = re.findall (r'\d', fname)
		print (fname)
		year = ''.join(map(str, matchObj))
	
		svcg_file = "".join((path,"sample_svcg_"))
		newstr = "".join((svcg_file, year))
		string = ".csv"
		file = "".join((newstr, string))
		print (file)
	
		df1 = pd.read_csv(fname, sep = '|', low_memory = False, names=['Credit score', 'First payment date', 'First time homebuyer flag', 'Maturity Date', 'Metropolitan Statistical Area', 'Mortgage Insurance Percentage', 'Number of units', 'Occupancy status', 'Original combined loan-to-value (CLTV)', 'Original debt-to-income ratio', 'Original UPB', 'Original loan-to-value (LTV)', 'Original Interest Rate', 'Channel', 'Prepayment penalty mortgage flag', 'Product type', 'Property state', 'Property type', 'Postal code', 'Loan Sequence Number', 'Loan Purpose', 'Original Loan Term', 'Number of borrowers', 'Seller Name', 'Servicer Name', 'Super Conforming Flag'])
		df2 = pd.read_csv(file, sep = '|', low_memory = False, names=['Loan Sequence Number', 'Monthly Reporting Period', 'Current Actual UPB', 'Current Loan delinquency status', 'Loan Age', 'Remaining months to legal maturity', 'Repurchased flag', 'Modification flag', 'Zero Balance Code', 'Zero Balance Effective date', 'Current Interest Rate', 'Current deferred UPB', 'Due date of last paid installment', 'Mortgage Insurance Recoveries', 'Net Sales Proceeds', 'Non Mortgage Insurance Recoveries', 'Expenses', 'Legal Costs', 'Maintenance and Preservation Costs', 'Taxes and Insurance', 'Miscellaneous Expenses', 'Actual loss calculation', 'Modification Cost'])
	
		df2['Zero Balance Code'] = df2['Zero Balance Code'].astype(str)
	
		df2['Mortgage Status'] = df2.apply (lambda row: label_balance_code (row),axis=1)
	
		df2['Zero Balance Code'].fillna(value='NA', inplace = True)
	
		df2['Mortgage Status'].replace('None', 'NA', inplace=True)
	
		df2.loc[df2['Mortgage Status'].isnull(), 'Mortgage Status'] = 'NA'
	
		df2['Current Loan delinquency status'] = df2['Current Loan delinquency status'].astype(str)
	
		df2['Delinquent Status'] = df2.apply (lambda row: label_delinquent_status (row),axis=1)
	
		df2['Current Loan delinquency status'].fillna(value='NA', inplace = True)
	
		df2['Delinquent Status'].replace('None', 'NA', inplace=True)
	
		df2.loc[df2['Delinquent Status'].isnull(), 'Delinquent Status'] = 'NA'
	
		conditions = [
		(df2['Delinquent Status'] == 'U') & (df2['Mortgage Status'] == 'P'),
		(df2['Delinquent Status'] == 'C') & (df2['Mortgage Status'] == 'P'),
		(df2['Delinquent Status'] == 'L') & (df2['Mortgage Status'] == 'P'),
		(df2['Delinquent Status'] == 'R') & (df2['Mortgage Status'] == 'P'),
		(df2['Delinquent Status'] == 'U') & (df2['Mortgage Status'] == 'D'),
		(df2['Delinquent Status'] == 'C') & (df2['Mortgage Status'] == 'D'),
		(df2['Delinquent Status'] == 'L') & (df2['Mortgage Status'] == 'D'),
		(df2['Delinquent Status'] == 'R') & (df2['Mortgage Status'] == 'D'),
		(df2['Delinquent Status'] == 'U') & (df2['Mortgage Status'] == 'R'),
		(df2['Delinquent Status'] == 'C') & (df2['Mortgage Status'] == 'R'),
		(df2['Delinquent Status'] == 'L') & (df2['Mortgage Status'] == 'R'),
		(df2['Delinquent Status'] == 'R') & (df2['Mortgage Status'] == 'R'),
		(df2['Delinquent Status'] == 'U') & (df2['Mortgage Status'] == 'NA'),
		(df2['Delinquent Status'] == 'C') & (df2['Mortgage Status'] == 'NA'),
		(df2['Delinquent Status'] == 'L') & (df2['Mortgage Status'] == 'NA'),
		(df2['Delinquent Status'] == 'R') & (df2['Mortgage Status'] == 'NA')
		]
		
		choices = ['P', 'P', 'L', 'P','D','D','D','D','U','C','L','U','U','C','L','U']

		df2['Status'] = np.select(conditions, choices, default='NA')
		
		merge = df2.groupby('Loan Sequence Number')['Status'].agg(lambda x: '-'.join(x)).reset_index()
		
		mergeddata = pd.merge(df2, merge, on='Loan Sequence Number', how='left')
		
		REO = mergeddata.loc[mergeddata['Status_x'] == 'D'] 
		
		REODate = REO.groupby('Loan Sequence Number')['Monthly Reporting Period'].agg(lambda x: min(x)).reset_index()
		
		DefaultDate = pd.merge(mergeddata, REODate, on='Loan Sequence Number', how='left')
		
		LatestReportingMonth = DefaultDate.groupby('Loan Sequence Number')['Monthly Reporting Period_x'].agg(lambda x: max(x)).reset_index()
		
		Report = pd.merge(DefaultDate, LatestReportingMonth, on='Loan Sequence Number', how='left')
		
		Final = Report[Report['Monthly Reporting Period_x_x']==Report['Monthly Reporting Period_x_y']]
		
		mergedFinal = pd.merge(df1, Final, on='Loan Sequence Number', how='left')
		
		del mergedFinal['Mortgage Status']
		
		del mergedFinal['Delinquent Status']
		
		data = mergedFinal.replace('', 'NA', regex=True)
		
		data.where(pd.notnull(data), data.mean(), axis='columns')
		
		data.replace(r'\s+', np.nan, regex=True)
		
		data = data.rename(columns=lambda x: x.replace(' ', '_'))
		
		data = data.rename(columns=lambda x: x.replace('-', '_'))
		
		data['Class'] = np.where(data.Status_y.str.contains('D'), 'TRUE', 'FALSE')
		
		writeFile = "".join((path,"merged.csv"))
		
		data.to_csv(writeFile, mode = 'a')
	
	