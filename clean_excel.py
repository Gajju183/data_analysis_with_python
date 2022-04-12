import pandas as pd

excel_workbook = 'C:/Users/Gajraj Singh/python_examples/clean_excel.xlsx'
sheet1 = pd.read_excel(excel_workbook, sheet_name = 'Sheet1')
#print(sheet1.head(10))

first_name_list = []
last_name_list = []

excel_names = sheet1['First Name, Last Name']
#print(excel_names)

for name in excel_names:
    first_name, last_name = name.split(' ',1)
    first_name_list.append(first_name)
    last_name_list.append(last_name)

#print(first_name_list)

sheet1.insert(0,"First_Name",first_name_list)
sheet1.insert(1,"Last_Name",last_name_list)
del sheet1['First Name, Last Name']
#print(sheet1.head(10))

Important_numbers = sheet1['Important Number']
pd.to_numeric(Important_numbers)
#print(Important_numbers)
Edited_Important_Numbers = Important_numbers *2
sheet1['Important Number'] = Edited_Important_Numbers
#print(sheet1.head(10))

sheet1.to_excel('output.xlsx')
