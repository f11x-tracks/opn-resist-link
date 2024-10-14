import pandas as pd
import PyUber
import csv, re, time
from datetime import datetime
import os

directory = f'//f21pucnasn1.f21prod.mfg.intel.com/FuzionUploads/Litho/Tracks/dash-OPN-ENTITY-RCP-CHEM/'
table_directory = os.path.join(directory, 'table')

sql_opn = """
SELECT 
          eo.entity AS entity
         ,eo.operation AS operation
         ,Replace(Replace(Replace(Replace(Replace(Replace(o.oper_short_desc,',',';'),chr(9),' '),chr(10),' '),chr(13),' '),chr(34),''''),chr(7),' ') AS oper_short_desc
         ,Replace(Replace(Replace(Replace(Replace(Replace(o.oper_long_desc,',',';'),chr(9),' '),chr(10),' '),chr(13),' '),chr(34),''''),chr(7),' ') AS oper_long_desc
         ,o.dotprocess AS dotprocess
         ,o.cu_flag AS cu_flag
         ,o.cudesegeqpallowed AS cudesegeqpallowed
         ,o.rework_allowed AS rework_allowed
         ,o.module AS module
         ,eo.latest_flag AS entity_oper_latest_flag
FROM 
F_EntityOper eo
LEFT JOIN F_Operation O ON o.operation=eo.operation AND o.facility = eo.facility AND o.latest_version = 'Y'
WHERE
O.OPER_SHORT_DESC like 'L%' and (eo.ENTITY LIKE  'CAN%'
  OR eo.entity LIKE 'CIX%'
  OR eo.entity LIKE 'CNP%'
  OR eo.entity LIKE 'SBH%'
  OR eo.entity LIKE 'SCJ%'
  OR eo.entity LIKE 'SDJ%'
  OR eo.entity LIKE 'SLM%'
  OR eo.entity LIKE 'SNI%'
  OR eo.entity LIKE 'SNR%'
  OR eo.entity LIKE 'SNQ%'
  OR eo.entity LIKE 'STA%'
  OR eo.entity LIKE 'STB%'
  OR eo.entity LIKE 'STG%'
  OR eo.entity LIKE 'TBC%'
  OR eo.entity LIKE 'TCP%'
  OR eo.entity LIKE 'TCV%'
  OR eo.entity LIKE 'TCX%'
  OR eo.entity LIKE 'TNI%'
  OR eo.entity LIKE 'TTK%'
  OR eo.entity LIKE 'TZH%'
  OR eo.entity LIKE 'UTX%'
  )
    AND      eo.latest_flag = 'Y'
   and (o.dotprocess like '%1243%' or o.dotprocess like '%1217%' or o.dotprocess like '%1225%' or o.dotprocess like '%1226%' or o.dotprocess like '%5051%')
"""

sql_AM = """
SELECT AM_LDR_PATH, AM_LDR_MODELNAME, ENTITY, ROUTE, OPERATION, PRODUCT, PARAMETER_LIST FROM F_AM_F3
WHERE AM_LDR_PATH like '%Litho%' and PARAMETER_LIST like '%TRACK_RECIPE%'
and (AM_LDR_MODELNAME like '%ASML%' or AM_LDR_MODELNAME like '%Nikon%' or AM_LDR_MODELNAME like '%CNP%' or AM_LDR_MODELNAME like '%TZH%' or AM_LDR_MODELNAME like '%UTX%')
"""

sql_LCA = """
select  
    ENTITY AS entity
    ,OPERATION AS operation
    ,PRODUCT AS PRODUCT
    ,ROUTE AS ROUTE
    ,RETICLE AS RETICLE
    ,UPDATE_DATE AS UPDATE_DATE
    ,COMMENTS AS COMMENTS
    ,APF_FLAG AS FLAG
    ,OVER_LAYER AS OVER_LAYER
from F_AM_CA
"""

sql_LOCKOUT_HIST = """
select MODEL_PATH, REF_ROW_ID as ROW_ID, DATETIME, ACTION, USER_NAME, ROW_PROCESS, LOAD_DATE, TABLE_NAME from F_AM_HISTORY_CONFIG WHERE TABLE_NAME LIKE '%LOCKOUT%' 
AND MODEL_PATH Not Like '%CDSEM%' and MODEL_PATH not like '%Registration%'
AND ACTION LIKE '%delete%'
"""

sql_LOCKOUT = """
SELECT AM_LDR_PATH, ROW_ID, ENTITY, ROUTE, OPERATION, PRODUCT, COMMENTS, PARAMETER_LIST, LOAD_DATE FROM F_AM_F3 WHERE AM_LDR_OBJECTNAME LIKE '%LOCKOUT%'
AND AM_LDR_PATH Not Like '%CDSEM%' and AM_LDR_PATH not like '%Registration%'
"""

try:
    conn = PyUber.connect(datasource='F21_PROD_XEUS')
    df_opn = pd.read_sql(sql_opn, conn)
    df_resistAM = pd.read_sql(sql_AM, conn)
    df_LCA = pd.read_sql(sql_LCA, conn)     # LCA table
    df_LOCKOUT_HIST_DELETED = pd.read_sql(sql_LOCKOUT_HIST, conn)
    df_lo = pd.read_sql(sql_LOCKOUT, conn)  # Lockout table
except:
    print('Cannot run SQL script - Consider connecting to VPN')

vendord = {
    'CIX': 'UTX',
    'CNP': 'CNP',
    'UTX': 'UTX',
    'SCJ': 'ASML',
    'SBH': 'ASML',
    'SDJ': 'ASML',
    'STA': 'Nikon',
    'STB': 'Nikon',
    'STG': 'Nikon'
}

df_master = df_resistAM
df_master = df_master[df_master['OPERATION'] != "*"]
df_master['OPERATION'] = df_master['OPERATION'].str.split('|')
df_master = df_master.explode('OPERATION')  
df_master['ENTITY_OPN'] = None
df_master['TRACK_RECIPE'] = None
df_master['RESIST'] = None
df_master['CHEMICALS'] = None
df_master['OPER_SHORT_DESC'] = None
df_master['OPER_LONG_DESC'] = None
df_master['DOTPROCESS'] = None
df_master['CU_FLAG'] = None
df_master['CUDESEGEQPALLOWED'] = None
df_master['REWORK_ALLOWED'] = None
df_master['MODULE'] = None

def extract_value(parameter_list, key):
    pattern = rf"{key}=([^;]+)"
    match = re.search(pattern, parameter_list)
    return match.group(1) if match else None

# Extract values from PARAMETER_LIST
for index, row in df_master.iterrows():
    parameter_list = row['PARAMETER_LIST']
    df_master.loc[index, 'TRACK_RECIPE'] = extract_value(parameter_list, 'TRACK_RECIPE')
    df_master.loc[index, 'RESIST'] = extract_value(parameter_list, 'RESIST')
    df_master.loc[index, 'CHEMICALS'] = extract_value(parameter_list, 'CHEMICALS')

# Function to match entities with wildcards
def wildcard_match(entity_master, entity_opn):
    pattern = re.escape(entity_master).replace(r'\*', '.*')
    return re.fullmatch(pattern, entity_opn) is not None

# List to hold new rows
new_rows = []

# Iterate through each row in df_opn
for opn_index, opn_row in df_opn.iterrows():
    matching_rows = df_master[df_master['OPERATION'] == opn_row['OPERATION']]
    for master_index, master_row in matching_rows.iterrows():
        if wildcard_match(master_row['ENTITY'], opn_row['ENTITY']):
            entity_prefix = opn_row['ENTITY'][:3]
            vendor_value = vendord.get(entity_prefix)
            if vendor_value and vendor_value in master_row['AM_LDR_MODELNAME']:
                # Duplicate the matching row
                new_row = master_row.copy()
                new_row['ENTITY_OPN'] = opn_row['ENTITY']
                new_row['OPER_SHORT_DESC'] = opn_row['OPER_SHORT_DESC']
                new_row['OPER_LONG_DESC'] = opn_row['OPER_LONG_DESC']
                new_row['DOTPROCESS'] = opn_row['DOTPROCESS']
                new_row['CU_FLAG'] = opn_row['CU_FLAG']
                new_row['CUDESEGEQPALLOWED'] = opn_row['CUDESEGEQPALLOWED']
                new_row['REWORK_ALLOWED'] = opn_row['REWORK_ALLOWED']
                new_row['MODULE'] = opn_row['MODULE']
                # Append the new row to the list
                new_rows.append(new_row)

# Concatenate the new rows to df_master
df_master = pd.concat([df_master, pd.DataFrame(new_rows)], ignore_index=True)

#Remove any duplicate rows that are not useful
# Identify duplicates based on OPERATION, ENTITY, and TRACK_RECIPE
duplicates = df_master[df_master.duplicated(subset=['OPERATION', 'ENTITY', 'TRACK_RECIPE'], keep=False)]
# Find rows with empty values for DOTPROCESS and ENTITY_OPN among the duplicates
empty_values = duplicates[(duplicates['DOTPROCESS'].isna()) & (duplicates['ENTITY_OPN'].isna())]
# Remove rows with empty values for DOTPROCESS and ENTITY_OPN
df_master = df_master.drop(empty_values.index)
# Remove rows where TRACK_RECIPE, CHEMICALS, and RESIST columns are all empty or None
df_master = df_master.dropna(subset=['TRACK_RECIPE', 'CHEMICALS', 'RESIST'], how='all')

#Sort and reorder columns
df_master = df_master.sort_values(by=['DOTPROCESS', 'OPERATION', 'ENTITY_OPN'])
# Move 'PARAMETER_LIST' column to the end
cols = [col for col in df_master.columns if col != 'PARAMETER_LIST'] + ['PARAMETER_LIST']
df_master = df_master[cols]
# Specify the new order of columns with important columns first
new_order = ['DOTPROCESS', 'OPER_SHORT_DESC', 'OPERATION', 'ENTITY_OPN', 'ENTITY', 'TRACK_RECIPE', 'CHEMICALS'] + \
            [col for col in df_master.columns if col not in ['DOTPROCESS', 'OPER_SHORT_DESC', 'OPERATION', 'ENTITY_OPN', 'ENTITY', 'TRACK_RECIPE', 'CHEMICALS']]
# Reorder the columns in df_master
df_master = df_master[new_order]

# output to excel
df_master = df_master.sort_values(by=['OPERATION'])
# Save the updated df_master to an Excel file
df_master.to_excel(f"{directory}/OPN-ENTITY-RCP-CHEM.xlsx", index=False)

# Create an HTML table with filterable columns (Product specific)
df_master = df_master.sort_values(by=['DOTPROCESS', 'OPERATION', 'ENTITY_OPN'])
html_table = df_master.to_html(index=False, classes='filterable')
# Define the JavaScript code
javascript = """
<script>
// Get all the headers
var headers = document.querySelectorAll('.filterable th');
// For each header
headers.forEach(function(header, index) {
    // Create a text box
    var textBox = document.createElement('input');
    // When something is typed in the text box
    textBox.onkeyup = function() {
        // Get the rows
        var rows = document.querySelectorAll('.filterable tbody tr');
        // For each row
        rows.forEach(function(row) {
            // If the text box is empty or its content is found in the corresponding cell
            if (textBox.value === '' || row.cells[index].textContent.includes(textBox.value)) {
                // Show the row
                row.style.display = '';
            } else {
                // Hide the row
                row.style.display = 'none';
            }
        });
    };
    // Add the text box to the header
    header.appendChild(textBox);
});
</script>
"""
# Add the JavaScript code to the HTML table
html_table += javascript

# Check if the directory exists
if os.path.exists(table_directory):
    # List files in the directory
    files = os.listdir(table_directory)
    # Remove each file
    for file in files:
        file_path = os.path.join(table_directory, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

# Write the HTML table to a file with retry logic
output_path = f"{directory}/table/OPN-ENTITY-RCP-CHEM.html"
max_retries = 5
retry_delay = 2  # seconds

for attempt in range(max_retries):
    try:
        with open(output_path, 'w') as f:
            f.write(html_table)
        print(f"File written successfully to {output_path}")
        break
    except PermissionError:
        print(f"PermissionError: Attempt {attempt + 1} of {max_retries}. Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)
else:
    print(f"Failed to write file after {max_retries} attempts. Please ensure the file is not open in another application.")

# Create a copy of df_master that is not PRODUCT specific and includes LOCKOUT information (which may have products)
df_no_prod = df_master.copy()
df_no_prod.drop(columns=['PRODUCT'], inplace=True)
df_no_prod.drop_duplicates(inplace=True)
df_no_prod['LOCKOUT'] = None
df_no_prod['LOCKOUT_COMMENT'] = None
df_no_prod['LOAD_DATE'] = None
df_no_prod['LOCKOUT_PRODUCT'] = None
df_no_prod['LOCKOUT_ROUTE'] = None

# Create lockout table and merge values with df_no_prod
# Ensure the column names are correct
df_LOCKOUT_HIST_DELETED.columns = df_LOCKOUT_HIST_DELETED.columns.str.upper()
df_lo.columns = df_lo.columns.str.upper()
# Remove rows in df_lo where the lockout history shows it was deleted
df_lo = df_lo[~df_lo['ROW_ID'].isin(df_LOCKOUT_HIST_DELETED['ROW_ID'])] # df_low should only contain current lockout entries (not previous deleted)
# Extract the value after 'LOCKOUT_STATE=' and before ';' from PARAMETER_LIST and put it in LOCKOUT
df_lo['LOCKOUT'] = df_lo['PARAMETER_LIST'].str.extract(r'LOCKOUT_STATE=([^;]+)')
# Extract the value after 'LOCKOUT_REASON=' and before ';' from PARAMETER_LIST
df_lo['LOCKOUT_REASON'] = df_lo['PARAMETER_LIST'].str.extract(r'LOCKOUT_REASON=([^;]+)')
# Concatenate the extracted LOCKOUT_REASON to the COMMENTS column
df_lo['COMMENTS'] = df_lo.apply(lambda row: f"{row['COMMENTS']} - {row['LOCKOUT_REASON']}" if pd.notnull(row['LOCKOUT_REASON']) else row['COMMENTS'], axis=1)
# Drop the temporary LOCKOUT_REASON column
df_lo.drop(columns=['LOCKOUT_REASON'], inplace=True)
# Split the OPERATION column by '|' and explode the DataFrame to create duplicate rows
df_lo['OPERATION'] = df_lo['OPERATION'].str.split('|') # the operation column becomes a 'list' of operations
df_lo = df_lo.explode('OPERATION') # this will create a new row for each operation in the list
df_lo.rename(columns={'ENTITY': 'ENTITY_OPN'}, inplace=True)
# Function to check for wildcard matches
def wildcard_match(entity_opn, pattern):
    if entity_opn is None or pattern is None:
        return False
    # Replace '*' with '.*' to create a regex pattern
    regex_pattern = re.escape(pattern).replace(r'\*', '.*')
    return re.fullmatch(regex_pattern, entity_opn) is not None

# Iterate through df_no_prod and check for matches in df_lo
for idx, row in df_no_prod.iterrows():
    entity_opn = row['ENTITY_OPN']
    operation = row['OPERATION']
    AMpath = row['AM_LDR_PATH']
    
    # Find matching rows in df_lo
    matches = df_lo[(df_lo['OPERATION'] == operation) & 
                    (df_lo['ENTITY_OPN'].apply(lambda x: wildcard_match(entity_opn, x))) &
                    (df_lo['AM_LDR_PATH'] == AMpath)]
    
    # If matches are found, update df_no_prod
    if not matches.empty:
        df_no_prod.at[idx, 'LOCKOUT'] = '; '.join(matches['LOCKOUT'].dropna().astype(str).unique())
        df_no_prod.at[idx, 'LOCKOUT_COMMENT'] = '; '.join(matches['COMMENTS'].dropna().astype(str).unique())
        df_no_prod.at[idx, 'LOAD_DATE'] = '; '.join(matches['LOAD_DATE'].dropna().astype(str).unique())
        df_no_prod.at[idx, 'LOCKOUT_PRODUCT'] = '; '.join(matches['PRODUCT'].dropna().astype(str).unique())
        df_no_prod.at[idx, 'LOCKOUT_ROUTE'] = '; '.join(matches['ROUTE'].dropna().astype(str).unique())

# Define the desired column order
desired_order = [
    'DOTPROCESS', 'OPER_SHORT_DESC', 'OPERATION', 'ENTITY_OPN', 'ENTITY', 
    'TRACK_RECIPE', 'CHEMICALS', 'LOCKOUT', 'LOCKOUT_COMMENT', 'LOAD_DATE', 
    'LOCKOUT_PRODUCT', 'LOCKOUT_ROUTE'
]
# Get the remaining columns
remaining_columns = [col for col in df_no_prod.columns if col not in desired_order]
# Reorder the columns
df_no_prod = df_no_prod[desired_order + remaining_columns]

df_no_prod.to_excel(f"{directory}/OPN-ENTITY-no-prod-LOCKOUT.xlsx", index=False)

# Create an HTML table with filterable columns (NOT Product specific)
df_no_prod = df_no_prod.sort_values(by=['DOTPROCESS', 'OPERATION', 'ENTITY_OPN'])
html_table = df_no_prod.to_html(index=False, classes='filterable')
# Define the JavaScript code
javascript = """
<script>
// Get all the headers
var headers = document.querySelectorAll('.filterable th');
// For each header
headers.forEach(function(header, index) {
    // Create a text box
    var textBox = document.createElement('input');
    // When something is typed in the text box
    textBox.onkeyup = function() {
        // Get the rows
        var rows = document.querySelectorAll('.filterable tbody tr');
        // For each row
        rows.forEach(function(row) {
            // If the text box is empty or its content is found in the corresponding cell
            if (textBox.value === '' || row.cells[index].textContent.includes(textBox.value)) {
                // Show the row
                row.style.display = '';
            } else {
                // Hide the row
                row.style.display = 'none';
            }
        });
    };
    // Add the text box to the header
    header.appendChild(textBox);
});
</script>
"""
# Add the JavaScript code to the HTML table
html_table += javascript

# Write the HTML table to a file with retry logic
output_path = f"{directory}/table/OPN-ENTITY-no-product-LOCKOUT.html"
max_retries = 5
retry_delay = 2  # seconds

for attempt in range(max_retries):
    try:
        with open(output_path, 'w') as f:
            f.write(html_table)
        print(f"File written successfully to {output_path}")
        break
    except PermissionError:
        print(f"PermissionError: Attempt {attempt + 1} of {max_retries}. Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)
else:
    print(f"Failed to write file after {max_retries} attempts. Please ensure the file is not open in another application.")


# LCA table setup
df_LCA.rename(columns={'ENTITY': 'ENTITY_OPN'}, inplace=True)

# Print column names for debugging
print("df_no_prod columns:", df_no_prod.columns)
print("df_LCA columns:", df_LCA.columns)

# Merge df_LCA with df_no_prod on ENTITY_OPN and OPERATION
merged_df = df_LCA.merge(df_no_prod[['ENTITY_OPN', 'OPERATION', 'TRACK_RECIPE', 'RESIST', 'CHEMICALS']],
                         on=['ENTITY_OPN', 'OPERATION'], how='left')

print("df_merged columns:", merged_df.columns)
# Update df_LCA with the merged values
df_LCA['TRACK_RECIPE'] = merged_df['TRACK_RECIPE']
df_LCA['RESIST'] = merged_df['RESIST']
df_LCA['CHEMICALS'] = merged_df['CHEMICALS']

df_LCA.to_excel(f"{directory}/OPN-ENTITY-Resist-LCA.xlsx", index=False)
