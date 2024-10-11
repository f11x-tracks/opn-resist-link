import pandas as pd
import PyUber
import csv, re
from datetime import datetime
import os

directory = f'//f21pucnasn1.f21prod.mfg.intel.com/FuzionUploads/Litho/Tracks/dash-OPN-ENTITY-RCP-CHEM/'

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

try:
    conn = PyUber.connect(datasource='F21_PROD_XEUS')
    df_opn = pd.read_sql(sql_opn, conn)
    df_resistAM = pd.read_sql(sql_AM, conn)
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

df_master = df_master.sort_values(by=['DOTPROCESS', 'OPERATION', 'ENTITY_OPN'])
# Move 'PARAMETER_LIST' column to the end
cols = [col for col in df_master.columns if col != 'PARAMETER_LIST'] + ['PARAMETER_LIST']
df_master = df_master[cols]
# Specify the new order of columns with important columns first
new_order = ['DOTPROCESS', 'OPER_SHORT_DESC', 'OPERATION', 'ENTITY_OPN', 'ENTITY', 'TRACK_RECIPE', 'CHEMICALS'] + \
            [col for col in df_master.columns if col not in ['DOTPROCESS', 'OPER_SHORT_DESC', 'OPERATION', 'ENTITY_OPN', 'ENTITY', 'TRACK_RECIPE', 'CHEMICALS']]

# Reorder the columns in df_master
df_master = df_master[new_order]


# Save the updated df_master to an Excel file
df_master.to_excel(f"{directory}/OPN-ENTITY-RCP-CHEM.xlsx", index=False)

# Create an HTML table with filterable columns
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