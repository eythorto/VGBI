import pandas as pd
import pyodbc

file_path = 'samgongustofa-turistar.csv'

# Read CSV file with headers on row 3 (index 2), data starts on row 4 (index 3)
# Column A (index 0) contains row labels
df = pd.read_csv(file_path, sep=';', header=2, index_col=0)

# Store data in dictionary: {(year, month): tourist_count}
tourists_data = {}

# Get the first data row (row 4, which is index 0 after reading with header=2)
first_data_row = df.iloc[0]

sum_of_values = 0
sum_of_rows = 0

# Iterate through each column (which represents year-month)
for col_name in df.columns:
    value = first_data_row[col_name]
    # Try to extract year and month from column name
    parts = col_name.strip().split("M")
    year, month = parts[0], parts[1]
    if (year == "2025"):
        continue
    tourists_data[(year, month)] = value
    sum_of_values += value
    sum_of_rows += 1
    
# Connection information
db_username = "vgbiUser"
db_password = "myUserPassword2026!is"
db_server = "ruvgbi2026.database.windows.net"
db_name = "vgbi"

odbc_driver = "ODBC Driver 18 for SQL Server"

conn_str_template = 'Driver={driver};Server={dbserver};Database={db};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'
conn_str = conn_str_template.format(driver=odbc_driver, username=db_username, password=db_password, dbserver=db_server, db=db_name)

# Connect to Database
cnxn = pyodbc.connect(conn_str, autocommit=False)
cursor = cnxn.cursor()

print(f"The sum of all values is: {sum_of_values}")
print(f"The number of rows are: {sum_of_rows}")
print(f"The avg is: {sum_of_values / sum_of_rows}")
for (year, month), count in tourists_data.items():
    if year == "2025":
        continue
    
    # Convert month to integer and validate
    try:
        month_int = int(month)
        if month_int < 1 or month_int > 12:
            print(f"Skipping invalid month: {year}-{month}")
            continue
    except ValueError:
        print(f"Skipping invalid month value: {year}-{month}")
        continue
    
    # Format as YYYY-MM with zero-padding
    year_month_date = f"{year}-{month_int:02d}"
    
    # Insert into database
    # cursor.execute(
    #     'INSERT INTO Tourists (year_month, tourist_count) VALUES (?, ?)',
    #     (year_month_date, count)
    # )

# Commit the transaction
cnxn.commit()

# Disconnect
cursor.close()
cnxn.close()