import pandas as pd
import pyodbc

file_path = 'samgongustofa-slys.csv'

# Read CSV file with headers on row 3 (index 2), data starts on row 4
df = pd.read_csv(file_path, sep=';', header=2)

# Get data as list of rows
data = df.values.tolist()

sum_of_values = 0
sum_of_rows = 0

# Count the number of accidents in a month per year
years = {}
for i in data:
    day, month, year = i[0].split(".")
    if (year, month) in years.keys():
        years[(year, month)] += 1
    else:
        years[(year, month)] = 1
        sum_of_rows += 1
    sum_of_values += 1

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

for (year, month), count in years.items():
    # Create date string in format YYYY-MM
    if (month == "10" or month == "11" or month == "12"):
        year_month_date = f"{year}-{month}"
    else:
        year_month_date = f"{year}-0{month}"
    
    # Insert into database
    # cursor.execute(
    #     'INSERT INTO Accidents (year_month, accident_count) VALUES (?, ?)',
    #     (year_month_date, count)
    # )

# Commit the transaction
cnxn.commit()

# Disconnect
cursor.close()
cnxn.close()