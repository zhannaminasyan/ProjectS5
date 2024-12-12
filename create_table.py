import os
from astropy.io import fits
import pymysql
import re

def is_fits_file(filename):
    return filename.lower().endswith('.fits')

def get_id(filename):
    match = re.search(r'fbs(\d+)_cor\.fits', filename)
    if match:
        return int(match.group(1))
    return None

def get_columns(fits_file):
    with fits.open(fits_file) as hdul:
        header = hdul[0].header
        
        keyword_data_types = []
        
        for key in header.keys():
            value = header[key]
            keyword_data_types.append((key, type(value).__name__))
            
    return keyword_data_types


mydb = pymysql.connect(
    host='localhost',
    user='root',
    password='1234',  
    database='univ'  
)
mycursor = mydb.cursor()
# directory_path = r'E:\\'
directory_path = r'C:\Users\Zhanna\Desktop\bao\data'

fits_file = os.path.join(directory_path, 'fbs0005_cor.fits')


cols = [(key, dtype) for key, dtype in get_columns(fits_file) if dtype != "_HeaderCommentaryCards"]
cols = [(key, 'VARCHAR(255)' if dtype == 'str' else 'DECIMAL(10,5)' if dtype == 'float' else 'INT' if dtype == 'int' else 'VARCHAR(255)') for key, dtype in cols]

sql = "CREATE TABLE IF NOT EXISTS header_data (ID INT AUTO_INCREMENT PRIMARY KEY"
for key, dtype in cols:
    sql += f", `{key}` {dtype}"
sql += ");"


mycursor.execute(sql)
mydb.commit()


for filename in os.listdir(directory_path):
    if is_fits_file(filename) and len(filename) == 16:
        file_path = os.path.join(directory_path, filename)

        with fits.open(file_path) as file:
            # SQL for inserting data
            sql = "INSERT INTO header_data (ID"
            values = f"VALUES ({get_id(file_path)}"

            for key, dtype in cols:
                try:
                    if dtype.startswith("VARCHAR"):
                        sql += f", `{key}`"
                        values += f", '{str(file[0].header[key]).replace('\'', '\'\'')}'"
                    else:
                        sql += f", `{key}`"
                        values += f", {file[0].header[key]}"
                except Exception as e:
                    sql += f", `{key}`"
                    values += ", NULL"

            sql += ") " + values + ");"
            mycursor.execute(sql)
            mydb.commit()

mycursor.close()
mydb.close()
