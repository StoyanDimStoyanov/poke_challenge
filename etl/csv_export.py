import os
import sqlite3
import csv


# Check if the database file exists
if not os.path.exists('pokemon_data.db'):
    raise FileNotFoundError("Database file 'pokemon_data.db' not found.")

# Connect to the SQLite database
conn = sqlite3.connect('pokemon_data.db')
c = conn.cursor()

# Query the data from the database
c.execute("SELECT * FROM pokemon_data")
data = c.fetchall()

# Close the connection
conn.close()

# Write the data to a CSV file
with open('exported_data.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow([i[0] for i in c.description])  # Write column headers
    csv_writer.writerows(data)

print("Data exported to 'exported_data.csv' successfully.")
