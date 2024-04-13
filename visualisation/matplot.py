import os
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns


# Check if the database file exists
if not os.path.exists('../etl/pokemon_data.db'):
    raise FileNotFoundError("Database file 'pokemon_data.db' not found.")

# Connect to the SQLite database
conn = sqlite3.connect('../etl/pokemon_data.db')
c = conn.cursor()

# Query the data from the database including BMI
c.execute("SELECT height, weight, bmi FROM pokemon_data")
data = c.fetchall()

# Close the connection
conn.close()

# Unpack the data
heights, weights, bmi = zip(*data)

# Create scatter plot
plt.figure(figsize=(10, 5))
plt.scatter(heights, weights, color='blue', alpha=0.5)
plt.title('Scatter Plot of Height vs Weight')
plt.xlabel('Height (m)')
plt.ylabel('Weight (kg)')
plt.grid(True)
plt.show()

# Create histogram of BMI values
plt.figure(figsize=(8, 5))
sns.histplot(bmi, bins=20, kde=True, color='green')
plt.title('Histogram of BMI Values')
plt.xlabel('BMI')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()
