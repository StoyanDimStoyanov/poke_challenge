import os
import json
import logging
import requests
import apache_beam as beam
import sqlite3
import ast

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


class PokeEtl:
    def __init__(self):
        self.pokemon_api_url = "https://pokeapi.co/api/v2/type/3"
        self.extracted_data = []
        self.transformed_data = []
        self.sqlite3_file_name = "pokemon_data.db"
        self.data_file_path = "poke_data.json"

    def extract(self):
        """get the pokemon data from the source api"""
        logging.info('extract')
        try:
            response = requests.get(self.pokemon_api_url)
            if response.status_code != 200:
                raise Exception("")

            data = response.json()
            pokemons_list = data['pokemon'][:50]
            for pokemon in pokemons_list:
                pokemon_url = pokemon['pokemon']['url']
                pokemon_data = requests.get(pokemon_url).json()
                details = {
                    'id': pokemon_data['id'],
                    'name': pokemon_data['name'],
                    'height': pokemon_data['height'],
                    'weight': pokemon_data['weight']
                }
                self.extracted_data.append(details)
                logging.info(details)

            # create json file with Pokémon details
            with open(self.data_file_path, 'w') as json_file:
                for obj in self.extracted_data:
                    json.dump(obj, json_file)
                    json_file.write('\n')

        except Exception as e:
            logging.info("Error occurred during data extraction:", e)

    def transform(self):
        """
        Transform the extracted data using Apache Beam
        """

        def calculate_bmi(obj):
            obj["bmi"] = round(obj["weight"] / (obj["height"] ** 2), 2)
            return obj

        def transform_weight_height(obj):
            obj["weight"] /= 1000
            obj["height"] /= 100
            return obj

        if not os.path.exists(self.data_file_path):
            raise ValueError('Pokemon json file not found')

        output_file_path = "transformed_data.json"  # Define the output file path

        with beam.Pipeline() as pipeline:
            transformed_data = (
                pipeline
                | beam.io.ReadFromText(self.data_file_path)
                | beam.Map(json.loads)
                | beam.Map(transform_weight_height)
                | beam.Map(calculate_bmi)
                | beam.io.WriteToText(output_file_path, shard_name_template='')  # Write transformed data to a json file
            )

        # Read transformed data from the text file and convert it to a list
        with open(output_file_path, 'r') as file:
            self.transformed_data = [ast.literal_eval(line) for line in file]

        # Delete the temporary output files
        os.remove(output_file_path)
        os.remove(self.data_file_path)

    def load(self):
        """
        Load the transformed data into an SQLite database
        """
        sql_query_values = []

        try:
            conn = sqlite3.connect(self.sqlite3_file_name)
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS pokemon_data (
                            id INTEGER PRIMARY KEY,
                            name TEXT,
                            height REAL,
                            weight REAL,
                            bmi REAL
                        )''')
            conn.commit()

            # Retrieve existing IDs from the database
            c.execute("SELECT id FROM pokemon_data")
            existing_ids = set(row[0] for row in c.fetchall())

            all_bmi = []

            for pokemon in self.transformed_data:
                logging.info(pokemon)
                if pokemon['id'] not in existing_ids:
                    all_bmi.append(pokemon['bmi'])
                    sql_query_values.append(
                        f"""({pokemon['id']}, '{pokemon['name']}', {pokemon['height']}, {pokemon['weight']}, 
                        {pokemon['bmi']})""")

            # If no new records need to be inserted, return early
            if not sql_query_values:
                logging.info("No new data to insert into the database")
                return

            logging.info(f'The average BMI of the pokemons is {round(sum(all_bmi) / len(all_bmi), 2)}')
            sorted_list = sorted(self.transformed_data, key=lambda x: x['bmi'])
            max_bmi_pokemon = sorted_list[-1]
            logging.info(f"The pokemon with higher BMI is {max_bmi_pokemon['name']} with BMI: {max_bmi_pokemon['bmi']}")

            # execute multi row insert
            q = f"""INSERT INTO pokemon_data (id, name, height, weight, bmi) 
                        VALUES {', '.join(sql_query_values)} """

            c.execute(q)
            conn.commit()
            conn.close()
            logging.info(f"{len(self.extracted_data) - len(existing_ids)} objects loaded into SQLite database "
                         f"successfully")
        except Exception as e:
            logging.error("Error occurred during data loading:", e)

    def execute_pipeline(self):
        self.extract()
        self.transform()
        self.load()


def main():
    pokemon_etl = PokeEtl()
    pokemon_etl.execute_pipeline()


if __name__ == "__main__":
    main()
