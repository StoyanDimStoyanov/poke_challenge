import logging
import requests
import json
import apache_beam as beam

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)




class PokeEtl:
    def __init__(self):
        self.pokemon_api_url = "https://pokeapi.co/api/v2/type/3"
        self.extracted_data = []
        self.transformed_data = []
        self.sqlite3_file_name = "pokemon_data.db"


    def extract(self):
        "get the pokemon data from the source api"
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
                print(details)

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

        with beam.Pipeline() as pipeline:
            transformed_data = (
                pipeline
                | beam.Create(self.extracted_data)
                | beam.Map(transform_weight_height)
                | beam.Map(calculate_bmi)
                | beam.Map(print)
            )

    def load(self):
        logging.info('load')
        pass


    def execute_pipeline(self):
        self.extract()
        self.transform()
        self.load()

def main():
    pokemon_etl = PokeEtl()
    pokemon_etl.execute_pipeline()

if __name__ == "__main__":
    main()