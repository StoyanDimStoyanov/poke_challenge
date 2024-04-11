import logging


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
        pass

    def transform(self):
        logging.info('transform')
        pass

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