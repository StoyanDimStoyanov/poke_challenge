# poke_challenge

* click on th green 'play' sign to install the requirements or execute the line in the terminal
`pip install -r requirements.txt`

* when the `main.py` file is executed the SQLite3 database file will be created automatically, if such is not found
* the first 50 pokemons will be extracted, transformed and loaded to the DB, in case some of the records are already
in the DB they will not be loaded

`csv_export.py` will generate csv file, which can be used with sites https://www.csvplot.com/ or https://statscharts.com/bar/histogram
to create scatter and histogram

* `matplot.py` can be used for visualisation if the IDE has plugin or scientific mode.