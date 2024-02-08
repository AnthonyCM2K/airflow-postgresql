import requests
import json
import datetime

DATE=str(datetime.date.today()).replace('-','')

def get_most_relevant_items_for_category(category):
    url = f"https://api.mercadolibre.com/sites/MLA/search?category={category}#json"
    response = requests.get(url).text
    response = json.loads(response)
    data = response["results"]
    print(data)

    #Abrimos un arichivo .tsv para que lo lea postgres
    with open('./AIRFLOW-DOCKER/tmp/file.tsv', 'w') as file: #colocar toda la ruta para crear el archivo en /tmp/
        #Filtrando los campos requeridos
        for item in data:
            _id = getKeyFromItem(item, 'id')
            site_id = getKeyFromItem(item, 'site_id')
            title = getKeyFromItem(item, 'title')
            price = getKeyFromItem(item, 'price')
            sold_quantily = getKeyFromItem(item, 'sold_quantity')
            thumbnail = getKeyFromItem(item, 'thumbnail')

            file.write(f"{_id}\t{site_id}\t{title}\t{price}\t{sold_quantily}\t{thumbnail}\n")

def getKeyFromItem(item, key):
    return str(item[key]).replace(' ','').strip() if item.get(key) else "null"


def main():
    CATEGORY = "MLA1577"
    get_most_relevant_items_for_category(CATEGORY)

main()

