import requests
import json
import time
import math
import re
import pandas as pd

from pathlib import Path
from datetime import date

BASE_PATH = Path(__file__).parent

class WebScraper():

    def __init__(self, apiKey = 'd5a45936c03454b7e98aa0a4f9e45e4a', enable_dollar_conversion = True) -> None:
        
        self.apiKey = apiKey
        self.country_to_ext = {"malasya": "com.my", "singapore": "sg", "vietnam": "vn", "thailand": "co.th", "indonesia": "co.id"}
        self.country_to_curr = {"malasya": "MYR", "singapore": "SGD", "vietnam": "VND", "thailand": "THB", "indonesia": "IDR"}
        self.enable_dollar_conversion = enable_dollar_conversion
        if self.enable_dollar_conversion:
            with open(BASE_PATH/"../utils/exchange_rates.json", "r") as f:
                self.conversion_rates = json.load(f)

    @staticmethod
    def all_finished(statuses):

        return statuses == ['finished']*len(statuses)

    @staticmethod
    def find_ind_not_finished(statuses):

        inds = []
        for i, status in enumerate(statuses):
            if status != 'finished':
                inds.append(i)
        return inds

    def update_conversion_rate(self):
        base_url = "https://fxds-public-exchange-rates-api.oanda.com/cc-api/currencies?base=[CURR]&quote=USD&data_type=general_currency_pair&start_date=2023-02-12&end_date=2023-02-13"
        r = requests.post(url = 'https://async.scraperapi.com/batchjobs', json={ 'apiKey': self.apiKey, 'urls': [base_url.replace("[CURR]", curr) for curr in self.country_to_curr.values()]})
        time.sleep(3)
        responses = [requests.get(url = res["statusUrl"]) for res in r.json()]
        statuses = [resp.json()["status"] for resp in responses]
        i = 0
        while i < 10 and not WebScraper.all_finished(statuses):
            i += 1
            time.sleep(2)
            inds_to_rerun = WebScraper.find_ind_not_finished(statuses)
            for ind in inds_to_rerun:
                responses[ind] = requests.get(url = r.json()[ind]["statusUrl"])
            statuses = [resp.json()["status"] for resp in responses]
        assert(i != 10), "oops, it doesn't seem to work for now, please try again later."
        values = [resp.json()["response"]["body"]["response"][0]["average_bid"] for resp in responses]
        self.conversion_rates = {key: value for key, value in zip(self.country_to_curr.keys(), values)}
        with open(BASE_PATH/"../utils/exchange_rates.json", "w") as f:
            json.dump(self.conversion_rates, f)

    def get_seller_data(self, shopid, country, save_raw = True, save_parsed = True, filter_sold_out=0, chunk_size=30):
        
        assert(country.lower() in ["malasya", "singapore", "vietnam", "thailand", "indonesia"]), 'The provided country should be one of the following countries : "Malasya", "Singapore", "Vietnam", "Thailand" or "Indonesia".'
        assert(type(shopid)==int), f"The shopid should be an integer. you provided a value of type {type(shopid)}."

        base_url = f'https://shopee.{self.country_to_ext[country.lower()]}/api/v4/shop/search_items?filter_sold_out={filter_sold_out}&limit={chunk_size}&offset=0&order=desc&shopid={shopid}&sort_by=pop&use_case=4'
        print(base_url)

        r = requests.post(url = 'https://async.scraperapi.com/jobs', json={ 'apiKey': self.apiKey, 'url': base_url})
        time.sleep(3)
        response = requests.get(url = r.json()["statusUrl"])
        status = response.json()["status"]
        i = 0
        while i < 12 and status != 'finished':
            i += 1
            time.sleep(5)
            response = requests.get(url = r.json()["statusUrl"])
            status = response.json()["status"]
        
        assert(status=='finished'), "an error has occured, please try again later"

        item_count = response.json()["response"]["body"]["total_count"]
        print(f"{item_count} items detected. Fetching all data")

        chunk_number = math.ceil(item_count/chunk_size)

        if chunk_number > 1:
            r = requests.post(url = 'https://async.scraperapi.com/batchjobs', json={ 'apiKey': self.apiKey, 'urls': [base_url.replace("offset=0", f"offset={ind*chunk_size}") for ind in range(1, chunk_number)]})
            time.sleep(3)
            responses = [requests.get(url = res["statusUrl"]) for res in r.json()]
            statuses = [resp.json()["status"] for resp in responses]
            i = 0
            while i < 12 and not WebScraper.all_finished(statuses):
                i += 1
                time.sleep(5)
                inds_to_rerun = WebScraper.find_ind_not_finished(statuses)
                for ind in inds_to_rerun:
                    responses[ind] = requests.get(url = r.json()[ind]["statusUrl"])
                statuses = [resp.json()["status"] for resp in responses]
            responses = [response] + responses
            if WebScraper.all_finished(statuses):
                print("extraction was succesfull!")
        else:
            print("extraction was succesfull!")
            responses = [response]

        items_list_raw = [resp.json()["response"]["body"]["items"] for resp in responses]
        items_list_raw = [item for item_list in items_list_raw for item in item_list]
        del(responses)
        
        if save_raw:
            base_path_raw = BASE_PATH/"../data/raw/"
            base_path_raw.mkdir(exist_ok=True)
            current_date = str(date.today())
            with open(base_path_raw/f"all-items-raw-shopid-{shopid}-country-{country}-{current_date}.json", "w") as f:
                json.dump(items_list_raw, f)

        if save_parsed:
            base_path_parsed =BASE_PATH/"../data/parsed/"
            base_path_parsed.mkdir(exist_ok=True)

            data = []
            if self.enable_dollar_conversion:
                rate = float(self.conversion_rates[country])
                columns = ["Name", f"Price ({self.country_to_curr[country]})", "Price (USD)"]
                while items_list_raw:
                    item = items_list_raw.pop(0)
                    data.append((re.sub(r"(\[(.)+\])*?", "", item["item_basic"]["name"]).strip(), "{:n}".format(item["item_basic"]["price"]/100000), "{:.2f}".format(rate*item["item_basic"]["price"]/100000)))
            else:
                columns = ["Name", f"Price ({self.country_to_curr[country]})"]
                while items_list_raw:
                    item = items_list_raw.pop(0)
                    data.append((re.sub(r"(\[(.)+\])*?", "", item["item_basic"]["name"]).strip(), item["item_basic"]["price"]))

            df = pd.DataFrame(data=data, columns=columns)
            df.to_csv(base_path_parsed/f"all-items-parsed-shopid-{shopid}-country-{country}-{current_date}.csv", index=False)