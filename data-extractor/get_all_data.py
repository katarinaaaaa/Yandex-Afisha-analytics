import requests
import json
import time
import datetime
import os
import math
import sys
import shutil

MAX_SET = 20
START_OFFSET = 0


def get_cities_file():
    cities = requests.get("https://afisha.yandex.ru/api/cities?city=saint-petersburg")
    if os.path.exists("storage/raw_data"):
        shutil.rmtree("storage/raw_data")
    os.mkdir("storage/raw_data")
    with open('storage/raw_data/cities.json', 'w', encoding="utf-8") as file:
        json.dump(cities.json(), file, ensure_ascii=False)


def get_url_date(date_obj):
    # yyyy-mm-dd
    date = date_obj.strftime("%Y") + "-" + date_obj.strftime("%m") + "-" + date_obj.strftime("%d")
    return date


def get_str_date(date_obj):
    # dd-mm-yyyy
    date = date_obj.strftime("%d") + "_" + date_obj.strftime("%m") + "_" + date_obj.strftime("%Y")
    return date


def get_day_events(date, city):
    url_date = get_url_date(date)

    # get total number of day events
    first = requests.get(
        f"https://afisha.yandex.ru/api/events/rubric/main?limit={MAX_SET}&offset={START_OFFSET}"
        f"&hasMixed=0&date={url_date}&period=1&city={city}"
    )

    if first.status_code != 200:
        time.sleep(30)
        return get_day_events(date, city)

    first_dict = first.json()
    total = first_dict["paging"]["total"]
    num_of_pages = math.ceil(total / MAX_SET)

    # get all day events
    data = []
    offsets = range(MAX_SET, MAX_SET * num_of_pages, MAX_SET)
    for offset in offsets:
        more = requests.get(
            f"https://afisha.yandex.ru/api/events/rubric/main?limit={MAX_SET}&offset={offset}"
            f"&hasMixed=0&date={url_date}&period=1&city={city}"
        )

        if more.status_code != 200:
            print(f"{city}, {date}: more failed")
            time.sleep(30)
            return get_day_events(date, city)

        cur_data = more.json()["data"]
        data = data + cur_data
        time.sleep(3)

    date_str = get_str_date(date)

    # after reading all the pages, we create the final json
    new_dict = {'date': date_str, 'total': first_dict["paging"]["total"], 'data': first_dict["data"] + data}

    with open(f'storage/raw_data/{city}/{city}_{date_str}.json', 'w', encoding="utf-8") as f:
        json.dump(new_dict, f, ensure_ascii=False, indent=4)

    return len(new_dict["data"])


def get_all_data(start, end, cities):
    range_of_dates = [start + datetime.timedelta(days=delta) for delta in range((end - start).days + 1)]

    for city in cities:
        if not os.path.exists(f"storage/raw_data/{city}"):
            os.mkdir(f"storage/raw_data/{city}")
        count = 0
        for date in range_of_dates:
            count += get_day_events(date, city)
        print(f"{city}: {count} events")


def main():

    # getting all data for 2024 year
    # start_data = datetime.date(2024, 1, 1)
    # end_data = datetime.date(2024, 12, 31)

    start_data = datetime.date.today()
    period = sys.argv[2]
    if period == "day":
        start_data = datetime.date.today() + datetime.timedelta(days=1)
        end_data = start_data
    elif period == "week":
        end_data = start_data + datetime.timedelta(weeks=1)
    elif period == "month":
        end_data = start_data + datetime.timedelta(days=30)
    elif period == "year":
        end_data = start_data + datetime.timedelta(days=365)
    else:
        print(period, "is incorrect argument")
        return

    print("Data collection started")
    get_cities_file()

    with open('storage/raw_data/cities.json', encoding="utf-8") as f:
        cities_json = json.load(f)
    cities_list = [city['id'] for city in cities_json['data']]

    get_all_data(start_data, end_data, cities_list)
    print("All data is collected")

    os.remove("storage/raw_data/cities.json")


if __name__ == '__main__':
    main()
