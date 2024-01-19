import json
import os
import shutil

EVENT_INFO_TAGS = ['title', 'contentRating', 'userRating', 'type', 'tags', 'tickets']
SCHEDULE_INFO_TAGS = ['oneOfPlaces', 'pushkinCardAllowed']
HAS_SUBTAGS = ['userRating', 'type', 'tags', 'tickets', 'oneOfPlaces']


def get_subtags_info(tag_dict, tag, count):
    if tag == 'userRating':
        info = {'value': tag_dict['overall']['value'], 'count': tag_dict['overall']['count']}
    elif tag == 'type':
        info = {'name': tag_dict['name'], 'type': tag_dict['type']}
    elif tag == 'tags':
        info = [{'name': cur['name'], 'type': cur['type']} for cur in tag_dict]
    elif tag == 'tickets':
        if tag_dict and tag_dict[0]['price']:
            info = {'min': int(tag_dict[0]['price']['min'] / 100), 'max': int(tag_dict[0]['price']['max'] / 100)}
        else:
            count += 1
            info = "NULL"
    elif tag == 'oneOfPlaces':
        if not tag_dict['coordinates']:
            count += 1
            info = {'tags': [{'name': cur['name']} for cur in tag_dict['tags']], 'title': tag_dict['title'],
                    'address': tag_dict['address'], 'city': tag_dict['city']['name'],
                    'metro': [station for station in tag_dict['metro']]}
        else:
            info = {'tags': [{'name': cur['name']} for cur in tag_dict['tags']], 'title': tag_dict['title'],
                    'address': tag_dict['address'], 'city': tag_dict['city']['name'],
                    'location': {'lon': tag_dict['coordinates']['longitude'],
                                 'lat': tag_dict['coordinates']['latitude']},
                    'metro': [station['name'] for station in tag_dict['metro']]}
    else:
        info = "NULL"
    return info, count


def get_tags_info(event_dict, type_of_info, count):
    info_dict = {}
    if type_of_info == 'event':
        for tag in EVENT_INFO_TAGS:
            if tag not in HAS_SUBTAGS:
                info_dict[tag] = event_dict[type_of_info][tag]
            else:
                if tag == 'tickets':
                    info, count = get_subtags_info(event_dict[type_of_info][tag], tag, count)
                    if info != "NULL":
                        info_dict['price'] = info
                else:
                    info_dict[tag], count = get_subtags_info(event_dict[type_of_info][tag], tag, count)
    if type_of_info == 'scheduleInfo':
        for tag in SCHEDULE_INFO_TAGS:
            if tag == 'oneOfPlaces':  # HAS_SUBTAGS
                if event_dict[type_of_info][tag]:
                    info_dict['place'], count = get_subtags_info(event_dict[type_of_info][tag], tag, count)
            else:
                info_dict[tag] = event_dict[type_of_info][tag]

    return info_dict, count


def get_day_event_dicts(city, file_name):
    with open(f"storage/raw_data/{city}/{file_name}", encoding="utf-8") as f:
        day_json = json.load(f)

    date = day_json['date']
    events = [event for event in day_json['data']]

    dicts = []
    count = 0
    for event in events:
        event_info, count = get_tags_info(event, 'event', count)
        schedule_info, count = get_tags_info(event, 'scheduleInfo', count)

        event_dict = event_info | schedule_info
        new_dict = {'city_id': city, 'date': date} | event_dict
        dicts.append(new_dict)

    print(f"{city} {date} done, {count} gaps")
    return dicts


def main():
    num_of_events = 0
    cities = os.listdir('storage/raw_data')

    if os.path.exists("storage/data"):
        shutil.rmtree("storage/data")
    os.mkdir("storage/data")

    print("Data preprocessing started")
    for city in cities:
        for day in os.listdir(f'storage/raw_data/{city}'):
            day_str = day.split('_', 1)[1].split('.')[0]

            event_dicts = get_day_event_dicts(city, day)
            for event in event_dicts:
                with open(f'storage/data/{city}_{day_str}_{num_of_events}.json', 'w', encoding="utf-8") as f:
                    json.dump(event, f, ensure_ascii=False, indent=4)
                num_of_events += 1
    print("All data is preprocessed")


if __name__ == '__main__':
    main()
