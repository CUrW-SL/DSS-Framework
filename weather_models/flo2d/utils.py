INVALID_VALUE = -9999


def search_in_dictionary_list(dictionary_list, key_name, key_value):
    for dictionary in dictionary_list:
        if dictionary[key_name] == key_value:
            return dictionary
    return None


def search_value_in_dictionary_list(dictionary_list, key_name, key_value, value_key):
    dictionary = search_in_dictionary_list(dictionary_list, key_name, key_value)
    if dictionary is not None:
        return dictionary[value_key]
    else:
        return INVALID_VALUE
