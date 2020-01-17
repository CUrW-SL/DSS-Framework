def search_in_dictionary_list(dictionary_list, key_name, key_value):
    for dictionary in dictionary_list:
        if dictionary[key_name] == key_value:
            return dictionary
