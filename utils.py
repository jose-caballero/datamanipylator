def display(nested_dict, indent=0):
    """
    display the results of the processing
    """
    if isinstance(nested_dict, dict):
        for key, value in nested_dict.items():
            print(" " * indent + str(key))
            display(value, indent + 4)
    elif isinstance(nested_dict, list):
        for item in nested_dict:
            print(" " * (indent) + '%s' %str(item))
    else:
        print(" " * (indent) + str(nested_dict))
