def display(nested_dict, indent=0):
    """
    display the results of the processing
    """
    output = ""
    for key, value in nested_dict.items():
        output += " " * indent + str(key) + "\n"
        if isinstance(value, dict):
            output += display(value, indent + 4)
        elif isinstance(value, list):
            for item in value:
                output += " " * (indent + 4) + '%s' %str(item) + '\n'
        else:
            output += " " * (indent + 4) + str(value) + "\n"
    return output


