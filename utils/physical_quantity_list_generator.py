from utils.configlib import Config


def _get_physical_quantity_list_by_conf_file():
    return [key for key in Config.get_data_extraction_conf('keys_of_rows') if key != 'all']


all_physical_quantity_list = _get_physical_quantity_list_by_conf_file()


def physical_quantity_list_generator(physical_quantity_name):
    if isinstance(physical_quantity_name, str):
        physical_quantity_name = [physical_quantity_name]

    if not all(single in all_physical_quantity_list for single in set(physical_quantity_name)) \
            and physical_quantity_name != 'all':
        physical_quantity_name = 'all'

    return all_physical_quantity_list if physical_quantity_name == 'all' else list(set(physical_quantity_name))


def is_in_physical_quantities(physical_quantities):
    is_in = False
    if isinstance(physical_quantities, str) or \
            all(isinstance(physical_quantity, str)
                for physical_quantity in physical_quantities):
        is_in = True
    return is_in
