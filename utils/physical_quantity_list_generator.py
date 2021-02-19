def physical_quantity_list_generator(physical_quantity_name):
    all_physical_quantity_list = ['isotope', 'radioactivity', 'absorption', 'fission', 'decay_heat', 'gamma_spectra']
    if physical_quantity_name not in all_physical_quantity_list and physical_quantity_name !='all':
        physical_quantity_name = 'all'
    return all_physical_quantity_list if physical_quantity_name == 'all' else [physical_quantity_name]
